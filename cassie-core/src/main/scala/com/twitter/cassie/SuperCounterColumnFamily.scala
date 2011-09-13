package com.twitter.cassie

import com.twitter.cassie.codecs.Codec
import com.twitter.cassie.connection.ClientProvider

import org.apache.cassandra.finagle.thrift
import com.twitter.logging.Logger
import java.nio.ByteBuffer
import java.util.Collections.{singleton => singletonJSet}
import com.twitter.cassie.util.ByteBufferUtil.EMPTY

import java.util.{ArrayList => JArrayList, HashMap => JHashMap,
    Iterator => JIterator, List => JList, Map => JMap, Set => JSet}
import scala.collection.JavaConversions._

import com.twitter.util.Future
import com.twitter.finagle.stats.{StatsReceiver, NullStatsReceiver}
import scala.collection.mutable.ListBuffer
import com.twitter.cassie.util.FutureUtil.timeFutureWithFailures

@deprecated("use compound columns instead")
case class SuperCounterColumnFamily[Key, Name, SubName](
    keyspace: String,
    name: String,
    provider: ClientProvider,
    keyCodec: Codec[Key],
    nameCodec: Codec[Name],
    subNameCodec: Codec[SubName],
    stats: StatsReceiver,
    readConsistency: ReadConsistency = ReadConsistency.Quorum,
    writeConsistency: WriteConsistency = WriteConsistency.One) {

  val log: Logger = Logger.get

  def consistency(rc: ReadConsistency) = copy(readConsistency = rc)
  def consistency(wc: WriteConsistency) = copy(writeConsistency = wc)

  def multigetSlices(keys: JSet[Key], start: Name, end: Name): Future[JMap[Key, JMap[Name, JMap[SubName, CounterColumn[SubName]]]]] = {
    try {
      val pred = sliceRangePredicate(Some(start), Some(end), Order.Normal, Int.MaxValue)
      multigetSlice(keys, pred)
    } catch {
      case e => Future.exception(e)
    }
  }

  private def sliceRangePredicate(startColumnName: Option[Name], endColumnName: Option[Name], order: Order, count: Int) = {
    val startBytes = startColumnName.map { c => nameCodec.encode(c) }.getOrElse(EMPTY)
    val endBytes = endColumnName.map { c => nameCodec.encode(c) }.getOrElse(EMPTY)
    val pred = new thrift.SlicePredicate()
    pred.setSlice_range(new thrift.SliceRange(startBytes, endBytes, order.reversed, count))
  }

  private def multigetSlice(keys: JSet[Key], pred: thrift.SlicePredicate): Future[JMap[Key, JMap[Name, JMap[SubName, CounterColumn[SubName]]]]] = {
    val cp = new thrift.ColumnParent(name)
    log.debug("multiget_counter_slice(%s, %s, %s, %s, %s)", keyspace, keys, cp, pred, readConsistency.level)
    val encodedKeys = keyCodec.encodeSet(keys)
    timeFutureWithFailures(stats, "multiget_slice") {
      provider.map {
        _.multiget_slice(encodedKeys, cp, pred, readConsistency.level)
      }.map { result =>
        val rows: JMap[Key, JMap[Name, JMap[SubName, CounterColumn[SubName]]]] = new JHashMap(result.size)
        for (rowEntry <- asScalaIterable(result.entrySet)) {
          val sCols: JMap[Name, JMap[SubName, CounterColumn[SubName]]] = new JHashMap(rowEntry.getValue.size)
          for (scol <- asScalaIterable(rowEntry.getValue)) {
            val cols: JMap[SubName, CounterColumn[SubName]] = new JHashMap(scol.getCounter_super_column.columns.size)
            for (counter <- asScalaIterable(scol.getCounter_super_column().columns)) {
              val col = CounterColumn.convert(subNameCodec, counter)
              cols.put(col.name, col)
            }
            sCols.put(nameCodec.decode(scol.getCounter_super_column.BufferForName()), cols)
          }
          rows.put(keyCodec.decode(rowEntry.getKey), sCols)
        }
        rows
      }
    }
  }

  def batch() = new SuperCounterBatchMutationBuilder[Key, Name, SubName](this)

  private[cassie] def batch(mutations: JMap[ByteBuffer, JMap[String, JList[thrift.Mutation]]]) = {
    log.debug("batch_mutate(%s, %s, %s", keyspace, mutations, writeConsistency.level)
    timeFutureWithFailures(stats, "batch_mutate") {
        provider.map {
        _.batch_mutate(mutations, writeConsistency.level)
      }
    }
  }

}

@deprecated("use compound columns instead")
class SuperCounterBatchMutationBuilder[Key, Name, SubName](cf: SuperCounterColumnFamily[Key,Name, SubName]) extends BatchMutation {

  case class Insert(key: Key, name: Name, column: CounterColumn[SubName])

  private val ops = new ListBuffer[Insert]

  def insert(key: Key, name: Name, column: CounterColumn[SubName]) = synchronized {
    ops.append(Insert(key, name, column))
    this
  }

  /**
    * Submits the batch of operations, returning a future to allow blocking for success. */
  def execute() = {
    try {
      cf.batch(mutations)
    } catch {
      case e => Future.exception(e)
    }
  }

  private[cassie] def mutations: JMap[ByteBuffer, JMap[String, JList[thrift.Mutation]]] = synchronized {
    val mutations = new JHashMap[ByteBuffer, JMap[String, JList[thrift.Mutation]]]()

    ops.map { insert =>
      val cosc = new thrift.ColumnOrSuperColumn()
      val counterColumn = new thrift.CounterColumn(cf.subNameCodec.encode(insert.column.name), insert.column.value)
      val sc = new thrift.CounterSuperColumn(cf.nameCodec.encode(insert.name), new JArrayList[thrift.CounterColumn](){counterColumn})
      cosc.setCounter_super_column(sc)
      val mutation = new thrift.Mutation
      mutation.setColumn_or_supercolumn(cosc)

      val encodedKey = cf.keyCodec.encode(insert.key)

      val h = Option(mutations.get(encodedKey)).getOrElse{val x = new JHashMap[String, JList[thrift.Mutation]]; mutations.put(encodedKey, x); x}
      val l = Option(h.get(cf.name)).getOrElse{ val y = new JArrayList[thrift.Mutation]; h.put(cf.name, y); y}
      l.add(mutation)
    }
    mutations
  }
}
