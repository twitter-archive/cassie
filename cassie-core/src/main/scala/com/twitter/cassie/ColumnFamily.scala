package com.twitter.cassie

import com.twitter.cassie.clocks.{MicrosecondEpochClock, Clock}
import com.twitter.cassie.codecs.{Codec}
import com.twitter.cassie.connection.ClientProvider

import org.apache.cassandra.finagle.thrift
import com.twitter.logging.Logger
import java.nio.ByteBuffer
import java.util.Collections.{singleton => singletonJSet}
import com.twitter.cassie.util.ByteBufferUtil.EMPTY
import java.util.{ArrayList => JArrayList, HashMap => JHashMap, List => JList,
  Map => JMap, Set => JSet}
import org.apache.cassandra.finagle.thrift
import scala.collection.JavaConversions._ // TODO get rid of this

import com.twitter.util.Future
import com.twitter.finagle.stats.{StatsReceiver, NullStatsReceiver}

/**
 * A readable, writable column family with batching capabilities. This is a
 * lightweight object: it inherits a connection pool from the Keyspace. */
case class ColumnFamily[Key, Name, Value](
    keyspace: String,
    name: String,
    provider: ClientProvider,
    keyCodec: Codec[Key],
    nameCodec: Codec[Name],
    valueCodec: Codec[Value],
    stats: StatsReceiver = NullStatsReceiver,
    readConsistency: ReadConsistency = ReadConsistency.Quorum,
    writeConsistency: WriteConsistency = WriteConsistency.Quorum
  ) extends ColumnFamilyLike[Key, Name, Value] {

  private[cassie] var clock: Clock = MicrosecondEpochClock
  val log: Logger = Logger.get

  def keysAs[K](codec: Codec[K]): ColumnFamily[K, Name, Value] = copy(keyCodec = codec)
  def namesAs[N](codec: Codec[N]): ColumnFamily[Key, N, Value] = copy(nameCodec = codec)
  def valuesAs[V](codec: Codec[V]): ColumnFamily[Key, Name, V] = copy(valueCodec = codec)
  def consistency(rc: ReadConsistency) = copy(readConsistency = rc)
  def consistency(wc: WriteConsistency) = copy(writeConsistency = wc)

  def newColumn[N, V](n: N, v: V) = Column(n, v)
  def newColumn[N, V](n: N, v: V, ts: Long) = new Column(n, v, Some(ts), None)

  def getColumn(key: Key,
                columnName: Name): Future[Option[Column[Name, Value]]] = {
    getColumns(key, singletonJSet(columnName)).map { result => Option(result.get(columnName))}
  }

  def getRow(key: Key): Future[JMap[Name, Column[Name, Value]]] = {
    getRowSlice(key, None, None, Int.MaxValue, Order.Normal)
  }

  //TODO make the return value ordered
  def getRowSlice(key: Key,
                  startColumnName: Option[Name],
                  endColumnName: Option[Name],
                  count: Int,
                  order: Order): Future[JMap[Name, Column[Name, Value]]] = {
    try {
      val pred = sliceRangePredicate(startColumnName, endColumnName, order, count)
      getSlice(key, pred)
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

  private def sliceRangePredicate(columnNames: JSet[Name]) = {
    new thrift.SlicePredicate().setColumn_names(encodeNames(columnNames))
  }

  def getColumns(key: Key, columnNames: JSet[Name]): Future[JMap[Name, Column[Name, Value]]] = {
    try {
      val pred = new thrift.SlicePredicate().setColumn_names(encodeNames(columnNames))
      getSlice(key, pred)
    }  catch {
      case e => Future.exception(e)
    }
  }

  def multigetColumn(keys: JSet[Key], columnName: Name): Future[JMap[Key, Column[Name, Value]]] = {
    multigetColumns(keys, singletonJSet(columnName)).map { rows =>
      val cols: JMap[Key, Column[Name, Value]] = new JHashMap(rows.size)
      for (rowEntry <- asScalaIterable(rows.entrySet))
        if (!rowEntry.getValue.isEmpty)
          cols.put(rowEntry.getKey, rowEntry.getValue.get(columnName))
      cols
    }
  }

  def multigetColumns(keys: JSet[Key], columnNames: JSet[Name]) = {
    try {
      val cp = new thrift.ColumnParent(name)
      val pred = sliceRangePredicate(columnNames)
      log.debug("multiget_slice(%s, %s, %s, %s, %s)", keyspace, keys, cp, pred, readConsistency.level)
      stats.timeFuture("multiget_slice") {
        provider.map {
          _.multiget_slice(encodeKeys(keys), cp, pred, readConsistency.level)
        }.map { result =>
          val rows: JMap[Key, JMap[Name, Column[Name, Value]]] = new JHashMap(result.size)
          for (rowEntry <- asScalaIterable(result.entrySet)) {
            val cols: JMap[Name, Column[Name, Value]] = new JHashMap(rowEntry.getValue.size)
            for (cosc <- asScalaIterable(rowEntry.getValue)) {
              val col = Column.convert(nameCodec, valueCodec, cosc)
              cols.put(col.name, col)
            }
            rows.put(keyCodec.decode(rowEntry.getKey), cols)
          }
          rows
        }
      }
    } catch {
      case e => Future.exception(e)
    }
  }

  def insert(key: Key, column: Column[Name, Value]) = {
    try {
      val cp = new thrift.ColumnParent(name)
      val col = Column.convert(nameCodec, valueCodec, clock, column)
      log.debug("insert(%s, %s, %s, %s, %d, %s)", keyspace, key, cp, column.value,
        col.timestamp, writeConsistency.level)
      stats.timeFuture("insert") {
        provider.map {
          _.insert(keyCodec.encode(key), cp, col, writeConsistency.level)
        }
      }
    }  catch {
      case e => Future.exception(e)
    }
  }

  def truncate() = stats.timeFuture("truncate"){provider.map(_.truncate(name))}

  def removeColumn(key: Key, columnName: Name) = {
    try {
      val cp = new thrift.ColumnPath(name).setColumn(nameCodec.encode(columnName))
      val timestamp = clock.timestamp
      log.debug("remove(%s, %s, %s, %d, %s)", keyspace, key, cp, timestamp, writeConsistency.level)
      stats.timeFuture("remove") {
        provider.map {
          _.remove(keyCodec.encode(key), cp, timestamp, writeConsistency.level)
        }
      }
    }  catch {
      case e => Future.exception(e)
    }
  }

  def removeColumns(key: Key, columnNames: JSet[Name]): Future[Void] = {
    batch()
      .removeColumns(key, columnNames)
      .execute()
  }

  def removeColumns(key: Key, columnNames: JSet[Name], timestamp: Long): Future[Void] = {
    batch()
      .removeColumns(key, columnNames, timestamp)
      .execute()
  }

  def removeRow(key: Key) = {
    removeRowWithTimestamp(key, clock.timestamp)
  }

  def removeRowWithTimestamp(key: Key, timestamp: Long) = {
    val cp = new thrift.ColumnPath(name)
    log.debug("remove(%s, %s, %s, %d, %s)", keyspace, key, cp, timestamp, writeConsistency.level)
    stats.timeFuture("remove") {
      provider.map {
        _.remove(keyCodec.encode(key), cp, timestamp, writeConsistency.level)
      }
    }
  }

  def batch() = new BatchMutationBuilder(this)

  private[cassie] def batch(mutations: JMap[ByteBuffer, JMap[String, JList[thrift.Mutation]]]) = {
    log.debug("batch_mutate(%s, %s, %s", keyspace, mutations, writeConsistency.level)
    stats.timeFuture("batch_mutate") {
        provider.map {
        _.batch_mutate(mutations, writeConsistency.level)
      }
    }
  }

  def rowsIteratee(start: Key, end:Key, batchSize: Int, columnNames: JSet[Name]) = {
    RowsIteratee(this, start, end, batchSize, sliceRangePredicate(columnNames))
  }

  def rowsIteratee(batchSize: Int): RowsIteratee[Key, Name, Value] = {
    val pred = sliceRangePredicate(None, None, Order.Normal, Int.MaxValue)
    RowsIteratee(this, batchSize, pred)
  }

  def rowsIteratee(batchSize: Int,
                     columnName: Name): RowsIteratee[Key, Name, Value] =
    rowsIteratee(batchSize, singletonJSet(columnName))

  def rowsIteratee(batchSize: Int, columnNames: JSet[Name]): RowsIteratee[Key, Name, Value] = {
    val pred = sliceRangePredicate(columnNames)
    RowsIteratee(this, batchSize, pred)
  }

  def columnsIteratee(key: Key): ColumnsIteratee[Key, Name, Value] = {
    columnsIteratee(100, key)
  }

  def columnsIteratee(batchSize: Int, key: Key): ColumnsIteratee[Key, Name, Value] = {
    ColumnsIteratee(this, key, batchSize)
  }

  private[cassie] def getSlice(key: Key,
                          pred: thrift.SlicePredicate): Future[JMap[Name,Column[Name,Value]]] = {
    val cp = new thrift.ColumnParent(name)
    log.debug("get_slice(%s, %s, %s, %s, %s)", keyspace, key, cp, pred, readConsistency.level)
    stats.timeFuture("get_slice") {
      provider.map {
        _.get_slice(keyCodec.encode(key), cp, pred, readConsistency.level)
      } map { result =>
        val cols: JMap[Name,Column[Name,Value]] = new JHashMap(result.size)
        for (cosc <- result.iterator) {
          val col = Column.convert(nameCodec, valueCodec, cosc)
          cols.put(col.name, col)
        }
        cols
      }
    }
  }

  private[cassie] def getOrderedSlice(key: Key, start: Option[Name], end: Option[Name], size: Int): Future[Seq[Column[Name, Value]]] = {
    val pred = sliceRangePredicate(start, end, Order.Normal, size)
    val cp = new thrift.ColumnParent(name)
    log.debug("get_slice(%s, %s, %s, %s, %s)", keyspace, key, cp, pred, readConsistency.level)
    stats.timeFuture("get_slice") {
      provider.map {
        _.get_slice(keyCodec.encode(key), cp, pred, readConsistency.level) 
      } map { result =>
        result.map { cosc =>
          Column.convert(nameCodec, valueCodec, cosc)
        }
      }
    }
  }

  private[cassie] def getRangeSlice(startKey: Key,
                                    endKey: Key,
                                    count: Int,
                                    predicate: thrift.SlicePredicate) = {

    val cp = new thrift.ColumnParent(name)
    val range = new thrift.KeyRange(count).setStart_key(keyCodec.encode(startKey)).setEnd_key(keyCodec.encode(endKey))
    log.debug("get_range_slices(%s, %s, %s, %s, %s)", keyspace, cp, predicate, range, readConsistency.level)
    stats.timeFuture("get_range_slices") {
      provider.map {
        _.get_range_slices(cp, predicate, range, readConsistency.level)
      } map { slices =>
        val buf:JList[(Key, JList[Column[Name, Value]])] = new JArrayList[(Key, JList[Column[Name, Value]])](slices.size)
        slices.foreach { ks =>
          val key = keyCodec.decode(ks.key)
          val cols = new JArrayList[Column[Name, Value]](ks.columns.size)
          ks.columns.foreach { col =>
            cols.add(Column.convert(nameCodec, valueCodec, col))
          }
          buf.add((key, cols))
        }
        buf
      }
    }
  }

  def encodeNames(values: JSet[Name]): JList[ByteBuffer] = {
    val output = new JArrayList[ByteBuffer](values.size)
    for (value <- asScalaIterable(values))
      output.add(nameCodec.encode(value))
    output
  }

  def encodeKeys(values: JSet[Key]): JList[ByteBuffer] = {
    val output = new JArrayList[ByteBuffer](values.size)
    for (value <- asScalaIterable(values))
      output.add(keyCodec.encode(value))
    output
  }
}