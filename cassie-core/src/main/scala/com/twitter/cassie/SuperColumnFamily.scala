package com.twitter.cassie

import com.twitter.cassie.clocks.{MicrosecondEpochClock, Clock}
import com.twitter.cassie.codecs.{Codec}
import com.twitter.cassie.connection.ClientProvider
import com.twitter.cassie.util.FutureUtil.timeFutureWithFailures

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
import com.twitter.finagle.stats.StatsReceiver

/**
 * A readable, writable column family with batching capabilities. This is a
 * lightweight object: it inherits a connection pool from the Keyspace. */
@deprecated("use compound columns instead")
case class SuperColumnFamily[Key, Name, SubName, Value](
    keyspace: String,
    name: String,
    provider: ClientProvider,
    keyCodec: Codec[Key],
    nameCodec: Codec[Name],
    subNameCodec: Codec[SubName],
    valueCodec: Codec[Value],
    stats: StatsReceiver,
    readConsistency: ReadConsistency = ReadConsistency.Quorum,
    writeConsistency: WriteConsistency = WriteConsistency.Quorum
  )  {

  private[cassie] var clock: Clock = MicrosecondEpochClock
  val log: Logger = Logger.get

  def consistency(rc: ReadConsistency) = copy(readConsistency = rc)
  def consistency(wc: WriteConsistency) = copy(writeConsistency = wc)

  def insert(key: Key, superColumn: Name, column: Column[SubName, Value]) = {
    try {
      val cp = (new thrift.ColumnParent(name)).setSuper_column(nameCodec.encode(superColumn))
      val col = Column.convert(subNameCodec, valueCodec, clock, column)
      log.debug("insert(%s, %s, %s, %s, %d, %s)", keyspace, key, cp, column.value,
        col.timestamp, writeConsistency.level)
      timeFutureWithFailures(stats, "insert") {
        provider.map {
          _.insert(keyCodec.encode(key), cp, col, writeConsistency.level)
        }
      }
    }  catch {
      case e => Future.exception(e)
    }
  }

  def getRow(key: Key): Future[Seq[(Name, Seq[Column[SubName, Value]])]] = {
    getRowSlice(key, None, None, Int.MaxValue, Order.Normal)
  }

  def getRowSlice(key: Key,
                  startColumnName: Option[Name],
                  endColumnName: Option[Name],
                  count: Int,
                  order: Order): Future[Seq[(Name, Seq[Column[SubName, Value]])]] = {
    try {
      val pred = sliceRangePredicate(startColumnName, endColumnName, order, count)
      getSlice(key, None, None, Int.MaxValue)
    } catch {
      case e => Future.exception(e)
    }
  }

  private
  def getSlice(key: Key, start: Option[Name], end: Option[Name], size: Int): Future[Seq[(Name, Seq[Column[SubName, Value]])]] = {
    val pred = sliceRangePredicate(start, end, Order.Normal, size)
    val cp = new thrift.ColumnParent(name)
    log.debug("get_slice(%s, %s, %s, %s, %s)", keyspace, key, cp, pred, readConsistency.level)
    timeFutureWithFailures(stats, "get_slice") {
      provider.map {
        _.get_slice(keyCodec.encode(key), cp, pred, readConsistency.level) 
      } map { result =>
        result.map { cosc =>
          val sc = cosc.getSuper_column()
          (nameCodec.decode(sc.name), sc.columns.map(Column.convert(subNameCodec, valueCodec, _)))
        }
      }
    }
  }

  def removeRow(key: Key) = {
    val cp = new thrift.ColumnPath(name)
    val ts = clock.timestamp
    log.debug("remove(%s, %s, %s, %d, %s)", keyspace, key, cp, ts, writeConsistency.level)
    timeFutureWithFailures(stats, "remove") {
      provider.map {
        _.remove(keyCodec.encode(key), cp, ts, writeConsistency.level)
      }
    }
  }

  private def sliceRangePredicate(startColumnName: Option[Name], endColumnName: Option[Name], order: Order, count: Int) = {
    val startBytes = startColumnName.map { c => nameCodec.encode(c) }.getOrElse(EMPTY)
    val endBytes = endColumnName.map { c => nameCodec.encode(c) }.getOrElse(EMPTY)
    val pred = new thrift.SlicePredicate()
    pred.setSlice_range(new thrift.SliceRange(startBytes, endBytes, order.reversed, count))
  }

  private def sliceRangePredicate(columnNames: JSet[Name]) = {
    new thrift.SlicePredicate().setColumn_names(nameCodec.encodeSet(columnNames))
  }
}