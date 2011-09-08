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


}