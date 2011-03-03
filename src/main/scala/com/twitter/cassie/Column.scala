package com.twitter.cassie

import clocks.Clock

import codecs.Codec
import com.twitter.util.Duration
import com.twitter.conversions.time._
import org.apache.cassandra.thrift

object Column {
  def apply[A, B](name: A, value: B): Column[A, B] = apply(name, value, None)

  def apply[A, B](name: A, value: B, timestamp: Long): Column[A, B] = apply(name, value, Some(timestamp))

  def apply[A, B](name: A, value: B, timestamp: Long, ttl: Duration): Column[A, B] = Column(name, value, Some(timestamp), Some(ttl))

  def apply[A, B](name: A, value: B, timestamp: Option[Long]): Column[A, B] = apply(name, value, timestamp, None)

  private[cassie] def convert[A, B](nameCodec: Codec[A], valueCodec: Codec[B], colOrSCol: thrift.ColumnOrSuperColumn): Column[A, B] = {

    val ttl = if(colOrSCol.column.isSetTtl) {
      Some(colOrSCol.column.getTtl.seconds)
    } else {
      None
    }
    Column(
      nameCodec.decode(colOrSCol.column.name),
      valueCodec.decode(colOrSCol.column.value),
      Some(colOrSCol.column.timestamp),
      ttl
    )
  }

  private[cassie] def convert[A, B](nameCodec: Codec[A], valueCodec: Codec[B], clock: Clock, col: Column[A, B]): thrift.Column = {
    val tColumn = new thrift.Column(
      nameCodec.encode(col.name),
      valueCodec.encode(col.value),
      col.timestamp.getOrElse(clock.timestamp)
    )
    col.ttl.foreach { t => tColumn.setTtl(t.inSeconds) }
    tColumn
  }
}

/**
 * A column in a Cassandra. Belongs to a row in a column family.
 *
 * @author coda
 */
case class Column[A, B](name: A, value: B, timestamp: Option[Long], ttl: Option[Duration]) {
  def pair = name -> this
}

/*

Or does it make more sense to define a series of implicit codecs?

Encoding, say, Long instances would suck ass. But one potential way of getting
around this would be to just create a wrapper class to go with the codec:

chunks.insert(chunk.id, Column("gc-marker", VarLong(clock.timestamp)))

*/
