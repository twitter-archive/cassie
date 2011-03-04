package com.twitter.cassie

import clocks.Clock

import codecs.Codec
import com.twitter.util.Duration
import com.twitter.conversions.time._
import org.apache.cassandra.thrift

object Column {
  def apply[A, B](name: A, value: B): Column[A, B] = new Column(name, value)

  private[cassie] def convert[A, B](nameCodec: Codec[A], valueCodec: Codec[B], colOrSCol: thrift.ColumnOrSuperColumn): Column[A, B] = {
    val c = Column(
      nameCodec.decode(colOrSCol.column.name),
      valueCodec.decode(colOrSCol.column.value)
    ).timestamp(colOrSCol.column.timestamp)

    if(colOrSCol.column.isSetTtl) {
      c.ttl(colOrSCol.column.getTtl.seconds)
    } else {
      c
    }
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


case class Column[A, B](name: A, value: B, timestamp: Option[Long], ttl: Option[Duration]) {

  def this(name: A, value: B) = {
    this(name, value, None, None)
  }

  def timestamp(ts: Long): Column[A, B] = {
    copy(timestamp = Some(ts))
  }

  def ttl(t: Duration): Column[A, B] = {
    copy(ttl = Some(t))
  }

  def pair = name -> this
}

/*

Or does it make more sense to define a series of implicit codecs?

Encoding, say, Long instances would suck ass. But one potential way of getting
around this would be to just create a wrapper class to go with the codec:

chunks.insert(chunk.id, Column("gc-marker", VarLong(clock.timestamp)))

*/
