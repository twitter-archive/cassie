package com.twitter.cassie

import clocks.Clock

import codecs.Codec
import com.twitter.util.Duration
import com.twitter.conversions.time._
import org.apache.cassandra.finagle.thrift

object Column {
  def apply[A, B](name: A, value: B): Column[A, B] = new Column(name, value)

  /**
    * Convert from a thrift CoSC to a Cassie column. */
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

  /**
    * Convert from a cassie Column to a thrift.Column */
  private[cassie] def convert[A, B](nameCodec: Codec[A], valueCodec: Codec[B], clock: Clock, col: Column[A, B]): thrift.Column = {
    val tColumn = new thrift.Column(nameCodec.encode(col.name))
    tColumn.setValue(valueCodec.encode(col.value))
    tColumn.setTimestamp(col.timestamp.getOrElse(clock.timestamp))
    col.ttl.foreach { t => tColumn.setTtl(t.inSeconds) }
    tColumn
  }
}

case class Column[A, B](name: A, value: B, timestamp: Option[Long], ttl: Option[Duration]) {

  def this(name: A, value: B) = {
    this(name, value, None, None)
  }

  /**
    * Create a copy of this column with a timestamp set. Builder-style. */
  def timestamp(ts: Long): Column[A, B] = {
    copy(timestamp = Some(ts))
  }

  /**
    * Create a copy of this Column with a ttl set. Builder-style. */
  def ttl(t: Duration): Column[A, B] = {
    copy(ttl = Some(t))
  }

  def pair = name -> this
}
