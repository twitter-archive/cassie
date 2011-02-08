package com.twitter.cassie

import clocks.Clock

import codecs.Codec
import org.apache.cassandra.thrift

object Column {

  def apply[A, B](name: A, value: B, timestamp: Long): Column[A, B] = apply(name, value, Some(timestamp))

  private[cassie] def convert[A, B](nameCodec: Codec[A], valueCodec: Codec[B], colOrSCol: thrift.ColumnOrSuperColumn): Column[A, B] = {
    Column(
      nameCodec.decode(colOrSCol.column.name),
      valueCodec.decode(colOrSCol.column.value),
      colOrSCol.column.timestamp
    )
  }

  private[cassie] def convert[A, B](nameCodec: Codec[A], valueCodec: Codec[B], clock: Clock, col: Column[A, B]): thrift.Column = {
    new thrift.Column(
      nameCodec.encode(col.name),
      valueCodec.encode(col.value),
      col.timestamp.getOrElse(clock.timestamp)
    )
  }
}

/**
 * A column in a Cassandra. Belongs to a row in a column family.
 *
 * @author coda
 */
case class Column[A, B](name: A, value: B, timestamp: Option[Long] = None) {
  def pair = name -> this
}

/*

Or does it make more sense to define a series of implicit codecs?

Encoding, say, Long instances would suck ass. But one potential way of getting
around this would be to just create a wrapper class to go with the codec:

chunks.insert(chunk.id, Column("gc-marker", VarLong(clock.timestamp)))

*/
