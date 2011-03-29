package com.twitter.cassie

import clocks.Clock

import codecs.Codec
import org.apache.cassandra.finagle.thrift

object CounterColumn {

  private[cassie] def convert[A](nameCodec: Codec[A], counter: thrift.Counter): CounterColumn[A] = {
    CounterColumn(
      nameCodec.decode(counter.column.name),
      counter.column.value
    )
  }

  private[cassie] def convert[A](nameCodec: Codec[A], counter: thrift.CounterColumn): CounterColumn[A] = {
    CounterColumn(
      nameCodec.decode(counter.name),
      counter.value
    )
  }

  private[cassie] def convert[A](nameCodec: Codec[A], col: CounterColumn[A]): thrift.CounterColumn = {
    new thrift.CounterColumn(
      nameCodec.encode(col.name),
      col.value
    )
  }
}

/**
 * A column in a Cassandra. Belongs to a row in a column family.
 */
case class CounterColumn[A](name: A, value: Long) {
  def pair = name -> this
}
