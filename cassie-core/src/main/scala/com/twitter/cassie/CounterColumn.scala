package com.twitter.cassie

import codecs.Codec
import org.apache.cassandra.finagle.thrift

object CounterColumn {

  /**
   * Convert from a thrift.CounterColumn to a cassie CounterColumn
   */
  private[cassie] def convert[A](nameCodec: Codec[A], counter: thrift.CounterColumn): CounterColumn[A] = {
    CounterColumn(
      nameCodec.decode(counter.name),
      counter.value
    )
  }

  /**
   * Convert from a thrift.CounterColumn to a cassie CounterColumn
   */
  private[cassie] def convert[A](nameCodec: Codec[A], cosc: thrift.ColumnOrSuperColumn): CounterColumn[A] = {
    val counter = cosc.getCounter_column
    CounterColumn(
      nameCodec.decode(counter.name),
      counter.value
    )
  }

  /**
   * Convert from a cassie CounterColumn to a thrift CounterColumn
   */
  private[cassie] def convert[A](nameCodec: Codec[A], col: CounterColumn[A]): thrift.CounterColumn = {
    new thrift.CounterColumn(
      nameCodec.encode(col.name),
      col.value
    )
  }
}

/**
 * A counter column in a Cassandra. Belongs to a row in a column family.
 */
case class CounterColumn[A](name: A, value: Long) {
  def pair = name -> this
}
