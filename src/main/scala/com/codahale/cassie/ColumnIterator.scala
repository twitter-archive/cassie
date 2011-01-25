package com.codahale.cassie

import java.nio.ByteBuffer
import codecs.Codec
import scalaj.collection.Imports._
import collection.mutable.ArrayBuffer
import com.codahale.logula.Logging
import org.apache.cassandra.thrift.{ColumnOrSuperColumn, KeySlice, SlicePredicate}

/**
 * Given a column family, a key range, a batch size, a slice predicate, and
 * a consistency level, iterates through each matching column of each matching
 * key until a cycle is detected (e.g., Cassandra returns the last slice a
 * second time) or until an empty slice is returned (e.g., no more slices).
 * Provides a sequence of (row key, column). 
 *
 * @author coda
 */
class ColumnIterator[Key, Name, Value](val cf: ColumnFamily[_, _, _],
                                       val startKey: ByteBuffer,
                                       val endKey: ByteBuffer,
                                       val batchSize: Int,
                                       val predicate: SlicePredicate,
                                       val consistency: ReadConsistency,
                                       val keyCodec: Codec[Key],
                                       val nameCodec: Codec[Name],
                                       val valueCodec: Codec[Value])
        extends Iterator[(Key, Column[Name, Value])] with Logging {
  private var lastKey: Option[ByteBuffer] = None
  private var cycled = false
  private val buffer = new ArrayBuffer[(Key, Column[Name, Value])]

  def next() = {
    if (hasNext) {
      buffer.remove(0)
    } else {
      throw new NoSuchElementException("next on empty iterator")
    }
  }

  def hasNext = {
    if (!buffer.isEmpty) {
      true
    } else if (cycled) {
      false
    } else {
      getNextSlice
      !buffer.isEmpty
    }
  }

  private def getNextSlice() {
    val effectiveCount = lastKey.map { _ => batchSize }.getOrElse(batchSize+1)
    val slice = cf.getRangeSlice(lastKey.getOrElse(startKey), endKey, effectiveCount, predicate, consistency)
    val filterPred = (ks: KeySlice) => lastKey.map { _ == ks.key }.getOrElse(false)
    buffer ++= slice.filterNot(filterPred).flatMap { ks =>
      ks.columns.asScala.map { col =>
        keyCodec.decode(ks.key) -> Column.convert(nameCodec, valueCodec, col)
      }
    }
    if (!slice.isEmpty) {
      val lastFoundKey = Some(slice.last.key)
      cycled = lastKey == lastFoundKey
      lastKey = lastFoundKey
    }    
  }
}
