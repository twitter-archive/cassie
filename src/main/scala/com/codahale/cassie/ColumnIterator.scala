package com.codahale.cassie

import scalaj.collection.Imports._
import collection.mutable.ArrayBuffer
import com.codahale.logula.Logging
import org.apache.cassandra.thrift.{ColumnOrSuperColumn, SlicePredicate}

/**
 * Given a column family, a key range, a batch size, a slice predicate, and
 * a consistency level, iterates through each matching column of each matching
 * key until a cycle is detected (e.g., Cassandra returns the last slice a
 * second time) or until an empty slice is returned (e.g., no more slices).
 * Provides a sequence of (row key, column). 
 *
 * @author coda
 */
class ColumnIterator[Name, Value](val cf: ColumnFamily[Name, Value],
                                  val startKey: String,
                                  val endKey: String,
                                  val batchSize: Int,
                                  val predicate: SlicePredicate,
                                  val consistency: ReadConsistency)
        extends Iterator[(String, Column[Name, Value])] with Logging {
  
  private var lastKey: Option[String] = None
  private var cycled = false
  private val buffer = new ArrayBuffer[(String, Column[Name, Value])]

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
    buffer ++= slice.filterNot { col => lastKey.map { _ == col.key }.getOrElse(false) }.flatMap { ks =>
      ks.columns.asScala.map { col =>
        ks.key -> cf.convert(col)
      }
    }
    val lastFoundKey = Some(slice.last.key)
    cycled = lastKey == lastFoundKey
    lastKey = lastFoundKey
  }
}
