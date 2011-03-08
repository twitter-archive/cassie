package com.twitter.cassie

import java.nio.ByteBuffer
import java.util.List
import scala.collection.JavaConversions._
import com.twitter.util.Future

import codecs.Codec

import collection.mutable.ArrayBuffer
import org.apache.cassandra.finagle.thrift.{ColumnOrSuperColumn, KeySlice, SlicePredicate}

class ColumnIterator[Key, Name, Value](private var iteratee: ColumnIteratee[Key, Name, Value])
        extends java.util.Iterator[(Key, Column[Name, Value])]
        with Iterator[(Key, Column[Name, Value])] {
  private var iterator = iteratee.buffer.iterator

  def next() = {
    if (!hasNext())
      throw new NoSuchElementException("next on empty iterator")
    iterator.next
  }

  def hasNext(): Boolean = {
    if (iterator.hasNext())
      return true
    if (!iteratee.hasNext())
      return false
    // request the next batch
    iteratee = iteratee.next()()
    iterator = iteratee.buffer.iterator
    return iterator.hasNext()
  }

  def remove() = throw new UnsupportedOperationException()
}
