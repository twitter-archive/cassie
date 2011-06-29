package com.twitter.cassie

import java.util.{Iterator => JIterator}

/**
  * A fully synchronous wrapper around RowsIteratee and ColumnIteratee */
class ColumnIterator[Key, Name, Value](private var iteratee: Iteratee[Key, Name, Value])
        extends JIterator[(Key, Column[Name, Value])]
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
