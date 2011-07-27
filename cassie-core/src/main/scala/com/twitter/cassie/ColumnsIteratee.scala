package com.twitter.cassie

import scala.collection.JavaConversions._
import com.twitter.util.{Future, Promise}
import java.util.{Map => JMap, List => JList, ArrayList => JArrayList}
import org.apache.cassandra.finagle.thrift

/**
  * Async iteration across the columns for a given key.
  *
  * EXAMPLE
  * val cf = new Cluster("127.0.0.1").keyspace("foo")
  *   .connect().columnFamily("bar", Utf8Codec, Utf8Codec, Utf8Codec)
  *
  * val done = cf.columnsIteratee.foreach("bam").foreach {col =>
  *   println(col) // this function is executed asynchronously for each column
  * }
  * done() // this is a Future[Unit] that will be satisfied when the iteration
  *        //   is done
  */
  
trait ColumnsIteratee[Key, Name, Value] {
  def hasNext(): Boolean
  def next(): Future[ColumnsIteratee[Key, Name, Value]]

  def foreach(f: Column[Name, Value] => Unit): Future[Unit] = {
    val p = new Promise[Unit]
    next map (_.visit(p, f)) handle { case e => p.setException(e) }
    p
  }

  def visit(p: Promise[Unit], f: Column[Name, Value] => Unit): Unit
}

object ColumnsIteratee {
  def apply[Key, Name, Value](cf: ColumnFamily[Key, Name, Value], key: Key, batchSize: Int) = {
    new InitialColumnsIteratee(cf, key, batchSize)
  }
}

private[cassie] class InitialColumnsIteratee[Key, Name, Value](val cf: ColumnFamily[Key, Name, Value], key: Key, batchSize: Int)
    extends ColumnsIteratee[Key, Name, Value] {

  def hasNext() = true

  def next() = {
    cf.getOrderedSlice(key, None, None, batchSize).map { buf =>
      if(buf.size() < batchSize) {
        new FinalColumnsIteratee(buf)
      } else {
        new SubsequentColumnsIteratee(cf, key, batchSize, buf.last.name, buf)
      }
    }
  }

  def visit(p: Promise[Unit], f: Column[Name, Value] => Unit) {
    throw new UnsupportedOperationException("no need to visit the initial Iteratee")
  }
}

private[cassie] class SubsequentColumnsIteratee[Key, Name, Value](val cf: ColumnFamily[Key, Name, Value], 
    val key: Key, val batchSize: Int, val start: Name, val buffer: JList[Column[Name, Value]])
    extends ColumnsIteratee[Key, Name, Value] {

  def hasNext = true

  def next() = {
    cf.getOrderedSlice(key, Some(start), None, batchSize+1).map { buf =>
      val skipped = buf.subList(1, buf.length)
      if(skipped.size() < batchSize) {
        new FinalColumnsIteratee(skipped)
      } else {
        new SubsequentColumnsIteratee(cf, key, batchSize, skipped.last.name, skipped)
      }
    }
  }

  def visit(p: Promise[Unit], f: Column[Name, Value] => Unit) {
    for (c <- buffer) {
      f(c)
    }
    if (hasNext) {
      next map (_.visit(p, f)) handle { case e => p.setException(e) }
    } else {
      p.setValue(Unit)
    }
  }
}

private[cassie] class FinalColumnsIteratee[Key, Name, Value](val buffer: JList[Column[Name, Value]]) 
  extends ColumnsIteratee[Key, Name, Value] {
  def hasNext = false
  def next    = Future.exception(new UnsupportedOperationException("no next for the final iteratee"))

  def visit(p: Promise[Unit], f: Column[Name, Value] => Unit) {
    for (c <- buffer) {
      f(c)
    }
    p.setValue(Unit)
  }
}