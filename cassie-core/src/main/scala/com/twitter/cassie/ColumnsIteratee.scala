package com.twitter.cassie

import scala.collection.JavaConversions._
import com.twitter.util.{Future, Promise}
import java.util.{Map => JMap, List => JList, ArrayList => JArrayList}
import org.apache.cassandra.finagle.thrift
import com.twitter.cassie.util.ByteBufferUtil


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
case class ColumnsIteratee[Key, Name, Value](cf: ColumnFamily[Key, Name, Value],
                                             key: Key,
                                             batchSize: Int,
                                             buffer: JList[Column[Name, Value]] = Nil: JList[Column[Name, Value]],
                                             cycled: Boolean = false,
                                             skip: Option[Name] = None,
                                             startColumn: Option[Name] = None) {

  def foreach(f: Column[Name, Value] => Unit): Future[Unit] = {
    val p = new Promise[Unit]
    next map (_.visit(p, f)) handle { case e => p.setException(e) }
    p
  }

  private def visit(p: Promise[Unit], f: Column[Name, Value] => Unit): Unit = {
    for (c <- buffer) {
      f(c)
    }
    if (hasNext) {
      next map (_.visit(p, f)) handle { case e => p.setException(e) }
    } else {
      p.setValue(Unit)
    }
  }

  /** Copy constructors for next() and end() cases. */

  private def end(buffer: JList[Column[Name, Value]]) = copy(cycled = true, buffer = buffer)
  private def next(buffer: JList[Column[Name, Value]],
                    lastFoundColumn: Name) =
    copy(startColumn = Some(lastFoundColumn), skip = Some(lastFoundColumn), buffer = buffer)

  private val requestSize = batchSize + skip.size

  /** @return True if calling next() will request another batch of data. */
  private def hasNext() = {
    !cycled && buffer.size > 0
  }
  /**
   * If hasNext == true, requests the next batch of data, otherwise throws
   * UnsupportedOperationException.
   * @return a future that can contain [[org.apache.cassandra.finagle.thrift.TimedOutException]],
   *  [[org.apache.cassandra.finagle.thrift.UnavailableException]] or [[org.apache.cassandra.finagle.thrift.InvalidRequestException]]
   */
  def next(): Future[ColumnsIteratee[Key, Name, Value]] = {
    if (cycled) throw new UnsupportedOperationException("No more results.")

    requestNextSlice().map { slice =>
      val buffer = slice.drop(skip.size)
      if (buffer.size() == 0) {
        end(Nil: JList[Column[Name, Value]])
      } else if (buffer.size() < batchSize) {
        end(buffer)
      } else {
        next(buffer, slice.last.name)
      }
    }
  }

  private def requestNextSlice(): Future[Seq[Column[Name, Value]]] = {
    cf.getOrderedSlice(key, startColumn, None, requestSize)
  }
}
