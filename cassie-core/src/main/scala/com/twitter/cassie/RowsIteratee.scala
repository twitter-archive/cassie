package com.twitter.cassie

import scala.collection.JavaConversions._
import com.twitter.util.{Future, Promise}
import org.apache.cassandra.finagle.thrift
import java.util.{List => JList, ArrayList => JArrayList}
import com.twitter.cassie.util.ByteBufferUtil

/**
 * Given a column family, a key range, a batch size, a slice predicate, 
 * iterates through slices of each matching row until a cycle is detected 
 * (e.g., Cassandra returns the last slice a second time) or until an empty
 * slice is returned (e.g., no more slices).
 * Provides a sequence of (row key, columns).
 *
 * EXAMPLE: 
 * val cluster = new Cluster("127.0.0.1").keyspace("foo")
 *   .connect().columnFamily("bar", Utf8Codec, Utf8Codec, Utf8Codec)
 * val finished = cf.rowIteratee(100).foreach { case(key, columns)} =>
 *   println(key) //this function is executed async for each row
 *   println(cols)
 * }
 * finished() //this is a Future[Unit]. wait on it to know when the iteration is done
 */
case class RowsIteratee[Key, Name, Value](cf: ColumnFamily[Key, Name, Value],
                                            startKey: Option[Key],
                                            endKey: Option[Key],
                                            batchSize: Int,
                                            predicate: thrift.SlicePredicate,
                                            buffer: JList[(Key, JList[Column[Name, Value]])] = Nil: JList[(Key, JList[Column[Name, Value]])],
                                            cycled: Boolean = false,
                                            skip: Option[Key] = None) {

  def foreach(f: (Key, JList[Column[Name, Value]]) => Unit): Future[Unit] = {
    val p = new Promise[Unit]
    next map (_.visit(p, f))
    p
  }

  private def visit(p: Promise[Unit], f: (Key, JList[Column[Name, Value]]) => Unit): Unit = {
    if (buffer.size > 0) {
      for((key, columns) <- buffer){
        f(key, columns)
      }
    }
    if (hasNext) {
      next map {n =>
        n.visit(p, f)
      }
    } else {
      p.setValue(Unit)
    }
  }

  /** Copy constructors for next() and end() cases. */
  private def end(buffer: JList[(Key, JList[Column[Name, Value]])]) = copy(cycled = true, buffer = buffer)
  private def next(buf: JList[(Key, JList[Column[Name, Value]])],
                      startKey: Key) =
    copy(startKey = Some(startKey), skip = Some(startKey), buffer = buf)

  /** @return True if calling next() will request another batch of data. */
  private def hasNext() = !cycled && buffer.size > 0
  /**
   * If hasNext == true, requests the next batch of data, otherwise throws
   * UnsupportedOperationException.
   * @return a future that can contain [[org.apache.cassandra.finagle.thrift.TimedOutException]],
   *  [[org.apache.cassandra.finagle.thrift.UnavailableException]] or [[org.apache.cassandra.finagle.thrift.InvalidRequestException]]
   */
  private def next(): Future[RowsIteratee[Key, Name, Value]] = {
    if (cycled)
      throw new UnsupportedOperationException("No more results.")

    requestNextSlice().map { slice =>
      val skipped = if (!slice.isEmpty && cf.keyCodec.decode(slice.head.key) == skip.orNull) slice.tail else slice.toSeq
      val buf:JList[(Key, JList[Column[Name, Value]])] = new JArrayList[(Key, JList[Column[Name, Value]])](skipped.size)
      skipped.foreach { ks =>
        val key = cf.keyCodec.decode(ks.key)
        val cols = new JArrayList[Column[Name, Value]](ks.columns.size)
        ks.columns.foreach { col =>
          cols.add(Column.convert(cf.nameCodec, cf.valueCodec, col))
        }
        buf.add((key, cols))
      }
      // the last found key, or the end key if the slice was empty
      val lastFoundKey = slice.lastOption.map{r =>
        cf.keyCodec.decode(r.key)}.getOrElse(endKey.getOrElse(cf.keyCodec.decode(ByteBufferUtil.EMPTY)))
      if (lastFoundKey == endKey)
        // no more content: end with last batch
        end(buf)
      else
        // clone the iteratee with a new buffer and start key
        next(buf, lastFoundKey)
    }
  }

  private def requestNextSlice(): Future[JList[thrift.KeySlice]] = {
    val effectiveSize = if (skip.isDefined) batchSize else batchSize + 1
    cf.getRangeSlice(startKey.getOrElse(cf.keyCodec.decode(ByteBufferUtil.EMPTY)), endKey.getOrElse(cf.keyCodec.decode(ByteBufferUtil.EMPTY)), effectiveSize, predicate)
  }
}
