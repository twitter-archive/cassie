package com.twitter.cassie

import java.nio.ByteBuffer
import java.util.{List => JList}
import scala.collection.JavaConversions._
import com.twitter.util.Future

import codecs.Codec

import com.twitter.logging.Logger
import org.apache.cassandra.finagle.thrift

/**
 * Given a column family, a key range, a batch size, a slice predicate, and
 * a consistency level, iterates through each matching column of each matching
 * key until a cycle is detected (e.g., Cassandra returns the last slice a
 * second time) or until an empty slice is returned (e.g., no more slices).
 * Provides a sequence of (row key, column).
 * TODO an example
 */
case class ColumnIteratee[Key, Name, Value](cf: ColumnFamily[_, _, _], //TODO make this a CFL
                                            startKey: ByteBuffer,
                                            endKey: ByteBuffer,
                                            batchSize: Int,
                                            predicate: thrift.SlicePredicate,
                                            keyCodec: Codec[Key],
                                            nameCodec: Codec[Name],
                                            valueCodec: Codec[Value],
                                            buffer: JList[(Key, Column[Name, Value])] = Nil: JList[(Key, Column[Name, Value])],
                                            cycled: Boolean = false,
                                            skip: Option[ByteBuffer] = None)
        extends java.lang.Iterable[(Key, Column[Name, Value])] {
  val log = Logger.get

  /** Copy constructors for next() and end() cases. */
  private def end(buffer: JList[(Key, Column[Name, Value])]) = copy(cycled = true, buffer = buffer)
  private def next(buffer: JList[(Key, Column[Name, Value])],
                      startKey: ByteBuffer) =
    copy(startKey = startKey, skip = Some(startKey), buffer = buffer)

  /** @return True if calling next() will request another batch of data. */
  def hasNext() = !cycled
  /**
   * If hasNext == true, requests the next batch of data, otherwise throws
   * UnsupportedOperationException.
   * @return a future that can contain [[org.apache.cassandra.finagle.thrift.TimedOutException]],
   *  [[org.apache.cassandra.finagle.thrift.UnavailableException]] or [[org.apache.cassandra.finagle.thrift.InvalidRequestException]]
   */
  def next(): Future[ColumnIteratee[Key, Name, Value]] = {
    if (cycled)
      throw new UnsupportedOperationException("No more results.")

    requestNextSlice().map { slice =>
      val skipped = if (!slice.isEmpty && slice.head.key == skip.orNull) slice.tail else slice.toSeq
      val buffer = skipped.flatMap { ks =>
        asScalaIterable(ks.columns).map { col =>
          keyCodec.decode(ks.key) -> Column.convert(nameCodec, valueCodec, col)
        }
      }
      // the last found key, or the end key if the slice was empty
      val lastFoundKey = slice.lastOption.map { _.key }.getOrElse(endKey)
      if (lastFoundKey == endKey)
        // no more content: end with last batch
        end(buffer)
      else
        // clone the iteratee with a new buffer and start key
        next(buffer, lastFoundKey)
    }
  }

  /**
   * @return An Iterator which will consume this Iteratee synchronously.
   */
  def iterator() = new ColumnIterator(this)

  private def requestNextSlice(): Future[JList[thrift.KeySlice]] = {
    val effectiveSize = if (skip.isDefined) batchSize else batchSize + 1
    cf.getRangeSlice(startKey, endKey, effectiveSize, predicate)
  }
}
