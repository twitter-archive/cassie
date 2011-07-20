package com.twitter.cassie

import java.nio.ByteBuffer
import scala.collection.JavaConversions._
import com.twitter.util.Future

import codecs.Codec

import com.twitter.logging.Logger
import org.apache.cassandra.finagle.thrift
import java.util.{HashMap, Map, Iterator, List => JList}

case class ColumnsIteratee[Key, Name, Value](cf: ColumnFamily[Key, Name, Value], //TODO make this a CFL
                                             key: ByteBuffer,
                                             batchSize: Int,
                                             predicate: thrift.SlicePredicate,
                                             keyCodec: Codec[Key],
                                             nameCodec: Codec[Name],
                                             valueCodec: Codec[Value],
                                             buffer: JList[(Key, Column[Name, Value])] = Nil: JList[(Key, Column[Name, Value])],
                                             cycled: Boolean = false,
                                             skip: Option[ByteBuffer] = None,
                                             startColumn: ByteBuffer = ByteBuffer.wrap(Array[Byte]()))
    extends Iteratee[Key, Name, Value] {
  val log = Logger.get

  /** Copy constructors for next() and end() cases. */
  private def end(buffer: JList[(Key, Column[Name, Value])]) = copy(cycled = true, buffer = buffer)
  private def next(buffer: JList[(Key, Column[Name, Value])], lastFoundColumn: ByteBuffer) =
    copy(startColumn = lastFoundColumn, skip = Some(lastFoundColumn), buffer = buffer)

  private val requestSize = batchSize + skip.size

  /** @return True if calling next() will request another batch of data. */
  def hasNext() = !cycled

  /**
   * If hasNext == true, requests the next batch of data, otherwise throws
   * UnsupportedOperationException.
   * @return a future that can contain [[org.apache.cassandra.finagle.thrift.TimedOutException]],
   *  [[org.apache.cassandra.finagle.thrift.UnavailableException]] or [[org.apache.cassandra.finagle.thrift.InvalidRequestException]]
   */
  def next(): Future[ColumnsIteratee[Key, Name, Value]] = {
    if (cycled) throw new UnsupportedOperationException("No more results.")

    requestNextSlice().map { slice =>
      val buffer = slice.drop(skip.size).map { col =>
        (keyCodec.decode(key) -> col)
      }
      if (buffer.size() == 0) {
        end(Nil: JList[(Key, Column[Name, Value])])
      } else if (buffer.size() < batchSize) {
        end(buffer)
      } else {
        next(buffer, nameCodec.encode(slice.last.name))
      }
    }
  }

  private def requestNextSlice(): Future[Seq[Column[Name, Value]]] = {
    val sr = new thrift.SliceRange
    sr.start = startColumn
    sr.finish = ByteBuffer.wrap(Array[Byte]())
    sr.count = requestSize
    predicate.setSlice_range(sr)
    cf.getOrderedSlice(keyCodec.decode(key), predicate)
  }
}
