package com.twitter.cassie.codecs

import com.twitter.cassie.types.FixedInt
import java.nio.{IntBuffer, LongBuffer, ByteBuffer}

/**
 * Encodes and decodes 32-bit integers as 4-byte, big-endian byte arrays.
 *
 * @author coda
 */
object FixedIntCodec extends Codec[FixedInt] {
  private val length = 4

  def encode(v: FixedInt) = {
    val buf = ByteBuffer.allocate(length)
    buf.putInt(v.value)
    buf.rewind
    buf
  }

  def decode(buf: ByteBuffer) = {
    require(buf.remaining == length)
    FixedInt(buf.duplicate().getInt)
  }
}
