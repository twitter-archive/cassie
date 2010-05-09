package com.codahale.cassie.codecs

import com.codahale.cassie.types.FixedInt
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
    buf.array
  }

  def decode(buf: Array[Byte]) = {
    require(buf.length == length)
    FixedInt(ByteBuffer.wrap(buf).getInt)
  }
}
