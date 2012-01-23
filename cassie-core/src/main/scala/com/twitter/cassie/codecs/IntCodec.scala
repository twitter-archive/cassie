package com.twitter.cassie.codecs

import java.nio.ByteBuffer

/**
 * Encodes and decodes 32-bit integers as 4-byte, big-endian byte buffers.
 */
object IntCodec extends Codec[Int] {
  private val length = 4

  def encode(v: Int) = {
    val buf = ByteBuffer.allocate(length)
    buf.putInt(v)
    buf.rewind
    buf
  }

  def decode(buf: ByteBuffer) = {
    require(buf.remaining == length)
    buf.duplicate().getInt
  }
}
