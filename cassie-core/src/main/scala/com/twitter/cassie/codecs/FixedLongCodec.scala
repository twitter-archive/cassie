package com.twitter.cassie.codecs

import com.twitter.cassie.types.FixedLong
import java.nio.ByteBuffer

/**
  * Encodes and decodes 64-bit integers as 8-byte, big-endian byte arrays. */
object FixedLongCodec extends Codec[FixedLong] {
  private val length = 8

  def encode(v: FixedLong) = {
    val b = ByteBuffer.allocate(length)
    b.putLong(v.value)
    b.rewind
    b
  }

  def decode(buf: ByteBuffer) = {
    require(buf.remaining == length)
    FixedLong(buf.duplicate.getLong)
  }
}
