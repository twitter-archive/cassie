package com.twitter.cassie.codecs

import java.nio.ByteBuffer

/**
  * Encodes and decodes 64-bit integers as 8-byte, big-endian byte arrays. */
object LongCodec extends Codec[Long] {
  private val length = 8

  def encode(v: Long) = {
    val b = ByteBuffer.allocate(length)
    b.putLong(v)
    b.rewind
    b
  }

  def decode(buf: ByteBuffer) = {
    require(buf.remaining == length)
    buf.duplicate.getLong
  }
}
