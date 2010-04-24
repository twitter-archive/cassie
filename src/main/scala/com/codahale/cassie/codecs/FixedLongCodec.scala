package com.codahale.cassie.codecs

/**
 * Encodes and decodes 64-bit integers as 8-byte, big-endian byte arrays.
 *
 * @author coda
 */
object FixedLongCodec extends Codec[Long] {
  private val maxLength = 8

  def encode(v: Long) = {
    val b = Array.fill[Byte](maxLength)(0)

    b(0) = (v >>> 56).toByte
    b(1) = (v >>> 48).toByte
    b(2) = (v >>> 40).toByte
    b(3) = (v >>> 32).toByte
    b(4) = (v >>> 24).toByte
    b(5) = (v >>> 16).toByte
    b(6) = (v >>>  8).toByte
    b(7) = (v >>>  0).toByte

    b
  }

  def decode(buf: Array[Byte]) = {
    require(buf.length == maxLength)
    ((buf(0).toLong & 255) << 56) +
      ((buf(1).toLong & 255) << 48) +
      ((buf(2).toLong & 255) << 40) +
      ((buf(3).toLong & 255) << 32) +
      ((buf(4).toLong & 255) << 24) +
      ((buf(5).toLong & 255) << 16) +
      ((buf(6).toLong & 255) << 8) +
      ((buf(7).toLong & 255) << 0)
  }
}
