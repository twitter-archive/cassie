package com.codahale.cassie.codecs

/**
 * Encodes and decodes 32-bit integers as 4-byte, big-endian byte arrays.
 *
 * @author coda
 */
object FixedIntCodec extends Codec[Int] {
  private val maxLength = 4

  def encode(v: Int) = {
    val b = Array.fill[Byte](maxLength)(0)

    b(0) = (v >>> 24).toByte
    b(1) = (v >>> 16).toByte
    b(2) = (v >>>  8).toByte
    b(3) = (v >>>  0).toByte

    b
  }

  def decode(buf: Array[Byte]) = {
    require(buf.length == maxLength)
    ((buf(0) & 255) << 24) +
      ((buf(1) & 255) << 16) +
      ((buf(2) & 255) << 8) +
      ((buf(3) & 255) << 0)
  }
}
