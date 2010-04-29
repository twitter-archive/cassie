package com.codahale.cassie.codecs

import com.codahale.cassie.types.FixedInt

/**
 * Encodes and decodes 32-bit integers as 4-byte, big-endian byte arrays.
 *
 * @author coda
 */
object FixedIntCodec extends Codec[FixedInt] {
  private val maxLength = 4

  def encode(v: FixedInt) = {
    val b = Array.fill[Byte](maxLength)(0)
    val n = v.value
    b(0) = (n >>> 24).toByte
    b(1) = (n >>> 16).toByte
    b(2) = (n >>>  8).toByte
    b(3) = (n >>>  0).toByte

    b
  }

  def decode(buf: Array[Byte]) = {
    require(buf.length == maxLength)
    FixedInt(
      ((buf(0) & 255) << 24) +
      ((buf(1) & 255) << 16) +
      ((buf(2) & 255) <<  8) +
      ((buf(3) & 255) <<  0)
    )
  }
}
