package com.codahale.cassie.codecs

/**
 * Encodes and decodes 32-bit integers using Avro's zig-zag variable-length
 * encoding.
 *
 * @author coda
 */
object VarIntCodec extends Codec[Int] {
  private val maxLength = 5

  def encode(obj: Int) = {
    var n = obj
    val b = Array.fill[Byte](maxLength)(0)
    var pos = 0

    n = (n << 1) ^ (n >> 63) // move sign to low-order bit
    while ((n & ~0x7F) != 0) {

      b(pos) = ((n & 0x7f) | 0x80).toByte
      n >>>= 7
      pos += 1
    }
    b(pos) = n.toByte
    b.take(pos+1)
  }

  def decode(buf: Array[Byte]) = {
    require(buf.length < maxLength)
    val pos = 0
    var len = 1
    var b: Int = buf(pos) & 0xff
    var n: Int = b & 0x7f
    if (b > 0x7f) {
      b = buf(pos + len) & 0xff
      len += 1
      n ^= (b & 0x7f) << 7
      if (b > 0x7f) {
        b = buf(pos + len) & 0xff
        len += 1
        n ^= (b & 0x7f) << 14
        if (b > 0x7f) {
          b = buf(pos + len) & 0xff
          len += 1
          n ^= (b & 0x7f) << 21
          if (b > 0x7f) {
            b = buf(pos + len) & 0xff
            len += 1
            n ^= (b & 0x7f) << 28
          }
          if (b > 0x7f) {
            error("Invalid int encoding")
          }
        }
      }
    }
    (n >>> 1) ^ -(n & 1) // back to two's-complement
  }
}
