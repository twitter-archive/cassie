package com.codahale.cassie.codecs

import com.codahale.cassie.types.VarLong

import java.nio.ByteBuffer

/**
 * Encodes and decodes 64-bit integers using Avro's zig-zag variable-length
 * encoding.
 *
 * @author coda
 */
object VarLongCodec extends Codec[VarLong] {
  private val maxLength = 10

  def encode(obj: VarLong) = {
    var n = obj.value
    val b = Array.fill[Byte](maxLength)(0)
    var pos = 0

    n = (n << 1) ^ (n >> 63) // move sign to low-order bit
    while ((n & ~0x7F) != 0) {

      b(pos) = ((n & 0x7f) | 0x80).toByte
      n >>>= 7
      pos += 1
    }
    b(pos) = n.toByte
    // TODO: the 'take' would be more efficient as an adjustment of the bb length
    b2b(b.take(pos+1))
  }

  def decode(bytebuf: ByteBuffer) = {
    // TODO: operate directly on the bb
    val buf = b2b(bytebuf)
    require(buf.length < maxLength)
    val pos = 0
    var len = 1
    var b: Long = buf(pos) & 0xff
    var n: Long = b & 0x7f
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
            if (b > 0x7f) {
              b = buf(pos + len) & 0xff
              len += 1
              n ^= (b & 0x7f) << 35
              if (b > 0x7f) {
                b = buf(pos + len) & 0xff
                len += 1
                n ^= (b & 0x7f) << 42
                if (b > 0x7f) {
                  b = buf(pos + len) & 0xff
                  len += 1
                  n ^= (b & 0x7f) << 49
                  if (b > 0x7f) {
                    b = buf(pos + len) & 0xff
                    len += 1
                    n ^= (b & 0x7f) << 56
                    if (b > 0x7f) {
                      b = buf(pos + len) & 0xff
                      len += 1
                      n ^= (b & 0x7f) << 63
                      if (b > 0x7f) {
                        error("Invalid int encoding")
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
    VarLong((n >>> 1) ^ -(n & 1)) // back to two's-complement
  }
}
