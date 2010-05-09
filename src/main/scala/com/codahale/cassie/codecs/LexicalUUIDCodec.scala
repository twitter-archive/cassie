package com.codahale.cassie.codecs

import com.codahale.cassie.types.LexicalUUID

/**
 * Encodes and decodes UUIDs as 128-bit values.
 *
 * @author coda
 */
object LexicalUUIDCodec extends Codec[LexicalUUID] {
  private val length = 16

  // TODO: this and FixedLongCodec share rather a lot of logic

  def encode(uuid: LexicalUUID) = {
    val b = Array.fill[Byte](length)(0)
    val t = uuid.timestamp
    b( 0) = (t >>> 56).toByte
    b( 1) = (t >>> 48).toByte
    b( 2) = (t >>> 40).toByte
    b( 3) = (t >>> 32).toByte
    b( 4) = (t >>> 24).toByte
    b( 5) = (t >>> 16).toByte
    b( 6) = (t >>>  8).toByte
    b( 7) = (t >>>  0).toByte

    val w = uuid.workerID
    b( 8) = (w >>> 56).toByte
    b( 9) = (w >>> 48).toByte
    b(10) = (w >>> 40).toByte
    b(11) = (w >>> 32).toByte
    b(12) = (w >>> 24).toByte
    b(13) = (w >>> 16).toByte
    b(14) = (w >>>  8).toByte
    b(15) = (w >>>  0).toByte

    b
  }

  def decode(buf: Array[Byte]) = {
    require(buf.length == length)
    val t = ((buf( 0).toLong & 255) << 56) +
            ((buf( 1).toLong & 255) << 48) +
            ((buf( 2).toLong & 255) << 40) +
            ((buf( 3).toLong & 255) << 32) +
            ((buf( 4).toLong & 255) << 24) +
            ((buf( 5).toLong & 255) << 16) +
            ((buf( 6).toLong & 255) <<  8) +
            ((buf( 7).toLong & 255) <<  0)
    val w = ((buf( 8).toLong & 255) << 56) +
            ((buf( 9).toLong & 255) << 48) +
            ((buf(10).toLong & 255) << 40) +
            ((buf(11).toLong & 255) << 32) +
            ((buf(12).toLong & 255) << 24) +
            ((buf(13).toLong & 255) << 16) +
            ((buf(14).toLong & 255) <<  8) +
            ((buf(15).toLong & 255) <<  0)
    LexicalUUID(t, w)
  }
}
