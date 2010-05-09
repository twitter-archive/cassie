package com.codahale.cassie.codecs

import com.codahale.cassie.types.LexicalUUID
import java.nio.ByteBuffer

/**
 * Encodes and decodes UUIDs as 128-bit values.
 *
 * @author coda
 */
object LexicalUUIDCodec extends Codec[LexicalUUID] {
  private val length = 16

  def encode(uuid: LexicalUUID) = {
    val b = ByteBuffer.allocate(length)
    b.putLong(uuid.timestamp)
    b.putLong(uuid.workerID)
    b.array
  }

  def decode(buf: Array[Byte]) = {
    require(buf.length == length)
    val b = ByteBuffer.wrap(buf)
    LexicalUUID(b.getLong(), b.getLong())
  }
}
