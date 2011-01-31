package com.twitter.cassie.codecs

import com.twitter.cassie.types.LexicalUUID
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
    b.rewind
    b
  }

  def decode(buf: ByteBuffer) = {
    require(buf.remaining == length)
    val dupe = buf.duplicate
    LexicalUUID(dupe.getLong(), dupe.getLong())
  }
}
