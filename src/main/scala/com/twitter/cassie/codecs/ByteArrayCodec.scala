package com.twitter.cassie.codecs

import java.nio.ByteBuffer

/**
 * An identity encoding.
 *
 * TODO: Fix name.
 *
 * @author coda
 */
object ByteArrayCodec extends Codec[ByteBuffer] {
  def encode(obj: ByteBuffer) = obj
  def decode(ary: ByteBuffer) = ary
}
