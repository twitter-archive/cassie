package com.codahale.cassie.codecs

import java.nio.ByteBuffer
import java.nio.charset.Charset

/**
 * Encodes and decodes values as US-ASCII strings.
 *
 * @author coda
 */
object AsciiCodec extends Codec[String] {
  private val usAscii = Charset.forName("US-ASCII")

  def encode(obj: String) = obj.getBytes(usAscii)
  def decode(ary: Array[Byte]) = usAscii.decode(ByteBuffer.wrap(ary)).toString
}
