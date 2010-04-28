package com.codahale.cassie.codecs

import java.nio.ByteBuffer
import java.nio.charset.Charset

/**
 * A US-ASCII string.
 *
 * @author coda
 */
case class AsciiString(value: String)

/**
 * Encodes and decodes values as US-ASCII strings.
 *
 * @author coda
 */
object AsciiCodec extends Codec[AsciiString] {
  private val usAscii = Charset.forName("US-ASCII")

  def encode(obj: AsciiString) = obj.value.getBytes(usAscii)
  def decode(ary: Array[Byte]) = AsciiString(usAscii.decode(ByteBuffer.wrap(ary)).toString)
}
