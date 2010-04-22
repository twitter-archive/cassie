package com.codahale.cassie.codecs

import java.lang.String
import java.nio.charset.Charset
import java.nio.ByteBuffer

/**
 * Encodes and decodes values as UTF-8 strings.
 *
 * @author coda
 */
object Utf8Codec extends Codec[String] {
  private val utf8 = Charset.forName("UTF-8")

  def encode(obj: String) = obj.getBytes(utf8)
  def decode(ary: Array[Byte]) = utf8.decode(ByteBuffer.wrap(ary)).toString
}
