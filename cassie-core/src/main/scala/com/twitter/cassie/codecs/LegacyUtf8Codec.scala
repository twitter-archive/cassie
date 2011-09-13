package com.twitter.cassie.codecs

import java.nio.ByteBuffer
import java.lang.String
import org.apache.thrift.bootleg.Utf8Helper

/**
  * Encodes and decodes values as UTF-8 strings. */
object LegacyUtf8Codec extends Codec[String] {
  @deprecated("use the new Utf8Codec")
  def encode(s: String)       = b2b(Utf8Helper.encode(s))
  @deprecated("use the new Utf8Codec")
  def decode(ary: ByteBuffer) = Utf8Helper.decode(b2b(ary))
}
