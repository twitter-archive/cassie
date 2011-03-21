package com.twitter.cassie.codecs

import java.nio.ByteBuffer
import java.lang.String
import org.apache.thrift.bootleg.Utf8Helper

/**
 * Encodes and decodes values as UTF-8 strings.
 *
 * @author coda
 */
object Utf8Codec extends Codec[String] {
  def encode(s: String) = b2b(Utf8Helper.encode(s))
  def decode(ary: ByteBuffer) = {
    println(b2b(ary).length)
    Utf8Helper.decode(b2b(ary))
  }
}
