package com.twitter.cassie.codecs

import java.nio.ByteBuffer
import org.apache.thrift.bootleg.Utf8Helper

/**
  * Encodes and decodes values as UTF-8 strings. */
object Utf8Codec extends Codec[String] {
  def encode(s: String)       = b2b(s.getBytes("UTF-8"))
  def decode(ary: ByteBuffer) = new String(ary.array, ary.position, ary.remaining, "UTF-8")
}
