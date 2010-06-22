package com.codahale.cassie.codecs

import java.lang.String
import org.apache.thrift.bootleg.Utf8Helper

/**
 * Encodes and decodes values as UTF-8 strings.
 *
 * @author coda
 */
object Utf8Codec extends Codec[String] {
  def encode(s: String) = Utf8Helper.encode(s)
  def decode(ary: Array[Byte]) = Utf8Helper.decode(ary)
}
