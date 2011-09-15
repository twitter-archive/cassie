package com.twitter.cassie.codecs

import java.nio.ByteBuffer
import java.lang.String
import org.apache.thrift.bootleg.Utf8Helper

/**
  * Encodes and decodes values as UTF-8 strings. */
@deprecated("""Use the new Utf8Codec if you can. You may need to use this for backwards
  compatability with your stored data. This should only be a problem if you 
  use codepoints outside the BMP.""")
object LegacyUtf8Codec extends Codec[String] {
  @deprecated("""Use the new Utf8Codec if you can. You may need to use this for backwards
    compatability with your stored data. This should only be a problem if you 
    use codepoints outside the BMP.""")
  def encode(s: String)       = b2b(Utf8Helper.encode(s))
  @deprecated("""Use the new Utf8Codec if you can. You may need to use this for backwards
    compatability with your stored data. This should only be a problem if you 
    use codepoints outside the BMP.""")
  def decode(ary: ByteBuffer) = Utf8Helper.decode(b2b(ary))
}
