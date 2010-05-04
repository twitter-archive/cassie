package com.codahale.cassie.codecs

import com.codahale.cassie.types.Base64ByteArray
import org.apache.commons.codec.binary.Base64

/**
 * Encodes and decodes values as Base64-encoded strings.
 *
 * @author coda
 */
object Base64ByteArrayCodec extends Codec[Base64ByteArray] {
  private val b64 = new Base64(0, Array())

  def encode(obj: Base64ByteArray) = b64.encode(obj.value)
  def decode(ary: Array[Byte]) = Base64ByteArray(b64.decode(ary))
}
