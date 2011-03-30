package com.twitter.cassie.codecs

import java.nio.ByteBuffer
import com.twitter.cassie.types.HexByteArray
import org.apache.commons.codec.binary.Hex

/**
 * Encodes and decodes values as hexadecimal strings.
 */
object HexByteArrayCodec extends Codec[HexByteArray] {
  private val hex = new Hex

  def encode(obj: HexByteArray) = b2b(hex.encode(obj.value))
  def decode(ary: ByteBuffer) = HexByteArray(hex.decode(b2b(ary)))
}
