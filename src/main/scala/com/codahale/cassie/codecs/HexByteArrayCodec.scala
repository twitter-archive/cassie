package com.codahale.cassie.codecs

import com.codahale.cassie.types.HexByteArray
import org.apache.commons.codec.binary.Hex

/**
 * Encodes and decodes values as hexadecimal strings.
 *
 * @author coda
 */
object HexByteArrayCodec extends Codec[HexByteArray] {
  private val hex = new Hex

  def encode(obj: HexByteArray) = hex.encode(obj.value)
  def decode(ary: Array[Byte]) = HexByteArray(hex.decode(ary))
}
