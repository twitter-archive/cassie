package com.codahale.cassie.codecs

import java.nio.ByteBuffer
import com.codahale.cassie.types.HexByteArray
import org.apache.commons.codec.binary.Hex

/**
 * Encodes and decodes values as hexadecimal strings.
 *
 * @author coda
 */
object HexByteArrayCodec extends Codec[HexByteArray] {
  private val hex = new Hex

  def encode(obj: HexByteArray) = b2b(hex.encode(obj.value))
  def decode(ary: ByteBuffer) = HexByteArray(hex.decode(b2b(ary)))
}
