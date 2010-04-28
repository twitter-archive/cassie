package com.codahale.cassie

import codecs._

/**
 * Implicit parameters and conversions for all the types supported by Cassie.
 *
 * @author coda
 */
package object codecs {
  implicit val utf8 = Utf8Codec

  implicit val ascii = AsciiCodec
  implicit def String2AsciiString(value: String): AsciiString = AsciiString(value)

  implicit val bytes = ByteArrayCodec

  implicit val fixedInt = FixedIntCodec
  implicit def Int2FixedInt(value: Int): FixedInt = FixedInt(value)

  implicit val fixedLong = FixedLongCodec
  implicit def Long2FixedLong(value: Long): FixedLong = FixedLong(value)

  implicit val varInt = VarIntCodec
  implicit def Int2FVarInt(value: Int): VarInt = VarInt(value)

  implicit val varLong = VarLongCodec
  implicit def Long2VarLong(value: Long): VarLong = VarLong(value)
}
