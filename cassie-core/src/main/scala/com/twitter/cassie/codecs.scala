package com.twitter.cassie

import codecs._

/**
 * Implicit parameters for codecs for all the types supported by Cassie.
 */
object Codecs {
  implicit val utf8 = Utf8Codec
  implicit val ascii = AsciiStringCodec
  implicit val base64 = Base64ByteArrayCodec
  implicit val bytes = ByteArrayCodec
  implicit val fixedInt = FixedIntCodec
  implicit val fixedLong = FixedLongCodec
  implicit val hex = HexByteArrayCodec
  implicit val varInt = VarIntCodec
  implicit val varLong = VarLongCodec
  implicit val lexicalUUID = LexicalUUIDCodec
}
