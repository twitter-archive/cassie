package com.codahale.cassie.codecs

/**
 * A combined set of codecs.
 *
 * @author coda
 */
object Codecs {
  implicit val utf8 = Utf8Codec
  implicit val ascii = AsciiCodec
  implicit val bytes = ByteArrayCodec
  implicit val fixedInt = FixedIntCodec
  implicit val fixedLong = FixedLongCodec
  implicit val varInt = VarIntCodec
  implicit val varLong = VarLongCodec
}
