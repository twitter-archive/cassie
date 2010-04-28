package com.codahale.cassie

import codecs._

/**
 * A collection of codecs for all the types supported by Cassie.
 *
 * @author coda
 */
package object codecs {
  implicit val utf8 = Utf8Codec
  implicit val ascii = AsciiCodec
  implicit val bytes = ByteArrayCodec
  implicit val fixedInt = FixedIntCodec
  implicit val fixedLong = FixedLongCodec
  implicit val varInt = VarIntCodec
  implicit val varLong = VarLongCodec
}
