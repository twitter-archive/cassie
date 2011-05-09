package com.twitter.cassie.codecs

import java.nio.ByteBuffer
import java.nio.charset.Charset
import com.twitter.cassie.types.AsciiString

/**
  * Encodes and decodes values as US-ASCII strings. */
object AsciiStringCodec extends Codec[AsciiString] {
  // TODO: should this be threadlocal to prevent contention?
  // TODO: figure out if the above TODO is legit. is there a lock in Charset encode or decode?
  private val usAscii = Charset.forName("US-ASCII")

  def encode(obj: AsciiString) = { val buf = usAscii.encode(obj.value); buf.rewind; buf }
  def decode(ary: ByteBuffer) = AsciiString(usAscii.decode(ary.duplicate()).toString)
}
