package com.codahale.cassie.codecs.tests

import com.codahale.cassie.codecs.tests.ByteBufferLiteral._
import org.scalatest.Spec
import org.scalatest.matchers.MustMatchers
import com.codahale.cassie.codecs.AsciiStringCodec
import com.codahale.cassie.types.AsciiString

class AsciiStringCodecTest extends Spec with MustMatchers {
  describe("encoding a string") {
    it("produces an US-ASCII encoded array of bytes") {
      AsciiStringCodec.encode(AsciiString("123")) must equal(bb(49, 50, 51))
    }
  }

  describe("decoding an array of bytes") {
    it("produces a string") {
      AsciiStringCodec.decode(bb(49, 50, 51)) must equal(AsciiString("123"))
    }
  }
}
