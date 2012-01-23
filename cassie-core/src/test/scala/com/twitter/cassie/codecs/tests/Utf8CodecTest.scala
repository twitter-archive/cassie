package com.twitter.cassie.codecs.tests

import com.twitter.cassie.codecs.tests.ByteBufferLiteral._
import com.twitter.cassie.codecs.Utf8Codec
import org.scalacheck.Prop

class Utf8CodecTest extends CodecTest {
  describe("encoding a string") {
    it("produces a UTF-8 encoded array of bytes") {
      Utf8Codec.encode("123") must equal(bb(49, 50, 51))
    }
  }

  describe("decoding an array of bytes") {
    it("produces a string") {
      Utf8Codec.decode(bb(49, 50, 51)) must equal("123")
    }
  }

  check(Prop.forAll(unicodeString) { i: String => Utf8Codec.decode(Utf8Codec.encode(i)) == i })
}
