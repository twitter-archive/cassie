package com.codahale.cassie.codecs.tests

import org.scalatest.Spec
import org.scalatest.matchers.MustMatchers
import com.codahale.cassie.codecs.Utf8Codec

class Utf8CodecTest extends Spec with MustMatchers {
  describe("encoding a string") {
    it("produces a UTF-8 encoded array of bytes") {
      Utf8Codec.encode("123").toList must equal(List[Byte](49, 50, 51))
    }
  }

  describe("decoding an array of bytes") {
    it("produces a string") {
      Utf8Codec.decode(Array(49, 50, 51)) must equal("123")
    }
  }
}
