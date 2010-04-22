package com.codahale.cassie.codecs.tests

import org.scalatest.Spec
import org.scalatest.matchers.MustMatchers
import com.codahale.cassie.codecs.AsciiCodec

class AsciiCodecTest extends Spec with MustMatchers {
  describe("encoding a string") {
    it("produces an US-ASCII encoded array of bytes") {
      AsciiCodec.encode("123").toList must equal(List[Byte](49, 50, 51))
    }
  }

  describe("decoding an array of bytes") {
    it("produces a string") {
      AsciiCodec.decode(Array(49, 50, 51)) must equal("123")
    }
  }
}
