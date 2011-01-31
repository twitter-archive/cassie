package com.twitter.cassie.codecs.tests

import com.twitter.cassie.codecs.tests.ByteBufferLiteral._
import org.scalatest.Spec
import org.scalatest.matchers.MustMatchers
import com.twitter.cassie.codecs.ByteArrayCodec

class ByteArrayCodecTest extends Spec with MustMatchers {
  describe("encoding an array of bytes") {
    it("produces an array of bytes") {
      ByteArrayCodec.encode(bb(1, 2, 3)) must equal(bb(1, 2, 3))
    }
  }

  describe("decoding an array of bytes") {
    it("produces an array of bytes") {
      ByteArrayCodec.decode(bb(49, 50, 51)) must equal(bb(49, 50, 51))
    }
  }
}
