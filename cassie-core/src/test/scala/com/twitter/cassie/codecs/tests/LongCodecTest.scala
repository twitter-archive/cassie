package com.twitter.cassie.codecs.tests

import com.twitter.cassie.codecs.tests.ByteBufferLiteral._
import org.scalatest.Spec
import org.scalatest.matchers.MustMatchers
import com.twitter.cassie.codecs.LongCodec

class LongCodecTest extends Spec with MustMatchers {
  describe("encoding a long") {
    it("produces a variable length zig-zag encoded array of bytes") {
      LongCodec.encode(199181989101092820L) must equal(bb(2, -61, -94, -10, -70, 6, -65, -44))
    }
  }

  describe("decoding an array of bytes") {
    it("produces a long") {
      LongCodec.decode(bb(2, -61, -94, -10, -70, 6, -65, -44)) must equal(199181989101092820L)
    }
  }
}
