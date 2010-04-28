package com.codahale.cassie.codecs.tests

import org.scalatest.Spec
import org.scalatest.matchers.MustMatchers
import com.codahale.cassie.codecs.{FixedLong, FixedLongCodec}

class FixedLongCodecTest extends Spec with MustMatchers {
  describe("encoding a long") {
    it("produces a variable length zig-zag encoded array of bytes") {
      FixedLongCodec.encode(FixedLong(199181989101092820L)).toList must equal(List[Byte](2, -61, -94, -10, -70, 6, -65, -44))
    }
  }

  describe("decoding an array of bytes") {
    it("produces a long") {
      FixedLongCodec.decode(Array(2, -61, -94, -10, -70, 6, -65, -44)) must equal(FixedLong(199181989101092820L))
    }
  }
}
