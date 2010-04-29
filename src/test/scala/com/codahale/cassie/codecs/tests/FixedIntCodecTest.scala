package com.codahale.cassie.codecs.tests

import org.scalatest.Spec
import org.scalatest.matchers.MustMatchers
import com.codahale.cassie.codecs.FixedIntCodec
import com.codahale.cassie.types.FixedInt

class FixedIntCodecTest extends Spec with MustMatchers {
  describe("encoding an int") {
    it("produces a variable length zig-zag encoded array of bytes") {
      FixedIntCodec.encode(FixedInt(199181)).toList must equal(List[Byte](0, 3, 10, 13))
    }
  }

  describe("decoding an array of bytes") {
    it("produces an int") {
      FixedIntCodec.decode(Array(0, 3, 10, 13)) must equal(FixedInt(199181))
    }
  }
}
