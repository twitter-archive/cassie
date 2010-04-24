package com.codahale.cassie.codecs.tests


import org.scalatest.Spec
import org.scalatest.matchers.MustMatchers
import com.codahale.cassie.codecs.VarIntCodec

class VarIntCodecTest extends Spec with MustMatchers {
  describe("encoding an int") {
    it("produces a variable length zig-zag encoded array of bytes") {
      VarIntCodec.encode(199181).toList must equal(List[Byte](-102, -88, 24))
    }
  }

  describe("decoding an array of bytes") {
    it("produces an int") {
      VarIntCodec.decode(Array(-102, -88, 24)) must equal(199181)
    }
  }
}
