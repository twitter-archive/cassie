package com.codahale.cassie.codecs.tests

import com.codahale.cassie.codecs.tests.ByteBufferLiteral._
import org.scalatest.Spec
import org.scalatest.matchers.MustMatchers
import com.codahale.cassie.codecs.VarIntCodec
import com.codahale.cassie.types.VarInt

class VarIntCodecTest extends Spec with MustMatchers {
  describe("encoding an int") {
    it("produces a variable length zig-zag encoded array of bytes") {
      VarIntCodec.encode(VarInt(199181)) must equal(bb(-102, -88, 24))
    }
  }

  describe("decoding an array of bytes") {
    it("produces an int") {
      VarIntCodec.decode(bb(-102, -88, 24)) must equal(VarInt(199181))
    }
  }
}
