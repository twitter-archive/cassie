package com.codahale.cassie.codecs.tests

import org.scalatest.Spec
import org.scalatest.matchers.MustMatchers
import com.codahale.cassie.codecs.VarLongCodec
import com.codahale.cassie.types.VarLong

class VarLongCodecTest extends Spec with MustMatchers {
  describe("encoding a long") {
    it("produces a variable length zig-zag encoded array of bytes") {
      VarLongCodec.encode(VarLong(199181989101092820L)).toList must equal(List[Byte](-88, -1, -75, -96, -41, -67, -47, -61, 5))
    }
  }

  describe("decoding an array of bytes") {
    it("produces a long") {
      VarLongCodec.decode(Array(-88, -1, -75, -96, -41, -67, -47, -61, 5)) must equal(VarLong(199181989101092820L))
    }
  }
}
