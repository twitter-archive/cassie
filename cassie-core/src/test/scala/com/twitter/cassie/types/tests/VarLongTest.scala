package com.twitter.cassie.types.tests

import org.scalatest.Spec
import org.scalatest.matchers.MustMatchers
import com.twitter.cassie.types.VarLong
import com.twitter.cassie.codecs.VarLongCodec

class VarLongTest extends Spec with MustMatchers {
  describe("a VarLong") {
    it("can be implicitly converted to a Long") {
      val i: Long = VarLong(12311L)

      i must equal(12311L)
    }
  }

  describe("a Long") {
    it("can be implicitly converted to a VarLong") {
      val i: VarLong = 12311L

      i.value must equal(12311L)
    }
  }

  describe("encoding MaxValue") {
    it("shouldn't throw an exception") {
      val b = VarLongCodec.encode(VarLong(Long.MaxValue))
      VarLongCodec.decode(b) must equal(VarLong(Long.MaxValue))
    }
  }

  describe("encoding MinValue") {
    it("shouldn't throw an exception") {
      val b = VarLongCodec.encode(VarLong(Long.MinValue))
      VarLongCodec.decode(b) must equal(VarLong(Long.MinValue))
    }
  }

}
