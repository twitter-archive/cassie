package com.codahale.cassie.types.tests

import org.scalatest.Spec
import org.scalatest.matchers.MustMatchers
import com.codahale.cassie.types.VarInt

class VarIntTest extends Spec with MustMatchers {
  describe("a VarInt") {
    it("can be implicitly converted to an Int") {
      val i: Int = VarInt(12311)

      i must equal(12311)
    }
  }

  describe("an Int") {
    it("can be implicitly converted to a VarInt") {
      val i: VarInt = 12311

      i.value must equal(12311)
    }
  }
}
