package com.codahale.cassie.types.tests

import org.scalatest.Spec
import org.scalatest.matchers.MustMatchers
import com.codahale.cassie.types.VarLong

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
}
