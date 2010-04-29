package com.codahale.cassie.types.tests

import org.scalatest.Spec
import org.scalatest.matchers.MustMatchers
import com.codahale.cassie.types.FixedLong

class FixedLongTest extends Spec with MustMatchers {
  describe("a FixedLong") {
    it("can be implicitly converted to a Long") {
      val i: Long = FixedLong(12311L)

      i must equal(12311L)
    }
  }

  describe("a Long") {
    it("can be implicitly converted to a FixedLong") {
      val i: FixedLong = 12311L

      i.value must equal(12311L)
    }
  }
}
