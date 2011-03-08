package com.twitter.cassie.types.tests

import org.scalatest.Spec
import org.scalatest.matchers.MustMatchers
import com.twitter.cassie.types.FixedInt

class FixedIntTest extends Spec with MustMatchers {
  describe("a FixedInt") {
    it("can be implicitly converted to an Int") {
      val i: Int = FixedInt(12311)

      i must equal(12311)
    }
  }

  describe("an Int") {
    it("can be implicitly converted to a FixedInt") {
      val i: FixedInt = 12311

      i.value must equal(12311)
    }
  }
}
