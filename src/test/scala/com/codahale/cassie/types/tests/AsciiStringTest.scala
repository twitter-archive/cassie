package com.codahale.cassie.types.tests

import com.codahale.cassie.types.AsciiString
import org.scalatest.Spec
import org.scalatest.matchers.MustMatchers

class AsciiStringTest extends Spec with MustMatchers {
  describe("an AsciiString") {
    it("can be implicitly converted to a String") {
      val s: String = AsciiString("woo hoo")

      s must equal("woo hoo")
    }
  }

  describe("a String") {
    it("can be implicitly converted to an AsciiString") {
      val s: AsciiString = "woo hoo"

      s.value must equal("woo hoo")
    }
  }
}
