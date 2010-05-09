package com.codahale.cassie

import org.scalatest.Spec
import org.scalatest.matchers.MustMatchers

class FNV1ATest extends Spec with MustMatchers {
  describe("the FNV1A hash function") {
    it("matches up with existing implementations") {
      println("%x".format(FNV1A("foobar".getBytes)))
      FNV1A("foobar".getBytes) must equal(0x85944171f73967e8L)
    }
  }
}
