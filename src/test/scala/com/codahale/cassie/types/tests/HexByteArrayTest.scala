package com.codahale.cassie.types.tests

import org.scalatest.Spec
import org.scalatest.matchers.MustMatchers
import com.codahale.cassie.types.HexByteArray

class HexByteArrayTest extends Spec with MustMatchers {
    val ary = Array[Byte](1, 2, 3)

  describe("a HexByteArray") {
    it("can be implicitly converted to an Array[Byte]") {
      val a: Array[Byte] = HexByteArray(ary)

      a must equal(ary)
    }
  }

  describe("an Array[Byte]") {
    it("can be implicitly converted to a HexByteArray") {
      val a: HexByteArray = ary

      a.value must equal(ary)
    }
  }
}
