package com.codahale.cassie.types.tests

import org.scalatest.Spec
import org.scalatest.matchers.MustMatchers
import com.codahale.cassie.types.Base64ByteArray

class Base64ByteArrayTest extends Spec with MustMatchers {
  val ary = Array[Byte](1, 2, 3)

  describe("a Base64ByteArray") {
    it("can be implicitly converted to an Array[Byte]") {
      val a: Array[Byte] = Base64ByteArray(ary)

      a must equal(ary)
    }
  }

  describe("an Array[Byte]") {
    it("can be implicitly converted to a Base64ByteArray") {
      val a: Base64ByteArray = ary

      a.value must equal(ary)
    }
  }
}
