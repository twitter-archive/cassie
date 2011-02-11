package com.twitter.cassie.codecs.tests

import com.twitter.cassie.codecs.tests.ByteBufferLiteral._
import org.scalatest.Spec
import org.scalatest.matchers.MustMatchers
import com.twitter.cassie.codecs._
import com.twitter.cassie.test.thrift.Person

class ThriftCodecTest extends Spec with MustMatchers {
  describe("encoding a person") {
    it("must be decodable") {
      val codec = new ThriftCodec(classOf[Person])
      val person = new Person("joe", "doe")
      val bytes = codec.encode(person)
      codec.decode(bytes) must equal(person)
    }
  }
}
