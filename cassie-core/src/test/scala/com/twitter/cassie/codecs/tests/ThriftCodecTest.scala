package com.twitter.cassie.codecs.tests

import com.twitter.cassie.codecs.tests.ByteBufferLiteral._
import com.twitter.cassie.codecs.ThriftCodec
import com.twitter.cassie.test.thrift.Person
import org.scalacheck._

class ThriftCodecTest extends CodecTest {
  describe("encoding a person") {
    it("must be decodable") {
      val codec = new ThriftCodec(classOf[Person])
      val person = new Person("joe", "doe")
      val bytes = codec.encode(person)
      codec.decode(bytes) must equal(person)

      // We do this 2x to verify that we aren't introducing bugs with object reuse
      val another = new Person("john", "doe")
      val moreBytes = codec.encode(another)
      codec.decode(moreBytes) must equal(another)
    }
  }

  check(Prop.forAll(unicodeString, unicodeString) { (fname: String, lname: String) => 
    val p = new Person(fname, lname)
    val codec = new ThriftCodec(classOf[Person])
    codec.decode(codec.encode(p)) == p
  })
}
