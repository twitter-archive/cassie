package com.twitter.cassie.codecs.tests

import com.twitter.cassie.codecs.tests.ByteBufferLiteral._
import org.scalatest.Spec
import org.scalatest.matchers.MustMatchers
import com.twitter.cassie.codecs.Base64ByteArrayCodec

class Base64ByteArrayCodecTest extends Spec with MustMatchers {
  describe("encoding an array of bytes") {
    it("produces a base64-encoded string") {
      new String(Base64ByteArrayCodec.encode("one two".getBytes).array) must equal("b25lIHR3bw==")
    }
  }

  describe("decoding a base64-encoded string") {
    it("produces a byte array") {
      new String(Base64ByteArrayCodec.decode(bb("b25lIHR3bw==".getBytes: _*))) must equal("one two")
    }
  }
}
