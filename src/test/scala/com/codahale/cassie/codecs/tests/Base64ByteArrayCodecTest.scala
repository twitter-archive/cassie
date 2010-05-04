package com.codahale.cassie.codecs.tests

import org.scalatest.Spec
import org.scalatest.matchers.MustMatchers
import com.codahale.cassie.codecs.Base64ByteArrayCodec

class Base64ByteArrayCodecTest extends Spec with MustMatchers {
  describe("encoding an array of bytes") {
    it("produces a base64-encoded string") {
      new String(Base64ByteArrayCodec.encode("one two".getBytes)) must equal("b25lIHR3bw==")
    }
  }

  describe("decoding a base64-encoded string") {
    it("produces a byte array") {
      new String(Base64ByteArrayCodec.decode("b25lIHR3bw==".getBytes)) must equal("one two")
    }
  }
}
