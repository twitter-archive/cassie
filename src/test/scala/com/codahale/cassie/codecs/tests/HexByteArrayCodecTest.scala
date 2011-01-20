package com.codahale.cassie.codecs.tests

import com.codahale.cassie.codecs.tests.ByteBufferLiteral._
import org.scalatest.Spec
import org.scalatest.matchers.MustMatchers
import com.codahale.cassie.codecs.HexByteArrayCodec

class HexByteArrayCodecTest extends Spec with MustMatchers {
  describe("encoding an array of bytes") {
    it("produces a hexadecimal string") {
      new String(HexByteArrayCodec.encode("one two".getBytes).array) must equal("6f6e652074776f")
    }
  }

  describe("decoding a hexidecimal string") {
    it("produces a byte array") {
      new String(HexByteArrayCodec.decode(bb("6f6e652074776f".getBytes: _*))) must equal("one two")
    }
  }
}
