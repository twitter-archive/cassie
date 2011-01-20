package com.codahale.cassie.codecs.tests

import com.codahale.cassie.codecs.tests.ByteBufferLiteral._
import org.scalatest.Spec
import org.scalatest.matchers.MustMatchers
import com.codahale.cassie.types.LexicalUUID
import com.codahale.cassie.codecs.LexicalUUIDCodec

class LexicalUUIDCodecTest extends Spec with MustMatchers {
  val uuid = LexicalUUID(0x990213812L, 0x899813298123L)
  val bytes = bb(0, 0, 0, 9, -112, 33, 56, 18, 0, 0, -119, -104, 19, 41, -127, 35)

  describe("encoding a UUID") {
    it("produces a 16-byte array") {
      LexicalUUIDCodec.encode(uuid) must equal(bytes)
    }
  }

  describe("decoding a UUID") {
    it("produces a LexicalUUID") {
      LexicalUUIDCodec.decode(bytes) must equal(uuid)
    }
  }
}
