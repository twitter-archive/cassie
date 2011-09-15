package com.twitter.cassie.codecs.tests

import com.twitter.cassie.codecs.tests.ByteBufferLiteral._
import com.twitter.cassie.codecs.ByteArrayCodec
import java.nio.ByteBuffer
import org.scalacheck._
import org.scalacheck.Prop

class ByteArrayCodecTest extends CodecTest {
  describe("encoding an array of bytes") {
    it("produces an array of bytes") {
      ByteArrayCodec.encode(bb(1, 2, 3)) must equal(bb(1, 2, 3))
    }
  }

  describe("decoding an array of bytes") {
    it("produces an array of bytes") {
      ByteArrayCodec.decode(bb(49, 50, 51)) must equal(bb(49, 50, 51))
    }
  }

  check(Prop.forAll(randomBuffer){ i: ByteBuffer => ByteArrayCodec.decode(ByteArrayCodec.encode(i)) == i })
}
