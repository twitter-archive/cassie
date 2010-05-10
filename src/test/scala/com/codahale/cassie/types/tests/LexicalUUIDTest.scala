package com.codahale.cassie.types.tests

import org.scalatest.Spec
import org.scalatest.matchers.MustMatchers
import com.codahale.cassie.types.LexicalUUID
import com.codahale.cassie.clocks.Clock

class LexicalUUIDTest extends Spec with MustMatchers {
  describe("a lexical UUID") {
    val uuid = LexicalUUID(0xFF9281, 0xA0091991)

    it("has a timestamp") {
      uuid.timestamp must equal(0xFF9281)
    }

    it("has a worker ID") {
      uuid.workerID must equal(0xA0091991)
    }

    it("is human-readable") {
      uuid.toString must equal("00000000-00ff-9281-ffffffffa0091991")
    }

    it("is convertible to a String") {
      val s: String = uuid

      s must equal("00000000-00ff-9281-ffffffffa0091991")
    }

    it("is convertible from a String") {
      val u: LexicalUUID = "00000000-00ff-9281-ffffffffa0091991"

      u must equal(uuid)
    }
  }

  describe("generating a lexical UUID") {
    implicit val clock = new Clock {
      def timestamp = 19910019L
    }

    val uuid = LexicalUUID(1001)

    it("uses the timestamp from the clock and the provided worker ID") {
      uuid.toString must equal("00000000-012f-cd83-00000000000003e9")
    }
  }

  describe("ordering lexical UUIDs") {
    val uuid1 = LexicalUUID(0xFF9281, 0xA0091991)
    val uuid2 = LexicalUUID(0xFF9281, 0xA0091992)
    val uuid3 = LexicalUUID(0xFF9282, 0xA0091991)

    it("orders by timestamp, then worker ID") {
      val ordered = Seq(uuid2, uuid3, uuid1).sortWith { _ < _ }

      ordered must equal(Seq(uuid1, uuid2, uuid3))
    }
  }
}
