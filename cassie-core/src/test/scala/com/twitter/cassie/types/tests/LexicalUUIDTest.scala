// Copyright 2012 Twitter, Inc.

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

// http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.twitter.cassie.types.tests

import com.twitter.cassie.clocks.Clock
import com.twitter.cassie.types.LexicalUUID
import org.scalatest.matchers.MustMatchers
import org.scalatest.Spec

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
    val clock = new Clock {
      def timestamp = 19910019L
    }

    val uuid = new LexicalUUID(clock, 1001)

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
