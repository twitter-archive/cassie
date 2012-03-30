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

package com.twitter.cassie.codecs.tests

import com.twitter.cassie.codecs.LexicalUUIDCodec
import com.twitter.cassie.codecs.tests.ByteBufferLiteral._
import com.twitter.cassie.types.LexicalUUID
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class LexicalUUIDCodecTest extends CodecTest {
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

  check { (i: Long, j: Long) =>
    LexicalUUIDCodec.decode(LexicalUUIDCodec.encode(LexicalUUID(i, j))) == LexicalUUID(i, j)
  }
}
