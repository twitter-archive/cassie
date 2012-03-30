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

import com.twitter.cassie.codecs.LongCodec
import com.twitter.cassie.codecs.tests.ByteBufferLiteral._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class LongCodecTest extends CodecTest {
  describe("encoding a long") {
    it("produces a variable length zig-zag encoded array of bytes") {
      LongCodec.encode(199181989101092820L) must equal(bb(2, -61, -94, -10, -70, 6, -65, -44))
    }
  }

  describe("decoding an array of bytes") {
    it("produces a long") {
      LongCodec.decode(bb(2, -61, -94, -10, -70, 6, -65, -44)) must equal(199181989101092820L)
    }
  }

  check { i: Long => LongCodec.decode(LongCodec.encode(i)) == i }
}
