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

import com.twitter.cassie.codecs.IntCodec
import com.twitter.cassie.codecs.tests.ByteBufferLiteral._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class IntCodecTest extends CodecTest {
  describe("encoding an int") {
    it("produces a variable length zig-zag encoded array of bytes") {
      IntCodec.encode(199181) must equal(bb(0, 3, 10, 13))
    }
  }

  describe("decoding an array of bytes") {
    it("produces an int") {
      IntCodec.decode(bb(0, 3, 10, 13)) must equal(199181)
    }
  }

  check { i: Int => IntCodec.decode(IntCodec.encode(i)) == i }
}
