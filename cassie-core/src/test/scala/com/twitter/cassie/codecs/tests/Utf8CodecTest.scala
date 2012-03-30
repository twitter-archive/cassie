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

import com.twitter.cassie.codecs.tests.ByteBufferLiteral._
import com.twitter.cassie.codecs.Utf8Codec
import org.scalacheck.Prop
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class Utf8CodecTest extends CodecTest {
  describe("encoding a string") {
    it("produces a UTF-8 encoded array of bytes") {
      Utf8Codec.encode("123") must equal(bb(49, 50, 51))
    }
  }

  describe("decoding an array of bytes") {
    it("produces a string") {
      Utf8Codec.decode(bb(49, 50, 51)) must equal("123")
    }
  }

  check(Prop.forAll(unicodeString) { i: String => Utf8Codec.decode(Utf8Codec.encode(i)) == i })
}
