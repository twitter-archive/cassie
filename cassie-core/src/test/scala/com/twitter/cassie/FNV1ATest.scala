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

package com.twitter.cassie

import org.scalatest.matchers.MustMatchers
import org.scalatest.Spec

class FNV1ATest extends Spec with MustMatchers {
  describe("the FNV1A hash function") {
    it("matches up with existing implementations") {
      FNV1A("foobar".getBytes) must equal(0x85944171f73967e8L)
    }
  }
}
