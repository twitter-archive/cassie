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

package com.twitter.cassie.tests

import com.twitter.cassie.Column
import com.twitter.conversions.time._
import org.scalatest.matchers.MustMatchers
import org.scalatest.Spec

class ColumnTest extends Spec with MustMatchers {
  describe("a column with an explicit ttl") {
    val col = Column("id", 300).timestamp(400L).ttl(1.minute)

    it("has a name") {
      col.name must equal("id")
    }

    it("has a value") {
      col.value must equal(300)
    }

    it("has a timestamp") {
      col.timestamp must equal(Some(400L))
    }

    it("has a ttl") {
      col.ttl must equal(Some(60.seconds))
    }

  }
}
