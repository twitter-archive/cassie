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

import com.twitter.cassie.ReadConsistency
import org.apache.cassandra.finagle.thrift.ConsistencyLevel
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.matchers.MustMatchers
import org.scalatest.FunSpec

@RunWith(classOf[JUnitRunner])
class ReadConsistencyTest extends FunSpec with MustMatchers {
  describe("a read consistency of One") {
    it("is human readable") {
      ReadConsistency.One.toString must equal("ReadConsistency.One")
    }

    it("has a corresponding Thrift ConsistencyLevel of ONE") {
      ReadConsistency.One.level must equal(ConsistencyLevel.ONE)
    }
  }

  describe("a read consistency of Quorum") {
    it("is human readable") {
      ReadConsistency.Quorum.toString must equal("ReadConsistency.Quorum")
    }

    it("has a corresponding Thrift ConsistencyLevel of QUORUM") {
      ReadConsistency.Quorum.level must equal(ConsistencyLevel.QUORUM)
    }
  }

  describe("a read consistency of All") {
    it("is human readable") {
      ReadConsistency.All.toString must equal("ReadConsistency.All")
    }

    it("has a corresponding Thrift ConsistencyLevel of ALL") {
      ReadConsistency.All.level must equal(ConsistencyLevel.ALL)
    }
  }
}
