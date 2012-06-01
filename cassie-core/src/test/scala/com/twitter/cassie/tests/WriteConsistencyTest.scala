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

import com.twitter.cassie.WriteConsistency
import org.apache.cassandra.finagle.thrift.ConsistencyLevel
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.matchers.MustMatchers
import org.scalatest.FunSpec


@RunWith(classOf[JUnitRunner])
class WriteConsistencyTest extends FunSpec with MustMatchers {
  describe("a write consistency of Any") {
    it("is human readable") {
      WriteConsistency.Any.toString must equal("WriteConsistency.Any")
    }

    it("has a corresponding Thrift ConsistencyLevel of ANY") {
      WriteConsistency.Any.level must equal(ConsistencyLevel.ANY)
    }
  }

  describe("a write consistency of One") {
    it("is human readable") {
      WriteConsistency.One.toString must equal("WriteConsistency.One")
    }

    it("has a corresponding Thrift ConsistencyLevel of ONE") {
      WriteConsistency.One.level must equal(ConsistencyLevel.ONE)
    }
  }

  describe("a write consistency of Quorum") {
    it("is human readable") {
      WriteConsistency.Quorum.toString must equal("WriteConsistency.Quorum")
    }

    it("has a corresponding Thrift ConsistencyLevel of QUORUM") {
      WriteConsistency.Quorum.level must equal(ConsistencyLevel.QUORUM)
    }
  }

  describe("a write consistency of All") {
    it("is human readable") {
      WriteConsistency.All.toString must equal("WriteConsistency.All")
    }

    it("has a corresponding Thrift ConsistencyLevel of ALL") {
      WriteConsistency.All.level must equal(ConsistencyLevel.ALL)
    }
  }
}
