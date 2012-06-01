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

import com.twitter.cassie.Cluster
import com.twitter.conversions.time._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.matchers.MustMatchers
import org.scalatest.mock.MockitoSugar
import org.scalatest.FunSpec

@RunWith(classOf[JUnitRunner])
class ClusterTest extends FunSpec with MustMatchers with MockitoSugar {
  describe("a cluster") {
    val cluster = new Cluster("nonhost").mapHostsEvery(0.minutes)

    it("creates a keyspace with the given name and provider") {
      val ks = cluster.keyspace("ks").connect()

      ks.name must equal("ks")
    }
  }
}
