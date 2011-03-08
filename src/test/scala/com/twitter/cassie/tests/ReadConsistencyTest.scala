package com.twitter.cassie.tests

import org.scalatest.Spec
import org.scalatest.matchers.MustMatchers
import com.twitter.cassie.ReadConsistency
import org.apache.cassandra.finagle.thrift.ConsistencyLevel

class ReadConsistencyTest extends Spec with MustMatchers {
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
