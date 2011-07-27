package com.twitter.cassie.tests

import org.scalatest.matchers.MustMatchers
import org.scalatest.Spec
import com.twitter.cassie.Cluster
import org.scalatest.mock.MockitoSugar
import com.twitter.conversions.time._

class ClusterTest extends Spec with MustMatchers with MockitoSugar {
  describe("a cluster") {
    val cluster = new Cluster("nonhost").mapHostsEvery(0.minutes)

    it("creates a keyspace with the given name and provider") {
      val ks = cluster.mapHostsEvery(0.minutes).keyspace("ks").connect()

      ks.name must equal("ks")
    }
  }
}
