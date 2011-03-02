package com.twitter.cassie.tests

import org.scalatest.matchers.MustMatchers
import org.scalatest.Spec
import com.twitter.cassie.Cluster
import org.scalatest.mock.MockitoSugar
import com.twitter.cassie.connection.ClientProvider
import com.twitter.conversions.time._

class ClusterTest extends Spec with MustMatchers with MockitoSugar {
  describe("a cluster") {
    val cluster = new Cluster("nonhost")

    it("creates a keyspace with the given name and provider") {
      val ks = cluster.keyspace("poop").mapHostsEvery(0.minutes).connect()

      ks.name must equal("poop")
    }
  }
}
