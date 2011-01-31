package com.twitter.cassie.tests

import org.scalatest.matchers.MustMatchers
import org.scalatest.Spec
import com.twitter.cassie.Cluster
import org.scalatest.mock.MockitoSugar
import com.twitter.cassie.connection.ClientProvider

class ClusterTest extends Spec with MustMatchers with MockitoSugar {
  describe("a cluster") {
    val cluster = new Cluster("nonhost")

    it("creates a keyspace with the given name and provider") {
      val ks = cluster.keyspace("poop").performMapping(false).connect()

      ks.name must equal("poop")
    }
  }
}
