package com.codahale.cassie.tests

import org.scalatest.matchers.MustMatchers
import org.scalatest.Spec
import com.codahale.cassie.Cluster
import org.scalatest.mock.MockitoSugar
import com.codahale.cassie.connection.ClientProvider

class ClusterTest extends Spec with MustMatchers with MockitoSugar {
  describe("a cluster") {
    val provider = mock[ClientProvider]
    val cluster = new Cluster(provider)

    it("creates a keyspace with the given name and provider") {
      val ks = cluster.keyspace("poop")

      ks.name must equal("poop")
      ks.provider must equal(provider)
    }
  }
}
