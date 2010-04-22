package com.codahale.cassie.tests

import org.scalatest.Spec
import org.scalatest.matchers.MustMatchers
import org.scalatest.mock.MockitoSugar
import com.codahale.cassie.client.ClientProvider
import com.codahale.cassie.Cluster

class ClusterTest extends Spec with MustMatchers with MockitoSugar {
  describe("a cluster") {
    val provider = mock[ClientProvider]
    val cluster = new Cluster(provider)

    it("builds a keyspace") {
      val ks = cluster.keyspace("Woo")

      ks.name must equal("Woo")
      ks.provider must equal(provider)
    }
  }
}
