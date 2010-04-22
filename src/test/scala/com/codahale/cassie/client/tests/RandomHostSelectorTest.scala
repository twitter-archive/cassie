package com.codahale.cassie.client.tests

import org.scalatest.Spec
import org.scalatest.matchers.MustMatchers
import org.scalatest.mock.MockitoSugar
import java.net.InetSocketAddress
import org.mockito.Mockito.when
import com.codahale.cassie.client.{RandomHostSelector, ClusterMap}

class RandomHostSelectorTest extends Spec with MustMatchers with MockitoSugar {
  describe("getting another host") {
    val hosts = Set(new InetSocketAddress("c1.exmaple.com", 10000), new InetSocketAddress("c2.example.com", 10000), new InetSocketAddress("c3.example.com", 10000))

    val clusterMap = mock[ClusterMap]
    when(clusterMap.hosts).thenReturn(hosts)

    val selector = new RandomHostSelector(clusterMap)

    it("returns a random node") {
      1.to(10).foreach { i =>
        hosts.contains(selector.next) must be(true)
      }
    }
  }
}
