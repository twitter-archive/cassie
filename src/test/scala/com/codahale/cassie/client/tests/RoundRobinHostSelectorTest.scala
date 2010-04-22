package com.codahale.cassie.client.tests


import org.scalatest.Spec
import org.scalatest.matchers.MustMatchers
import com.codahale.cassie.client.{ClusterMap, RoundRobinHostSelector}
import java.net.InetSocketAddress
import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito.when

class RoundRobinHostSelectorTest extends Spec with MustMatchers with MockitoSugar {
  describe("getting another host") {
    val host1 = new InetSocketAddress("c1.exmaple.com", 10000)
    val host2 = new InetSocketAddress("c2.example.com", 10000)
    val host3 = new InetSocketAddress("c3.example.com", 10000)
    val hosts = Set(host1, host2, host3)

    val clusterMap = mock[ClusterMap]
    when(clusterMap.hosts).thenReturn(hosts)

    val selector = new RoundRobinHostSelector(clusterMap)

    it("returns nodes in a round-robin fashion") {
      1.to(6).map { i => selector.next } must equal(Seq(host1, host2, host3, host1, host2, host3))
    }
  }
}
