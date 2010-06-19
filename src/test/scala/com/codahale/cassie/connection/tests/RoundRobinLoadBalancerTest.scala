package com.codahale.cassie.connection.tests

import org.scalatest.matchers.MustMatchers
import org.scalatest.mock.MockitoSugar
import org.scalatest.{OneInstancePerTest, Spec}
import org.mockito.Mockito.{when, inOrder}
import org.apache.cassandra.thrift.Cassandra.Client
import com.codahale.cassie.connection.{UnsuccessfulQueryException, FailureAwareConnectionPool, RoundRobinLoadBalancer}

class RoundRobinLoadBalancerTest extends Spec
        with MustMatchers with MockitoSugar with OneInstancePerTest {

  val f: (Client => String) = { i => i.describe_version }

  def nodes = (
    mock[FailureAwareConnectionPool],
    mock[FailureAwareConnectionPool],
    mock[FailureAwareConnectionPool]
  )


  describe("a round-robin load balancer with three nodes") {
    val (node1, node2, node3) = nodes
    when(node1.map(f)).thenReturn(Some("node1"))
    when(node2.map(f)).thenReturn(Some("node2"))
    when(node3.map(f)).thenReturn(Some("node3"))
    val balancer = new RoundRobinLoadBalancer(Set(node1, node2, node3), 6)

    it("balances queries across all three nodes in round-robin fashion") {
      balancer.map(f) must equal("node1")
      balancer.map(f) must equal("node2")
      balancer.map(f) must equal("node3")
      balancer.map(f) must equal("node1")
      balancer.map(f) must equal("node2")
      balancer.map(f) must equal("node3")
    }
  }

  describe("a round-robin load balancer with two good nodes and one bad node") {
    val (node1, node2, node3) = nodes
    when(node1.map(f)).thenReturn(Some("node1"))
    when(node2.map(f)).thenReturn(Some("node2"))
    when(node3.map(f)).thenReturn(None)
    val balancer = new RoundRobinLoadBalancer(Set(node1, node2, node3), 6)

    it("balances queries across all three nodes in round-robin fashion") {
      balancer.map(f) must equal("node1")
      balancer.map(f) must equal("node2")
      balancer.map(f) must equal("node1")
      balancer.map(f) must equal("node2")
      balancer.map(f) must equal("node1")
      balancer.map(f) must equal("node2")
    }
  }

  describe("a round-robin load balancer with three bad nodes") {
    val (node1, node2, node3) = nodes
    when(node1.map(f)).thenReturn(None)
    when(node2.map(f)).thenReturn(None)
    when(node3.map(f)).thenReturn(None)
    val balancer = new RoundRobinLoadBalancer(Set(node1, node2, node3), 6)

    it("tries the query a specific number of times and throws an exception") {
      evaluating {
        balancer.map(f)
      } must produce[UnsuccessfulQueryException]


      val order = inOrder(node1, node2, node3)
      order.verify(node1).map(f)
      order.verify(node2).map(f)
      order.verify(node3).map(f)
      order.verify(node1).map(f)
      order.verify(node2).map(f)
      order.verify(node3).map(f)
    }
  }
}
