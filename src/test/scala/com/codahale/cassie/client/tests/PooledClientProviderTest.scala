package com.codahale.cassie.client.tests

import scalaj.collection.Imports._
import org.scalatest.matchers.MustMatchers
import org.scalatest.mock.MockitoSugar
import org.scalatest.{BeforeAndAfterAll, Spec}
import org.mockito.Mockito.when
import com.codahale.cassie.client.{ClusterMap, RoundRobinHostSelector, PooledClientProvider}
import java.net.InetSocketAddress
import org.scalatest.concurrent.Conductor
import java.util.concurrent.CopyOnWriteArraySet
import com.codahale.cassie.tests.util.MockCassandraServer

class PooledClientProviderTest
        extends Spec with MustMatchers with MockitoSugar with BeforeAndAfterAll {
  
  val server1 = new MockCassandraServer(MockCassandraServer.choosePort())
  when(server1.cassandra.describe_version).thenReturn("one")

  val server2 = new MockCassandraServer(MockCassandraServer.choosePort())
  when(server2.cassandra.describe_version).thenReturn("two")

  val server3 = new MockCassandraServer(MockCassandraServer.choosePort())
  when(server3.cassandra.describe_version).thenReturn("three")

  override protected def beforeAll() {
    Seq(server1, server2, server3).foreach { _.start() }
  }

  override protected def afterAll() {
    Seq(server1, server2, server3).foreach { _.stop() }
  }

  describe("a cluster-wide pooled client provider with round-robin node selection") {
    val map = mock[ClusterMap]
    when(map.hosts).thenReturn(Set(new InetSocketAddress(server1.port), new InetSocketAddress(server2.port), new InetSocketAddress(server3.port)))
    val provider = new PooledClientProvider(new RoundRobinHostSelector(map), 3, 5, 10)

    it("connects to all servers") {
      val conductor = new Conductor
      val versions = new CopyOnWriteArraySet[String]

      for (i <- 1 to 10) {
        conductor.thread("thread-"+i) {
          versions.add(provider.map { c => c.describe_version})
        }
      }

      conductor.whenFinished {
        versions.asScala must equal(Set("one", "two", "three"))
      }
    }
  }
}
