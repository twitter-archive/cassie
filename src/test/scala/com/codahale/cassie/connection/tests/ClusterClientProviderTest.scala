package com.codahale.cassie.connection.tests

import org.scalatest.matchers.MustMatchers
import org.scalatest.{BeforeAndAfterAll, Spec}
import org.mockito.Mockito.{when, verify, times}
import java.nio.ByteBuffer
import com.codahale.logula.Logging
import org.apache.log4j.Level
import com.codahale.cassie.connection.ClusterClientProvider
import com.codahale.cassie.tests.util.MockCassandraServer
import java.net.InetSocketAddress
import org.apache.cassandra.thrift.{TimedOutException, ColumnOrSuperColumn, ColumnPath, ConsistencyLevel}

class ClusterClientProviderTest extends Spec with MustMatchers with BeforeAndAfterAll {
  val cp = new ColumnPath("cf")
  val cosc = new ColumnOrSuperColumn
  val keyBytes = ByteBuffer.wrap("key".getBytes)

  val server1 = new MockCassandraServer(MockCassandraServer.choosePort())
  when(server1.cassandra.describe_version).thenReturn("node1")
  when(server1.cassandra.get(keyBytes, cp, ConsistencyLevel.ALL)).thenReturn(cosc)

  val server2 = new MockCassandraServer(MockCassandraServer.choosePort())
  when(server2.cassandra.describe_version).thenReturn("node2")
  when(server2.cassandra.get(keyBytes, cp, ConsistencyLevel.ALL)).thenReturn(cosc)

  val server3 = new MockCassandraServer(MockCassandraServer.choosePort())
  when(server3.cassandra.describe_version).thenReturn("node3")
  when(server3.cassandra.get(keyBytes, cp, ConsistencyLevel.ALL)).thenThrow(new TimedOutException())

  val hosts = Set(
    new InetSocketAddress("127.0.0.1", server1.port),
    new InetSocketAddress("127.0.0.1", server2.port),
    new InetSocketAddress("127.0.0.1", server3.port)
  )

  override protected def beforeAll() {
    server1.start()
    server2.start()
    server3.start()
  }

  override protected def afterAll() {
    server1.stop()
    server2.stop()
    server3.stop()
  }

  describe("a cluster") {
    val provider = new ClusterClientProvider(
      hosts,
      "spaceman",
      retryAttempts = 3,
      readTimeoutInMS = 10000,
      partialFailureThreshold = 2,
      downTimeoutInMS = 1000,
      minConnectionsPerHost = 2,
      maxConnectionsPerHost = 5,
      removeAfterIdleForMS = 10*60*1000
    )
    Logging.configure(_.level = Level.OFF)

    it("balances requests between nodes") {
      provider.map { c => c.describe_version } must equal("node1")
      provider.map { c => c.describe_version } must equal("node2")
      provider.map { c => c.describe_version } must equal("node3")
      provider.map { c => c.describe_version } must equal("node1")
      provider.map { c => c.describe_version } must equal("node2")
      provider.map { c => c.describe_version } must equal("node3")

    }

    it("handles nodes which are down gracefully") {
      provider.map { c => c.get(keyBytes, cp, ConsistencyLevel.ALL) } must equal(cosc)
      provider.map { c => c.get(keyBytes, cp, ConsistencyLevel.ALL) } must equal(cosc)
      provider.map { c => c.get(keyBytes, cp, ConsistencyLevel.ALL) } must equal(cosc)
      provider.map { c => c.get(keyBytes, cp, ConsistencyLevel.ALL) } must equal(cosc)
      provider.map { c => c.get(keyBytes, cp, ConsistencyLevel.ALL) } must equal(cosc)
      provider.map { c => c.get(keyBytes, cp, ConsistencyLevel.ALL) } must equal(cosc)
      provider.map { c => c.get(keyBytes, cp, ConsistencyLevel.ALL) } must equal(cosc)
      provider.map { c => c.get(keyBytes, cp, ConsistencyLevel.ALL) } must equal(cosc)
      provider.map { c => c.get(keyBytes, cp, ConsistencyLevel.ALL) } must equal(cosc)
      provider.map { c => c.get(keyBytes, cp, ConsistencyLevel.ALL) } must equal(cosc)

      verify(server1.cassandra, times(5)).get(keyBytes, cp, ConsistencyLevel.ALL)
      verify(server2.cassandra, times(5)).get(keyBytes, cp, ConsistencyLevel.ALL)
      verify(server3.cassandra, times(2)).get(keyBytes, cp, ConsistencyLevel.ALL)
    }
  }
}
