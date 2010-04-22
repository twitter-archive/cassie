package com.codahale.cassie.client.tests

import org.scalatest.matchers.MustMatchers
import org.scalatest.{BeforeAndAfterAll, Spec}
import org.mockito.Mockito.when
import com.codahale.cassie.client.SingleClientProvider
import java.net.InetSocketAddress
import com.codahale.cassie.tests.util.MockCassandraServer

class SingleClientProviderTest extends Spec with MustMatchers with BeforeAndAfterAll {
  val server = new MockCassandraServer(MockCassandraServer.choosePort())
  when(server.cassandra.describe_version).thenReturn("moof")

  override protected def beforeAll() {
    server.start()
  }

  override protected def afterAll() {
    server.stop()
  }

  describe("connecting to a Cassandra server") {
    val singleClient = new SingleClientProvider(new InetSocketAddress("127.0.0.1", server.port))

    it("sends requests and receives responses") {
      singleClient.map { client => client.describe_version } must equal("moof")
    }
  }
}
