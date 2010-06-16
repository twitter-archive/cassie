package com.codahale.cassie.connection.tests

import org.scalatest.matchers.MustMatchers
import org.scalatest.{BeforeAndAfterAll, Spec}
import com.codahale.cassie.tests.util.MockCassandraServer
import org.mockito.Mockito.{when, verify}
import java.net.InetSocketAddress
import org.scalatest.mock.MockitoSugar
import org.apache.cassandra.thrift.Cassandra.Client
import org.apache.thrift.protocol.TProtocol
import org.apache.thrift.transport.TTransport
import com.codahale.cassie.connection.ClientFactory

class ClientFactoryTest extends Spec with MustMatchers with BeforeAndAfterAll with MockitoSugar {
  val server = new MockCassandraServer(MockCassandraServer.choosePort())
  when(server.cassandra.describe_version).thenReturn("moof")

  override protected def beforeAll() {
    server.start()
  }

  override protected def afterAll() {
    server.stop()
  }

  val factory = new ClientFactory(new InetSocketAddress("127.0.0.1", server.port))

  describe("a client factory building a client") {
    it("connects to the host") {
      val client = factory.build
      
      client.describe_version must equal("moof")
    }
  }

  describe("a client factory destroying a client") {
    val transport = mock[TTransport]

    val protocol = mock[TProtocol]
    when(protocol.getTransport).thenReturn(transport)

    val client = mock[Client]
    when(client.getOutputProtocol).thenReturn(protocol)

    it("it closes the output protocol's transport") {
      factory.destroy(client)

      verify(transport).close()
    }
  }
}
