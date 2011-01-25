package com.codahale.cassie.connection.tests

import org.scalatest.matchers.MustMatchers
import org.mockito.Matchers.{eq => matchEq}
import org.scalatest.{BeforeAndAfterAll, Spec}
import com.codahale.cassie.tests.util.MockCassandraServer
import org.mockito.Mockito.{when, verify}
import java.net.InetSocketAddress
import org.scalatest.mock.MockitoSugar
import org.apache.cassandra.thrift.Cassandra.Client
import org.apache.thrift.protocol.TProtocol
import com.codahale.cassie.connection.ClientFactory
import org.mockito.stubbing.Answer
import org.mockito.invocation.InvocationOnMock
import org.apache.thrift.transport.{TTransportException, TTransport}

class ClientFactoryTest extends Spec with MustMatchers with BeforeAndAfterAll with MockitoSugar {
  val server = new MockCassandraServer(MockCassandraServer.choosePort())
  when(server.cassandra.describe_version).thenReturn("moof")
  when(server.cassandra.describe_cluster_name).thenAnswer(new Answer[String] {
    def answer(invocation: InvocationOnMock) = {
      Thread.sleep(5000)
      "poop"
    }
  })

  override protected def beforeAll() {
    server.start()
  }

  override protected def afterAll() {
    server.stop()
  }

  val factory = new ClientFactory(new InetSocketAddress("127.0.0.1", server.port), "spaceman", 1000)

  describe("a client factory building a client") {
    it("connects to the host") {
      val client = factory.build
      
      client.describe_version must equal("moof")
    }

    it("sets the keyspace") {
      verify(server.cassandra).set_keyspace(matchEq("spaceman"))
    }
  }

  describe("a client created by a factory with a timeout") {
    it("throws an exception when the server doesn't respond for the length of the timeout") {
      val client = factory.build

      evaluating { client.describe_cluster_name } must produce[TTransportException]
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
