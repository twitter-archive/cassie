package com.codahale.cassie.client.tests

import org.scalatest.matchers.MustMatchers
import org.scalatest.mock.MockitoSugar
import org.apache.cassandra.thrift.Cassandra.Client
import org.mockito.Mockito.{when, verify, never}
import org.apache.thrift.TException
import com.codahale.logula.Log
import org.apache.thrift.protocol.TProtocol
import org.apache.thrift.transport.TTransport
import com.codahale.cassie.client.{HostSelector, PooledClientFactory}
import org.scalatest.{BeforeAndAfterAll, Spec}
import java.net.InetSocketAddress
import java.util.logging.Level
import com.codahale.cassie.tests.util.MockCassandraServer

class PooledClientFactoryTest extends Spec with MustMatchers with MockitoSugar with BeforeAndAfterAll {
  Log.forClass(classOf[PooledClientFactory]).level = Level.OFF

  val server = new MockCassandraServer(MockCassandraServer.choosePort())
  when(server.cassandra.describe_version).thenReturn("moof")

  override protected def beforeAll() {
    server.start()
  }

  override protected def afterAll() {
    server.stop()
  }

  describe("validating a working client connection") {
    val client = mock[Client]
    val selector = mock[HostSelector]
    val factory = new PooledClientFactory(selector)

    it("calls describe_version") {
      factory.validateObject(client)

      verify(client).describe_version
    }

    it("returns true") {
      factory.validateObject(client) must be(true)
    }
  }

  describe("validating a broken client connection") {
    val client = mock[Client]
    when(client.describe_version).thenThrow(new TException("NO YUO"))

    val selector = mock[HostSelector]
    val factory = new PooledClientFactory(selector)

    it("calls describe_version") {
      factory.validateObject(client)

      verify(client).describe_version
    }

    it("returns false") {
      factory.validateObject(client) must be(false)
    }
  }

  describe("validating a random object") {
    val selector = mock[HostSelector]
    val factory = new PooledClientFactory(selector)

    it("returns false") {
      factory.validateObject("HI THERE") must be(false)
    }
  }

  describe("destroying an open client connection") {
    val transport = mock[TTransport]
    when(transport.isOpen).thenReturn(true)

    val protocol = mock[TProtocol]
    when(protocol.getTransport).thenReturn(transport)

    val client = mock[Client]
    when(client.getOutputProtocol).thenReturn(protocol)

    val selector = mock[HostSelector]
    val factory = new PooledClientFactory(selector)

    it("closes the connection") {
      factory.destroyObject(client)

      verify(transport).close()
    }
  }

  describe("destroying a closed client connection") {
    val transport = mock[TTransport]
    when(transport.isOpen).thenReturn(false)

    val protocol = mock[TProtocol]
    when(protocol.getTransport).thenReturn(transport)

    val client = mock[Client]
    when(client.getOutputProtocol).thenReturn(protocol)

    val selector = mock[HostSelector]
    val factory = new PooledClientFactory(selector)

    it("does not re-close the connection") {
      factory.destroyObject(client)

      verify(transport, never).close()
    }
  }

  describe("creating a new connection") {
    val selector = mock[HostSelector]
    when(selector.next).thenReturn(new InetSocketAddress("127.0.0.1", server.port))

    val factory = new PooledClientFactory(selector)

    it("connects to the next node provided by the host selector") {
      val client = factory.makeObject.asInstanceOf[Client]
      client.describe_version must equal("moof")
    }
  }
}
