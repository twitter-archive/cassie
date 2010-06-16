package com.codahale.cassie.connection.tests

import org.scalatest.matchers.MustMatchers
import org.mockito.Mockito.{when, verify, never}
import org.mockito.Matchers.{any, anyString}
import org.scalatest.{OneInstancePerTest, Spec}
import com.codahale.cassie.connection.{ClientFactory, Connection}
import org.apache.cassandra.thrift.Cassandra.Client
import org.scalatest.mock.MockitoSugar
import org.apache.thrift.transport.TTransportException
import org.apache.cassandra.thrift.{ConsistencyLevel, ColumnPath, TimedOutException}
import com.codahale.logula.Logging
import java.util.logging.Level

class ConnectionTest extends Spec with MustMatchers with MockitoSugar with OneInstancePerTest {
  Logging.configure(Level.OFF)

  describe("a closed connection") {
    val client = mock[Client]
    when(client.describe_version).thenReturn("moof")

    val factory = mock[ClientFactory]
    when(factory.build).thenReturn(client)

    val connection = new Connection(factory)

    it("is not open") {
      connection.isOpen must be(false)
    }

    it("is not healthy") {
      connection.isHealthy() must be(false)
    }

    it("connects to the server when required") {
      connection.map { c => c.describe_version } must equal(Some("moof"))
      connection.isOpen must be(true)

      verify(client).describe_version
    }
  }

  describe("an open connection") {
    val client = mock[Client]
    when(client.describe_version).thenReturn("moof")

    val factory = mock[ClientFactory]
    when(factory.build).thenReturn(client)

    val connection = new Connection(factory)
    connection.map { c => c.describe_keyspaces }

    it("returns None and disconnects if a request fails due to a transport error") {
      when(client.describe_version).thenThrow(new TTransportException("AUGH"))

      connection.map { c => c.describe_version } must equal(None)

      connection.isOpen must be(false)
      verify(factory).destroy(client)
    }

    it("returns None if a request times out") {
      when(client.get(anyString, anyString, any(classOf[ColumnPath]), any(classOf[ConsistencyLevel]))).thenThrow(new TimedOutException)

      val cp = new ColumnPath("cf")
      connection.map { c => c.get("herp", "derp", cp, ConsistencyLevel.ALL) } must equal(None)

      connection.isOpen must be(true)
      verify(factory, never).destroy(client)
    }

    it("is healthy if no errors are raised during describe_version") {
      connection.isHealthy() must be(true)

      verify(client).describe_version()
    }

    it("is not healthy if an error is raised during describe_version") {
      when(client.describe_version).thenThrow(new TTransportException("WOOWHA"))

      connection.isHealthy() must be(false)

      verify(client).describe_version()
    }
  }
}
