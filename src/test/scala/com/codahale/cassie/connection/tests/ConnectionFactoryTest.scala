package com.codahale.cassie.connection.tests

import org.scalatest.Spec
import org.scalatest.matchers.MustMatchers
import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito.{when, verify}
import com.codahale.cassie.connection.{Connection, ConnectionFactory, ClientFactory}
import com.codahale.logula.Logging
import org.apache.log4j.Level

class ConnectionFactoryTest extends Spec with MustMatchers with MockitoSugar {
  describe("a connection factory") {
    Logging.configure(_.level = Level.OFF)
    val clientFactory = mock[ClientFactory]
    val factory = new ConnectionFactory(clientFactory)

    describe("making a new connection") {
      it("creates a new connection") {
        val conn = factory.makeObject
        conn.factory must be(clientFactory)
      }
    }

    describe("validating a connection") {
      val conn = mock[Connection]

      it("returns true if the connection is healthy") {
        when(conn.isHealthy).thenReturn(true)

        factory.validateObject(conn) must be(true)
      }

      it("returns false if the connection is not healthy") {
        when(conn.isHealthy).thenReturn(false)

        factory.validateObject(conn) must be(false)
      }

      it("returns false if the argument is not a connection") {
        factory.validateObject("moo") must be(false)
      }
    }

    describe("destroying a connection") {
      val conn = mock[Connection]

      it("closes the connection") {
        factory.destroyObject(conn)

        verify(conn).close()
      }
    }

    describe("destroying a non-connection") {
      it("does nothing") {
        factory.destroyObject("moo")
      }
    }
  }
}
