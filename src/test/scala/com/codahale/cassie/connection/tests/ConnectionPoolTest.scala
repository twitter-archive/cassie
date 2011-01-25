package com.codahale.cassie.connection.tests

import org.scalatest.matchers.MustMatchers
import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito.when
import org.scalatest.concurrent.Conductor
import com.codahale.cassie.tests.util.MockCassandraServer
import org.scalatest.{BeforeAndAfterAll, Spec}
import com.codahale.cassie.connection.{ClientFactory, ConnectionPool, ConnectionFactory}
import java.net.InetSocketAddress
import com.codahale.logula.Logging
import org.apache.log4j.Level

class ConnectionPoolTest extends Spec
        with MustMatchers with MockitoSugar
        with BeforeAndAfterAll {
  val server = new MockCassandraServer(MockCassandraServer.choosePort())
  when(server.cassandra.describe_version).thenReturn("moof")

  override protected def beforeAll() {
    server.start()
  }

  override protected def afterAll() {
    server.stop()
  }

  describe("a connection pool") {
    Logging.configure(_.level = Level.OFF)

    val clientFactory = new ClientFactory(new InetSocketAddress("0.0.0.0", server.port), "spaceman", 1000)
    val factory = new ConnectionFactory(clientFactory)
    val pool = new ConnectionPool(factory, 2, 4, 500)

    it("is not initially empty") {
      pool.isEmpty must be(false)
    }

    it("executes queries") {
      pool.map { f => 1 } must equal(Some(1))
    }

    it("returns None if empty") {
      val conductor = new Conductor

      for (i <- 1 to 4) {
        conductor.thread("thread-%d".format(i)) {
          pool.map { f =>
            Thread.sleep(400)
            "woo"
          }
        }
      }

      conductor.thread {
        Thread.sleep(100)
        pool.isEmpty must be(true)
        pool.map { f => "woo" } must equal(None)
      }

      conductor.whenFinished {
        pool.isEmpty must be(false)
      }
    }

    it("removes execess idle connections after a period of time") {
      val conductor = new Conductor

      for (i <- 1 to 4) {
        conductor.thread("thread-%d".format(i)) {
          pool.map { f =>
            Thread.sleep(50)
            "woo"
          }
        }
      }

      conductor.whenFinished {
        pool.size must be(4)
        Thread.sleep(2000)
        pool.size must be(2)
      }
    }
  }
}
