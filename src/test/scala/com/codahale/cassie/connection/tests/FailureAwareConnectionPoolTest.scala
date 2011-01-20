package com.codahale.cassie.connection.tests

import org.scalatest.matchers.MustMatchers
import org.scalatest.mock.MockitoSugar
import org.scalatest.{OneInstancePerTest, Spec}
import org.mockito.Mockito.when
import org.mockito.Matchers.any
import org.apache.cassandra.thrift.Cassandra.{Client}
import com.codahale.cassie.connection.{ConnectionPool, FailureAwareConnectionPool}
import org.mockito.stubbing.Answer
import org.mockito.invocation.InvocationOnMock
import com.codahale.logula.Logging
import org.apache.log4j.Level
import org.scalatest.concurrent.Conductor

class FailureAwareConnectionPoolTest extends Spec
        with MustMatchers with MockitoSugar with OneInstancePerTest {

  def anyCallback = any(classOf[Function1[Client, String]])
  def makeConnectionPoolWork(p: ConnectionPool, client: Client) {
    when(p.map(anyCallback)).thenAnswer(new Answer[Option[String]] {
      def answer(invocation: InvocationOnMock) = {
        // Pretty sure this is a Mockito+Scala bug. If I double-mock this method,
        // even with the same damn matcher, it starts calling this Answer with
        // null as the one and only argument. So that's rad.
        val blah = invocation.getArguments().head
        if (blah == null) {
          None
        } else {
          Some(blah.asInstanceOf[Client => String](client))
        }
      }
    })
  }
  def makeConnectionPoolBroken(p: ConnectionPool) {
    when(p.map(anyCallback)).thenReturn(None)
  }
  def newConnectionPool(client: Client) = {
    val p = mock[ConnectionPool]
    when(p.size).thenReturn(12)
    makeConnectionPoolWork(p, client)
    p
  }

  describe("a failure-aware connection pool with no partial failures") {
    Logging.configure(_.level = Level.OFF)

    def setup(client: Client) = {
      val p = newConnectionPool(client)
      (p, new FailureAwareConnectionPool(p, 2, 500))
    }

    val client = mock[Client]
    when(client.describe_version).thenReturn("moof")

    it("is up") {
      val (connectionPool, pool) = setup(client)
      pool.isUp must be(true)
    }

    it("is not down") {
      val (connectionPool, pool) = setup(client)
      pool.isDown must be(false)
    }

    it("is not recovering") {
      val (connectionPool, pool) = setup(client)
      pool.isRecovering must be(false)
    }

    it("is not empty") {
      val (connectionPool, pool) = setup(client)
      pool.isEmpty must be(false)
    }

    it("has connections") {
      val (connectionPool, pool) = setup(client)
      pool.size must be(12)
    }

    it("performs queries") {
      val (connectionPool, pool) = setup(client)
      pool.map { i => i.describe_version } must equal(Some("moof"))
    }

    it("marks itself as down after reaching a partial failure threshold") {
      val (connectionPool, pool) = setup(client)
      makeConnectionPoolBroken(connectionPool)
      pool.map { i => i.describe_version }
      pool.map { i => i.describe_version }
      pool.isDown must be(true)
    }
  }

  describe("a failure-aware connection pool which has reached its partial failure") {
    val client = mock[Client]
    when(client.describe_version).thenReturn("moof")

    def setup(client: Client) = {
      val p = newConnectionPool(client)
      makeConnectionPoolBroken(p)
      val p2 = new FailureAwareConnectionPool(p, 2, 100)
      p2.map { i => i.describe_version }
      p2.map { i => i.describe_version }
      (p, p2)
    }

    it("is not up") {
      val (connectionPool, pool) = setup(client)
      pool.isUp must be(false)
    }

    it("is down") {
      val (connectionPool, pool) = setup(client)
      pool.isDown must be(true)
    }

    it("is not recovering") {
      val (connectionPool, pool) = setup(client)
      pool.isRecovering must be(false)
    }

    it("is empty") {
      val (connectionPool, pool) = setup(client)
      pool.isEmpty must be(true)
    }

    it("has no connections") {
      val (connectionPool, pool) = setup(client)
      pool.size must be(0)
    }

    it("does not perform queries") {
      val (connectionPool, pool) = setup(client)
      pool.map { i => i.describe_version } must equal(None)
    }

    it("marks itself as recovering after the timeout period") {
      val (connectionPool, pool) = setup(client)
      Thread.sleep(200)

      pool.isRecovering must be(true)
    }
  }

  describe("a failure-aware connection pool after its timeout period has expired") {
    val client = mock[Client]
    when(client.describe_version).thenReturn("moof")

    def setup(client: Client) = {
      val p = newConnectionPool(client)
      makeConnectionPoolBroken(p)
      val p2 = new FailureAwareConnectionPool(p, 2, 100)
      p2.map { i => i.describe_version }
      p2.map { i => i.describe_version }
      Thread.sleep(200)
      (p, p2)
    }

    it("is recovering") {
      val (connectionPool, pool) = setup(client)

      pool.isRecovering must be(true)
    }

    it("marks itself as up when a query succeeds") {
      val (connectionPool, pool) = setup(client)
      makeConnectionPoolWork(connectionPool, client)

      pool.map { i => i.describe_version } must equal(Some("moof"))

      pool.isUp must be(true)
    }

    it("marks itself as down when a query fails") {
      val (connectionPool, pool) = setup(client)
      makeConnectionPoolBroken(connectionPool)

      pool.map { i => i.describe_version } must equal(None)

      pool.isDown must be(true)
    }

    it("only tries one query while recovering") {
      val conductor = new Conductor
      val (connectionPool, pool) = setup(client)
      makeConnectionPoolWork(connectionPool, client)

      conductor.thread {
        pool.map { i =>
          Thread.sleep(100)
          i.describe_version
        } must equal(Some("moof"))
      }

      conductor.thread {
        Thread.sleep(50)
        pool.map { i =>
          i.describe_version
        } must equal(None)
      }

      conductor.whenFinished {
        pool.isDown must be(false)
      }
    }
  }
}
