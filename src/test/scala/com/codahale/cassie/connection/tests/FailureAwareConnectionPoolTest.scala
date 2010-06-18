package com.codahale.cassie.connection.tests

import org.scalatest.matchers.MustMatchers
import org.scalatest.mock.MockitoSugar
import org.scalatest.{OneInstancePerTest, Spec}
import org.mockito.Mockito.when
import com.codahale.cassie.connection.{FailureAwareConnectionPool, ConnectionPool}
import org.apache.cassandra.thrift.Cassandra.{Client}

class TestableConnectionPool(val client: Client) extends ConnectionPool {
  var broken = false
  def size = 12
  def map[A](f: (Client) => A) = if (!broken) Some(f(client)) else None
  def isEmpty = false
  def clear() = null
}

class FailureAwareConnectionPoolTest extends Spec
        with MustMatchers with MockitoSugar with OneInstancePerTest {

  describe("a failure-aware connection pool with no partial failures") {
    def setup(p: ConnectionPool) = new FailureAwareConnectionPool(p, 2, 500)

    val client = mock[Client]
    when(client.describe_version).thenReturn("moof")

    val connectionPool = new TestableConnectionPool(client)

    it("is up") {
      val pool = setup(connectionPool)
      pool.isUp must be(true)
    }

    it("is not down") {
      val pool = setup(connectionPool)
      pool.isDown must be(false)
    }

    it("is not recovering") {
      val pool = setup(connectionPool)
      pool.isRecovering must be(false)
    }

    it("is not empty") {
      val pool = setup(connectionPool)
      pool.isEmpty must be(false)
    }

    it("has connections") {
      val pool = setup(connectionPool)
      pool.size must be(12)
    }

    it("performs queries") {
      val pool = setup(connectionPool)
      connectionPool.broken = false
      pool.map { i => i.describe_version } must equal(Some("moof"))
    }

    it("marks itself as down after reaching a partial failure threshold") {
      val pool = setup(connectionPool)
      connectionPool.broken = true
      pool.map { i => i.describe_version }
      pool.map { i => i.describe_version }
      pool.isDown must be(true)
    }
  }

  describe("a failure-aware connection pool which has reached its partial failure") {
    val client = mock[Client]
    when(client.describe_version).thenReturn("moof")
    
    val connectionPool = new TestableConnectionPool(client)
    connectionPool.broken = true

    def setup(p: ConnectionPool) = {
      val p2 = new FailureAwareConnectionPool(p, 2, 100)
      p2.map { i => i.describe_version }
      p2.map { i => i.describe_version }
      p2
    }

    it("is not up") {
      val pool = setup(connectionPool)
      pool.isUp must be(false)
    }

    it("is down") {
      val pool = setup(connectionPool)
      pool.isDown must be(true)
    }

    it("is not recovering") {
      val pool = setup(connectionPool)
      pool.isRecovering must be(false)
    }

    it("is empty") {
      val pool = setup(connectionPool)
      pool.isEmpty must be(true)
    }

    it("has no connections") {
      val pool = setup(connectionPool)
      pool.size must be(0)
    }

    it("does not perform queries") {
      val pool = setup(connectionPool)
      pool.map { i => i.describe_version } must equal(None)
    }

    it("marks itself as recovering after the timeout period") {
      val pool = setup(connectionPool)
      Thread.sleep(200)

      pool.isRecovering must be(true)
    }
  }

  describe("a failure-aware connection pool after its timeout period has expired") {
    val client = mock[Client]
    when(client.describe_version).thenReturn("moof")

    val connectionPool = new TestableConnectionPool(client)
    connectionPool.broken = true

    def setup(p: ConnectionPool) = {
      val p2 = new FailureAwareConnectionPool(p, 2, 100)
      p2.map { i => i.describe_version }
      p2.map { i => i.describe_version }
      Thread.sleep(200)
      p2
    }

    it("is recovering") {
      val pool = setup(connectionPool)

      pool.isRecovering must be(true)
    }

    it("marks itself as up when a query succeeds") {
      val pool = setup(connectionPool)
      connectionPool.broken = false

      pool.map { i => i.describe_version } must equal(Some("moof"))

      pool.isUp must be(true)
    }

    it("marks itself as down when a query fails") {
      val pool = setup(connectionPool)
      connectionPool.broken = true

      pool.map { i => i.describe_version } must equal(None)

      pool.isDown must be(true)
    }
  }
}
