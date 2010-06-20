package com.codahale.cassie.connection

import org.apache.cassandra.thrift.Cassandra.Client
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}
import com.codahale.logula.Logging

/**
 * A decorator for [[com.codahale.cassie.connection.ConnectionPool]]s which
 * wraps query execution in a circuit breaker. If a certain number of successive
 * queries fail, the pool is marked down for a period of time, during which all
 * queries will return None without attempting to contact the node. After the
 * down period is over, another successful query will reset the pool to fully
 * up; another failed query will mark the pool as down again.
 *
 * @param pool the underlying [[com.codahale.cassie.connection.ConnectionPool]]
 * @param partialFailureThreshold the number
 * @param downTimeoutInMS how long, in milliseconds, a node should be marked as
 *                        down before allowing a recovery query to be processed
 * @author coda
 * @see <a href="http://pragprog.com/titles/mnee/release-it">p. 115 of __Release It!__ by Michael Nygard</a>
 */
class FailureAwareConnectionPool(pool: ConnectionPool,
                                 val partialFailureThreshold: Int,
                                 val downTimeoutInMS: Int) extends Logging {
  private val _totalFailures = new AtomicInteger(0)
  private val _partialFailures = new AtomicInteger(0)
  private val _downUntil = new AtomicLong(0)

  /**
   * Returns the `InetSocketAddress` of the host.
   */
  def host = pool.host

  /**
   * Returns `true` if the connection pool is accepting queries.
   */
  def isUp = _partialFailures.get < partialFailureThreshold

  /**
   * Returns `true` if the connection pool is *not* accepting queries.
   */
  def isDown = !isUp && (_downUntil.get > System.currentTimeMillis)

  /**
   * Returns `true` if the connection pool was down but is now accepting
   * queries. A successful query will mark the pool as up; a failed query will
   * mark the query as down for another timeout period.
   */
  def isRecovering = !(isUp || isDown)

  /**
   * Returns the number of total failures (e.g., being marked as down)
   * experienced by the pool.
   */
  def totalFailures = _totalFailures.get

  /**
   * Returns the number of partial failures (e.g., a single failed query)
   * experienced by the pool.
   */
  def partialFailures = _partialFailures.get

  /**
   * Returns `true` if the pool has no idle connections to lend.
   */
  def isEmpty = isDown || pool.isEmpty

  /**
   * Returns the total number of connections in the pool.
   */
  def size = if (isDown) 0 else pool.size

  /**
   * Borrows a connection from the pool and passes it to a callback function.
   *
   * If the query succeeds, the result is returned wrapped in a `Some` instance,
   * otherwise `None` is returned. Failed queries are counted as partial
   * failures, and may mark the pool as down. Successful queries zero out the
   * partial failure count and mark the pool as up.
   *
   * @param f a function which given a Cassandra `Client`, returns a value
   * @tparam A the query result type
   * @return if `f` was called successfully, `Some(f(client))`, otherwise `None`
   */
  def map[A](f: Client => A): Option[A] = {
    if (isDown) {
      // if the node is down, always return None
      log.warning("%s IS STILL DOWN", pool.host)
      None
    } else {
      // otherwise, perform the query inside a circuit breaker
      val result = pool.map(f)
      if (result.isEmpty) {
        log.warning("Query for %s failed", pool.host)
        _partialFailures.incrementAndGet
        if (!isUp) {
          // if the node has too many partial failures, mark the pool as down
          _downUntil.set(System.currentTimeMillis + downTimeoutInMS)
          _totalFailures.incrementAndGet
          log.warning("%s IS DOWN", pool.host)
          // remove all existing connections to the node
          pool.clear()
        }
      } else {
        if (isRecovering) {
          log.warning("%s IS UP", pool.host)
        }
        _partialFailures.set(0)
      }
      result
    }
  }

  override def toString = "FailureAwareConnectionPool(%s)".format(pool)
}
