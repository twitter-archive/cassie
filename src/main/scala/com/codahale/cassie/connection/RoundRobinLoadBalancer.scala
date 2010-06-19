package com.codahale.cassie.connection

import org.apache.cassandra.thrift.Cassandra.Client
import com.codahale.logula.Logging
import java.util.concurrent.atomic.AtomicLong

/**
 * Balances requests between nodes in a round-robin fashion.
 *
 * @param nodes a set of [[com.codahale.cassie.connection.FailureAwareConnectionPool]]s
 *              for each node the client should send queries to
 * @param retryAttempts the number of times a query should be attempted before
 *                      throwing an exception
 * @author coda
 */
class RoundRobinLoadBalancer(nodes: Set[FailureAwareConnectionPool],
                             val retryAttempts: Int) extends Logging {
  private val pools = nodes.toArray
  private val index = new AtomicLong(0) // to prevent overflow

  /**
   * Borrows a connection from the pool and passes it to a callback function.
   *
   * If the callback does not succeed (i.e., there was a network or server
   * error), it will be re-called with a client for a different node, until the
   * number of attempts exceeds `retryAttempts`.
   *
   * @param f a function which given a Cassandra `Client`, returns a value
   * @tparam A the query result type
   * @return if `f` was called successfully, `f(client)`, otherwise throws an
   *         exception
   * @throws UnsuccessfulQueryException if none of the nodes are available
   */
  @throws(classOf[UnsuccessfulQueryException])
  def map[A](f: Client => A): A = {
    for (i <- 1 to retryAttempts) {
      val pool = current
      if (!pool.isEmpty) {
        val result = pool.map(f)
        if (result.isDefined) {
          return result.get
        }
      }
    }
    val hosts = pools.map { _.host }
    val msg = ("Made %d attempts to query nodes " +
               "(%s) with no success").format(retryAttempts, hosts.mkString(", "))
    log.severe(msg)
    throw new UnsuccessfulQueryException(msg)
  }

  private def current = pools((index.getAndIncrement % pools.length).toInt)
}
