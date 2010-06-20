package com.codahale.cassie.connection

import java.net.InetSocketAddress
import org.apache.cassandra.thrift.Cassandra.Client

/**
 * Manages connections to the nodes in a Cassandra cluster.
 *
 * @param retryAttempts the number of times a query should be attempted before
 *                      throwing an exception
 * @param partialFailureThreshold the number
 * @param downTimeoutInMS how long, in milliseconds, a node should be marked as
 *                        down before allowing a recovery query to be processed
 * @param minConnectionsPerHost the minimum number of connections to maintain to
 *                              the node
 * @param maxConnectionsPerHost the maximum number of connections to maintain to
 *                              the ndoe
 * @param removeAfterIdleForMS the amount of time, in milliseconds, after which
 *                             idle connections should be closed and removed
 *                             from the pool
 * @author coda
 */
class Cluster(val hosts: Set[InetSocketAddress],
              val retryAttempts: Int,
              val partialFailureThreshold: Int,
              val downTimeoutInMS: Int,
              val minConnectionsPerHost: Int,
              val maxConnectionsPerHost: Int,
              val removeAfterIdleForMS: Int) {
  
  private val pools = hosts.map { h =>
    val clientFactory = new ClientFactory(h)
    val factory = new ConnectionFactory(clientFactory)
    val pool = new ConnectionPool(factory, minConnectionsPerHost,
                                  maxConnectionsPerHost, removeAfterIdleForMS)
    new FailureAwareConnectionPool(pool, partialFailureThreshold, downTimeoutInMS)
  }
  private val balancer = new RoundRobinLoadBalancer(pools, retryAttempts)

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
  def map[A](f: Client => A): A = balancer.map(f)
}
