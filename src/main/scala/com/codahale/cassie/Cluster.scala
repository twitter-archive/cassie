package com.codahale.cassie

import connection._
import java.net.InetSocketAddress

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
              val retryAttempts: Int = 5,
              val partialFailureThreshold: Int = 3,
              val downTimeoutInMS: Int = 10000,
              val minConnectionsPerHost: Int = 1,
              val maxConnectionsPerHost: Int = 5,
              val removeAfterIdleForMS: Int = 60000) {
  
  private val pools = hosts.map { h =>
    val clientFactory = new ClientFactory(h)
    val factory = new ConnectionFactory(clientFactory)
    val pool = new ConnectionPool(factory, minConnectionsPerHost,
                                  maxConnectionsPerHost, removeAfterIdleForMS)
    new FailureAwareConnectionPool(pool, partialFailureThreshold, downTimeoutInMS)
  }
  private val balancer = new RoundRobinLoadBalancer(pools, retryAttempts)

  /**
   * Returns a [[com.codahale.cassie.Keyspace]] with the given name.
   *
   * @param name the keyspace's name
   */
  def keyspace(name: String) = new Keyspace(name, balancer)
}
