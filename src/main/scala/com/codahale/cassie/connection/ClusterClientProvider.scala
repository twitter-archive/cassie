package com.codahale.cassie.connection

import java.net.InetSocketAddress
import org.apache.cassandra.thrift.Cassandra.Client

/**
 * Manages connections to the nodes in a Cassandra cluster.
 *
 * @param retryAttempts the number of times a query should be attempted before
 *                      throwing an exception
 * @param readTimeoutInMS the amount of time, in milliseconds, the client will
 *                        wait for a response from the server before considering
 *                        the query to have failed
 * @param partialFailureThreshold the number of failed queries in a row a node
 *                                must return before being marked down
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
class ClusterClientProvider(val hosts: Set[InetSocketAddress],
                            val keyspace: String,
                            val retryAttempts: Int = 5,
                            val readTimeoutInMS: Int = 10000,
                            val partialFailureThreshold: Int = 3,
                            val downTimeoutInMS: Int = 10000,
                            val minConnectionsPerHost: Int = 1,
                            val maxConnectionsPerHost: Int = 5,
                            val removeAfterIdleForMS: Int = 60000) extends ClientProvider {
  private val pools = hosts.map { h =>
    val clientFactory = new ClientFactory(h, keyspace, readTimeoutInMS)
    val factory = new ConnectionFactory(clientFactory)
    val pool = new ConnectionPool(factory, minConnectionsPerHost,
                                  maxConnectionsPerHost, removeAfterIdleForMS)
    new FailureAwareConnectionPool(pool, partialFailureThreshold, downTimeoutInMS)
  }
  private val balancer = new RoundRobinLoadBalancer(pools, retryAttempts)

  def map[A](f: Client => A) = balancer.map(f)
}
