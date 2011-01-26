package com.codahale.cassie

import connection._
import java.net.InetSocketAddress

/**
 * A Cassandra cluster.
 *
 * @param hosts A set of seed hosts for a cluster: the set of hosts for a particular
 *        keyspace can optionally be determined via mapping.
 * @author coda
 */
class Cluster(seedHosts: Set[String], seedPort: Int) {

  def this(seedHosts: String*) = this(seedHosts.toSet, 9160)

  /**
   * Returns a [[com.codahale.cassie.Keyspace]] instance with a
   * [[com.codahale.cassie.connection.ClusterClientProvider]] with the provided
   * options.
   * @param name the keyspace's name
   * @param performMapping true to expand the cluster's list of seeds into a full
   *        list of hosts; false if the seed list should be used directly
   * @see com.codahale.cassie.connection.ClusterClientProvider
   */
  def keyspace(name: String,
               performMapping: Boolean = true,
               retryAttempts: Int = 5,
               readTimeoutInMS: Int = 10000,
               partialFailureThreshold: Int = 3,
               downTimeoutInMS: Int = 10000,
               minConnectionsPerHost: Int = 1,
               maxConnectionsPerHost: Int = 5,
               removeAfterIdleForMS: Int = 60000,
               framed: Boolean = true) = {
    val hosts = if (performMapping)
      // either map the cluster for this keyspace
      new ClusterMapper(name, seedHosts.head).hosts
    else
      // or connect directly to the hosts that were given as seeds
      seedHosts.map{ host => new InetSocketAddress(host, seedPort) }

    val ccp = new ClusterClientProvider(hosts, name, retryAttempts, readTimeoutInMS, minConnectionsPerHost, maxConnectionsPerHost, removeAfterIdleForMS)
    new Keyspace(name, ccp)
  }
}
