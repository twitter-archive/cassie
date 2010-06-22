package com.codahale.cassie

import connection._
import java.net.InetSocketAddress

/**
 * A Cassandra cluster.
 *
 * @param provider a [[com.codahale.cassie.connection.ClientProvider]] instance
 * @author coda
 */
class Cluster(provider: ClientProvider) {

  /**
   * Returns a [[com.codahale.cassie.Cluster]] instance with a
   * [[com.codahale.cassie.connection.ClusterClientProvider]] with the provided
   * options.
   *
   * @see com.codahale.cassie.connection.ClusterClientProvider
   */
  def this(hosts: Set[InetSocketAddress],
           retryAttempts: Int = 5,
           readTimeoutInMS: Int = 10000,
           partialFailureThreshold: Int = 3,
           downTimeoutInMS: Int = 10000,
           minConnectionsPerHost: Int = 1,
           maxConnectionsPerHost: Int = 5,
           removeAfterIdleForMS: Int = 60000) = {
    this(new ClusterClientProvider(hosts, retryAttempts, readTimeoutInMS, partialFailureThreshold, downTimeoutInMS, minConnectionsPerHost, maxConnectionsPerHost, removeAfterIdleForMS))
  }

  /**
   * Returns a [[com.codahale.cassie.Keyspace]] with the given name.
   *
   * @param name the keyspace's name
   */
  def keyspace(name: String) = new Keyspace(name, provider)
}
