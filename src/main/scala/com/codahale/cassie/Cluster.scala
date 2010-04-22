package com.codahale.cassie

import client.ClientProvider

/**
 * A Cassandra cluster.
 *
 * @author coda
 */
class Cluster(val provider: ClientProvider) {
  /**
   * Returns a Keyspace with the given name.
   */
  def keyspace(name: String) = new Keyspace(name, provider)
}
