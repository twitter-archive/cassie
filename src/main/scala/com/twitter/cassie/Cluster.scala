package com.twitter.cassie

import connection._
import java.net.InetSocketAddress
import scala.collection.JavaConversions._


/**
 * A Cassandra cluster.
 *
 * @author coda
 */
class Cluster(seedHosts: Set[String], seedPort: Int) {

  /**
   * @param hosts A comma separated list of seed hosts for a cluster: the set of
   *        hosts for a particular keyspace can optionally be determined via mapping.
   */
  def this(seedHosts: String) = this(seedHosts.split(',').filter{ !_.isEmpty }.toSet, 9160)
  def this(seedHosts: java.util.Collection[String]) = this(asScalaIterable(seedHosts).toSet, 9160)

    /**
   * Returns a [[com.twitter.cassie.Keyspace]] instance with a
   * [[com.twitter.cassie.connection.ClusterClientProvider]] with the provided
   * options.
   * @param name the keyspace's name
   * @param performMapping true to expand the cluster's list of seeds into a full
   *        list of hosts; false if the seed list should be used directly
   * @see com.twitter.cassie.connection.ClusterClientProvider
   */
  def keyspace(name: String): KeyspaceBuilder = KeyspaceBuilder(name)

  case class KeyspaceBuilder(
    _name: String,
    _performMapping: Boolean = true,
    _retryAttempts: Int = 0,
    _readTimeoutInMS: Int = 10000,
    _partialFailureThreshold: Int = 3,
    _minConnectionsPerHost: Int = 1,
    _maxConnectionsPerHost: Int = 5,
    _removeAfterIdleForMS: Int = 60000) {

    def connect(): Keyspace = {
      val hosts = if (_performMapping)
        // either map the cluster for this keyspace: TODO: use all seed hosts
        new ClusterMapper(_name, seedHosts.head).perform
      else
        // or connect directly to the hosts that were given as seeds
        seedHosts.map{ host => new InetSocketAddress(host, seedPort) }

      // TODO: move to builder pattern as well
      val ccp = new ClusterClientProvider(hosts, _name, _retryAttempts, _readTimeoutInMS, _minConnectionsPerHost, _maxConnectionsPerHost, _removeAfterIdleForMS)
      new Keyspace(_name, ccp)
    }

    def performMapping(p: Boolean): KeyspaceBuilder = copy(_performMapping = p)
    def retryAttempts(r: Int): KeyspaceBuilder = copy(_retryAttempts = r)
    def readTimeoutInMS(r: Int): KeyspaceBuilder = copy(_readTimeoutInMS = r)
    def partialFailureThreshold(p: Int): KeyspaceBuilder =
      copy(_partialFailureThreshold = p)
    def minConnectionsPerHost(m: Int): KeyspaceBuilder =
      copy(_minConnectionsPerHost = m)
    def maxConnectionsPerHost(m: Int): KeyspaceBuilder =
      copy(_maxConnectionsPerHost = m)
    def removeAfterIdleForMS(r: Int): KeyspaceBuilder =
      copy(_removeAfterIdleForMS = r)
  }
}
