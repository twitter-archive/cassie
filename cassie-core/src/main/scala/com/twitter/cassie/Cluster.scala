package com.twitter.cassie

import connection._
import java.net.InetSocketAddress
import scala.collection.JavaConversions._
import com.twitter.cassie.connection.{CCluster, RetryPolicy, SocketAddressCluster}
import com.twitter.util.Duration
import com.twitter.conversions.time._
import com.twitter.finagle.builder.{Cluster => FCluster}
import com.twitter.finagle.stats.{StatsReceiver, NullStatsReceiver}
import com.twitter.finagle.tracing.{Tracer, NullTracer}

/**
 * A Cassandra cluster.
 *
 * @param seedHosts list of some hosts in the cluster
 * @param seedPort the port number for '''all''' hosts in the cluster
 *        to refresh its host list.
 * @param stats a finagle stats receiver */
class Cluster(seedHosts: Set[String], seedPort: Int, stats: StatsReceiver) {
  private var mapHostsEvery: Duration = 10.minutes

  /**
    * @param seedHosts A comma separated list of seed hosts for a cluster. The rest of the
    *                  hosts can be found via mapping the cluser. See KeyspaceBuilder.mapHostsEvery.
    *                  The port number is assumed to be 9160. */
  def this(seedHosts: String, stats: StatsReceiver = NullStatsReceiver) =
    this(seedHosts.split(',').filter{ !_.isEmpty }.toSet, 9160, stats)

  /**
    * @param seedHosts A collection of seed host addresses. The port number is assumed to be 9160 */
  def this(seedHosts: java.util.Collection[String]) =
    this(asScalaIterable(seedHosts).toSet, 9160, NullStatsReceiver)

  /**
    * Returns a  [[com.twitter.cassie.KeyspaceBuilder]] instance.
    * @param name the keyspace's name */
  def keyspace(name: String): KeyspaceBuilder = {
    val scopedStats = stats.scope("cassie").scope(name)
    val seedAddresses = seedHosts.map{ host => new InetSocketAddress(host, seedPort) }.toSeq
    val cluster = if (mapHostsEvery > 0)
      // either map the cluster for this keyspace
      new ClusterRemapper(name, seedAddresses, mapHostsEvery, statsReceiver = stats.scope("remapper"))
    else
      // or connect directly to the hosts that were given as seeds
      new SocketAddressCluster(seedAddresses)

    KeyspaceBuilder(cluster, name, scopedStats)
  }

  /**
    * @param d Cassie will query the cassandra cluster every [[period]] period
    *          to refresh its host list. */
  def mapHostsEvery(period: Duration): Cluster = {
    mapHostsEvery = period
    this
  }
}


case class KeyspaceBuilder(
  cluster: CCluster,
  name: String,
  stats: StatsReceiver,
  _retries: Int = 0,
  _timeout: Int = 5000,
  _requestTimeout: Int = 1000,
  _connectTimeout: Int = 1000,
  _minConnectionsPerHost: Int = 1,
  _maxConnectionsPerHost: Int = 5,
  _hostConnectionMaxWaiters: Int = 100,
  _tracerFactory: Tracer.Factory = NullTracer.factory,
  _retryPolicy: RetryPolicy = RetryPolicy.Idempotent) {

  /**
    * connect to the cluster with the specified parameters */
  def connect(): Keyspace = {
    // TODO: move to builder pattern as well
    val ccp = new ClusterClientProvider(
      cluster,
      name,
      _retries,
      _timeout.milliseconds,
      _requestTimeout.milliseconds,
      _connectTimeout.milliseconds,
      _minConnectionsPerHost,
      _maxConnectionsPerHost,
      _hostConnectionMaxWaiters,
      stats,
      _tracerFactory,
      _retryPolicy)
    new Keyspace(name, ccp, stats)
  }

  def timeout(t: Int): KeyspaceBuilder = copy(_timeout = t)
  def retries(r: Int): KeyspaceBuilder = copy(_retries = r)
  def retryPolicy(r: RetryPolicy): KeyspaceBuilder = copy(_retryPolicy = r)

  /**
    * @see requestTimeout in [[http://twitter.github.com/finagle/finagle-core/target/doc/main/api/com/twitter/finagle/builder/ClientBuilder.html]] */
  def requestTimeout(r: Int): KeyspaceBuilder = copy(_requestTimeout = r)

  /**
    * @see connectionTimeout in [[http://twitter.github.com/finagle/finagle-core/target/doc/main/api/com/twitter/finagle/builder/ClientBuilder.html]]*/
  def connectTimeout(r: Int): KeyspaceBuilder = copy(_connectTimeout = r)

  def minConnectionsPerHost(m: Int): KeyspaceBuilder =
    copy(_minConnectionsPerHost = m)
  def maxConnectionsPerHost(m: Int): KeyspaceBuilder =
    copy(_maxConnectionsPerHost = m)

  /** A finagle stats receiver for reporting. */
  def reportStatsTo(r: StatsReceiver) = copy(stats = r)

  /** Set a tracer to collect request traces. */
  def tracerFactory(t: Tracer.Factory) = copy(_tracerFactory = t)

  def hostConnectionMaxWaiters(i: Int) = copy(_hostConnectionMaxWaiters = i)
}

