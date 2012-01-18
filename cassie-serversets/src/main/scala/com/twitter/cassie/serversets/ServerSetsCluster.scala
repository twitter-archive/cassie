package com.twitter.cassie

import java.net.InetSocketAddress

import scala.collection.JavaConversions

import com.twitter.cassie.connection.CCluster
import com.twitter.common.quantity.Amount
import com.twitter.common.quantity.Time
import com.twitter.common.zookeeper.ServerSet
import com.twitter.common.zookeeper.ServerSetImpl
import com.twitter.common.zookeeper.ZooKeeperClient
import com.twitter.finagle.stats.{ StatsReceiver, NullStatsReceiver }
import com.twitter.finagle.zookeeper.ZookeeperServerSetCluster

class ZookeeperServerSetCCluster(serverSet: ServerSet) extends ZookeeperServerSetCluster(serverSet) with CCluster {
  def close {}
}

/**
 * A Cassandra cluster where nodes are discovered using ServerSets.
 *
 *  import com.twitter.conversions.time._
 *  val clusterName = "cluster"
 *  val keyspace = "KeyspaceName"
 *  val zkPath = "/twitter/service/cassandra/%s".format(clusterName)
 *  val zkHosts = Seq(new InetSocketAddress("zookeeper.local.twitter.com", 2181))
 *  val timeoutMillis = 1.minute.inMilliseconds.toInt
 *  val stats = NullStatsReceiver // or OstrichStatsReciever or whatever
 *
 *  val cluster = new ServerSetsCluster(zkHosts, zkPath, timeoutMillis, stats)
 *  val keyspace = cluster.keyspace(keyspace).connect()
 *
 *
 * @param zkClient existing ZooKeeperClient
 * @param zkPath path to node where Cassandra hosts will exist under
 * @param stats a finagle stats receiver
 */
class ServerSetsCluster(zkClient: ZooKeeperClient, zkPath: String, stats: StatsReceiver) extends ClusterBase {

  /**
   * Convenience constructor that creates a ZooKeeperClient using the specified hosts and timeout.
   *
   * @param zkAddresses list of some ZooKeeper hosts
   * @param zkPath path to node where Cassandra hosts will exist under
   * @param timeoutMillis timeout for ZooKeeper connection
   * @param stats a finagle stats receiver
   */
  def this(zkAddresses: Iterable[InetSocketAddress], zkPath: String, timeoutMillis: Int,
      stats: StatsReceiver = NullStatsReceiver) =
    this(new ZooKeeperClient(Amount.of(timeoutMillis, Time.MILLISECONDS), JavaConversions.asJavaIterable(zkAddresses)), zkPath, stats)

  /**
   * Returns a  [[com.twitter.cassie.KeyspaceBuilder]] instance.
   * @param name the keyspace's name
   */
  def keyspace(name: String): KeyspaceBuilder = {
    val serverSet = new ServerSetImpl(zkClient, zkPath)
    val cluster = new ZookeeperServerSetCCluster(serverSet)
    KeyspaceBuilder(cluster, name, stats.scope("cassie").scope(name))
  }
}
