package com.twitter.cassie

import java.net.InetSocketAddress

import scala.collection.JavaConversions

import com.twitter.cassie.connection.CCluster
import com.twitter.common.quantity.Amount
import com.twitter.common.quantity.Time
import com.twitter.common.zookeeper.ServerSet
import com.twitter.common.zookeeper.ServerSetImpl
import com.twitter.common.zookeeper.ZooKeeperClient
import com.twitter.finagle.stats.{StatsReceiver, NullStatsReceiver}
import com.twitter.finagle.zookeeper.ZookeeperServerSetCluster

class ZookeeperServerSetCCluster(serverSet: ServerSet) extends ZookeeperServerSetCluster(serverSet) with CCluster {
  def close {}
}

/**
 * A Cassandra cluster where nodes are discovered using ServerSets
 *
 * @param zkAddersses list of some ZooKeeper hosts
 * @param zkPath path to node where Cassandra hosts will exist under
 * @param timeoutMillis timeout for ZooKeeper connection
 * @param stats a finagle stats receiver */
class ServerSetsCluster(zkAddresses: Iterable[InetSocketAddress], zkPath: String, timeoutMillis: Int, stats: StatsReceiver = NullStatsReceiver) {
  /**
    * Returns a  [[com.twitter.cassie.KeyspaceBuilder]] instance.
    * @param name the keyspace's name */
  def keyspace(name: String): KeyspaceBuilder = {
    val zkClient = new ZooKeeperClient(Amount.of(timeoutMillis, Time.MILLISECONDS), JavaConversions.asJavaIterable(zkAddresses))
    val serverSet = new ServerSetImpl(zkClient, zkPath)
    val cluster = new ZookeeperServerSetCCluster(serverSet)
    KeyspaceBuilder(cluster, name, stats.scope("cassie").scope(name))
  }
}
