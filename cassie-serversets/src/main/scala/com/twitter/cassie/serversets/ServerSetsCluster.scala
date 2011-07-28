package com.twitter.cassie

import java.net.InetSocketAddress

import scala.collection.JavaConversions

import com.twitter.cassie.connection.CCluster
import com.twitter.common.quantity.Amount
import com.twitter.common.quantity.Time
import com.twitter.common.zookeeper.ServerSet
import com.twitter.common.zookeeper.ServerSetImpl
import com.twitter.common.zookeeper.ZooKeeperClient
import com.twitter.finagle.zookeeper.ZookeeperServerSetCluster

class ZookeeperServerSetCCluster(serverSet: ServerSet) extends ZookeeperServerSetCluster(serverSet) with CCluster {
  def close {}
}

class ServerSetsCluster(zkHost: String, zkPort: Int, zkPath: String, timeoutMillis: Int) {
  /**
    * Returns a  [[com.twitter.cassie.KeyspaceBuilder]] instance.
    * @param name the keyspace's name */
  def keyspace(name: String): KeyspaceBuilder = {
    val zkAddress = new InetSocketAddress(zkHost, zkPort)
    val zkClient = new ZooKeeperClient(Amount.of(timeoutMillis, Time.MILLISECONDS), JavaConversions.asJavaIterable(List(zkAddress)))
    val serverSet = new ServerSetImpl(zkClient, zkPath)
    val cluster = new ZookeeperServerSetCCluster(serverSet)
    KeyspaceBuilder(name, cluster)
  }
}
