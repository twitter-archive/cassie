package com.twitter.cassie.connection

import com.twitter.finagle.builder.{Cluster => FCluster}
import java.net.SocketAddress
import com.twitter.finagle.ServiceFactory
import com.twitter.finagle.stats.{StatsReceiver, NullStatsReceiver}

trait CCluster extends FCluster {
  var statsReceiver: StatsReceiver = NullStatsReceiver

  def close

  def statsReceiver(statsReceiver: StatsReceiver): CCluster = {
    this.statsReceiver = statsReceiver
    this
  }
}


/**
  * A cassandra cluster specified by socket addresses. No remapping. */
class SocketAddressCluster(private[this] val underlying: Seq[SocketAddress]) extends CCluster {

  def mkFactories[Req, Rep](f: SocketAddress => ServiceFactory[Req, Rep]) = underlying map f

  def join(address: SocketAddress) {}

  def close() = ()
}
