package com.twitter.cassie.connection

import com.twitter.finagle.builder.{Cluster => FCluster}
import java.net.SocketAddress
import com.twitter.finagle.ServiceFactory

trait CCluster extends FCluster {
  def close
}


class SocketAddressCluster(underlying: Seq[SocketAddress])
  extends CCluster
{
  private[this] var self = underlying

  def mkFactories[Req, Rep](f: SocketAddress => ServiceFactory[Req, Rep]) = self map f

  def join(address: SocketAddress) {
    self = underlying ++ Seq(address)
  }

  def close() = ()
}
