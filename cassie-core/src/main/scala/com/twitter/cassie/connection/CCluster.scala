package com.twitter.cassie.connection

import java.net.SocketAddress
import com.twitter.finagle.builder.{ StaticCluster => FStaticCluster, Cluster => FCluster }

trait CCluster[T] extends FCluster[T] {
  def close
}

/**
 * A cassandra cluster specified by socket addresses. No remapping.
 */
class SocketAddressCluster(private[this] val underlying: Seq[SocketAddress])
  extends FStaticCluster[SocketAddress](underlying) with CCluster[SocketAddress] {
  def close() = ()
}
