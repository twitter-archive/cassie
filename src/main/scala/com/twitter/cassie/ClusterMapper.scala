package com.twitter.cassie

import scala.util.parsing.json.JSON
import java.io.IOException
import java.net.{SocketAddress, InetSocketAddress}
import com.codahale.logula.Logging
import scala.collection.JavaConversions._
import com.twitter.cassie.connection.ClusterClientProvider
import com.twitter.finagle.builder.SocketAddressCluster
import com.twitter.finagle.builder.{Cluster => FCluster}

/**
 * Given a seed host and port, returns a set of nodes in the cluster.
 *
 * TODO: Accept a set of seedHosts
 *
 * @param keyspace the keyspace to map
 * @param seedHost the hostname of the seed node
 * @param seedPort the Thrift port of the seed node
 * @author coda
 */
private class ClusterMapper(keyspace: String, seedHost: String, seedPort: Int = 9160, timeoutMS: Int = 10000) extends Logging with FCluster {
  private val ccp = new ClusterClientProvider(
    new SocketAddressCluster(Seq(new InetSocketAddress(seedHost, seedPort))),
    keyspace,
    readTimeoutInMS = timeoutMS,
    maxConnectionsPerHost = 1
  )

  def mapHosts[A](f: SocketAddress => A): Seq[A] = {
    log.info("Mapping cluster...")
    val ring = ccp.map{ _.describe_ring(keyspace) }()
    ccp.close
    log.debug("Received: %s", ring)
    val hosts = asScalaIterable(ring).flatMap{ h => asScalaIterable(h.endpoints).map{ host =>
      new InetSocketAddress(host, seedPort) } }
    hosts.toSeq map f
  }

  def join(address: SocketAddress) = {}
}
