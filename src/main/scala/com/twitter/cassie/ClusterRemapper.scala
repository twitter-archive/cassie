package com.twitter.cassie

import scala.util.parsing.json.JSON
import java.io.IOException
import java.net.{SocketAddress, InetSocketAddress}
import com.twitter.logging.Logger
import scala.collection.JavaConversions._
import com.twitter.cassie.connection.ClusterClientProvider
import com.twitter.finagle.builder.SocketAddressCluster
import com.twitter.finagle.builder.{Cluster => FCluster}
import com.twitter.conversions.time._
import com.twitter.finagle.util.Timer
import com.twitter.util.Time

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
private class ClusterRemapper(keyspace: String, seedHost: String, seedPort: Int = 9160, timeoutMS: Int = 10000) extends FCluster {
  private val log = Logger.get

  @volatile
  private[this] var hosts = Seq(new InetSocketAddress(seedHost, seedPort))

  Timer.default.schedule(Time.now, 10.minutes) {
    hosts = fetchHosts
  }

  def fetchHosts = {
     val ccp = new ClusterClientProvider(
      new SocketAddressCluster(hosts),
      keyspace,
      readTimeoutInMS = timeoutMS,
      maxConnectionsPerHost = 1
    )
    log.info("Mapping cluster...")
    val ring = ccp.map{ _.describe_ring(keyspace) }()
    ccp.close()
    log.debug("Received: %s", ring)
    asScalaIterable(ring).flatMap{ h => asScalaIterable(h.endpoints).map{ host =>
      new InetSocketAddress(host, seedPort) } }.toSeq
  }

  def mapHosts[A](f: SocketAddress => A): Seq[A] = {
    hosts map f
  }

  def join(address: SocketAddress) = {}
}
