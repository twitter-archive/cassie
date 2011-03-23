package com.twitter.cassie

import collection.SeqProxy
import com.google.common.collect.ImmutableSet
import com.twitter.cassie.connection.ClusterClientProvider
import com.twitter.finagle.builder.SocketAddressCluster
import com.twitter.finagle.builder.{Cluster => FCluster}
import com.twitter.finagle.ServiceFactory
import com.twitter.finagle.util.Timer
import com.twitter.logging.Logger
import com.twitter.util.Duration
import com.twitter.util.Time
import java.io.IOException
import java.net.{InetSocketAddress, SocketAddress}
import java.net.{SocketAddress, InetSocketAddress}
import org.jboss.netty.util.HashedWheelTimer
import scala.collection.JavaConversions._
import scala.util.parsing.json.JSON


/**
 * Given a seed host and port, returns a set of nodes in the cluster.
 *
 * TODO: Accept a set of seedHosts
 *
 * @param keyspace the keyspace to map
 * @param seedHost the hostname of the seed node
 * @param seedPort the Thrift port of the seed node
 */
private class ClusterRemapper(keyspace: String, seedHost: String, remapPeriod: Duration, seedPort: Int = 9160, timeoutMS: Int = 10000) extends FCluster {
  private val log = Logger.get

  // For servers, not clients.
  def join(address: SocketAddress) {}

  // Called once to get a Seq-like of ServiceFactories.
  def mkFactories[Req, Rep](mkBroker: (SocketAddress) => ServiceFactory[Req, Rep]) = {
    new SeqProxy[ServiceFactory[Req, Rep]] {

      @volatile private[this] var underlyingMap: Map[SocketAddress, ServiceFactory[Req, Rep]] = Map(Seq(seedHost) map { address =>
        new InetSocketAddress(address, seedPort) -> mkBroker(new InetSocketAddress(address, seedPort))
      }: _*)
      def self = underlyingMap.values.toSeq

      private[this] var timer = new Timer(new HashedWheelTimer())

      timer.schedule(Time.now, remapPeriod) {
        performChange(fetchHosts(underlyingMap.keys.toSeq))
      }

      private[this] def performChange(ring: Seq[SocketAddress]) {
        val oldMap = underlyingMap
        val (removed, same, added) = diff(oldMap.keys.toSet, ring.toSet)

        val addedBrokers = Map(added.toSeq map { address =>
          address -> mkBroker(address)
        }: _*)
        val sameBrokers = oldMap.filter { case (key, value) => same contains key }
        val newMap = addedBrokers ++ sameBrokers
        underlyingMap = newMap
        removed.foreach { address =>
          oldMap(address).close()
        }
      }

      private[this] def diff[A](oldSet: Set[A], newSet: Set[A]) = {
        val removed = oldSet &~ newSet
        val same = oldSet & newSet
        val added = newSet &~ oldSet

        (removed, same, added)
      }
    }
  }

  private[this] def fetchHosts(hosts: Seq[SocketAddress]): Seq[SocketAddress] = {
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
  

}
