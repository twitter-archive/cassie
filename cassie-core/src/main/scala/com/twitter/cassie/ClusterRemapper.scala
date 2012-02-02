package com.twitter.cassie

import com.twitter.cassie.connection.ClusterClientProvider
import com.twitter.cassie.connection.SocketAddressCluster
import com.twitter.cassie.connection.CCluster
import com.twitter.finagle.stats.{ StatsReceiver, NullStatsReceiver }
import com.twitter.finagle.util.Timer
import com.twitter.logging.Logger
import java.net.{ SocketAddress, InetSocketAddress }
import org.jboss.netty.util.HashedWheelTimer
import scala.collection.JavaConversions._
import com.twitter.concurrent.Spool
import com.twitter.util._
import com.twitter.finagle.builder.{Cluster => FCluster}


/**
 * Given a seed host and port, returns a set of nodes in the cluster.
 *
 * @param keyspace the keyspace to map
 * @param seeds seed node addresses
 * @param port the Thrift port of client nodes
 */
object ClusterRemapper {
  private val log = Logger.get(this.getClass)
}
private class ClusterRemapper(keyspace: String, seeds: Seq[InetSocketAddress], remapPeriod: Duration, port: Int = 9160, statsReceiver: StatsReceiver = NullStatsReceiver) extends CCluster[SocketAddress] {
  import ClusterRemapper._

  private[this] var hosts = seeds
  private[this] var changes = new Promise[Spool[FCluster.Change[SocketAddress]]]

  // Timer keeps updating the host list. Variables "hosts" and "changes" together reflect the cluster consistently
  // at any time
  private[cassie] var timer = new Timer(new HashedWheelTimer())
  timer.schedule(Time.now, remapPeriod) {
    fetchHosts(hosts) onSuccess { ring =>
      log.debug("Received: %s", ring)
      val (added, removed) = synchronized {
        val oldSet = hosts.toSet
        hosts = ring.flatMap { h =>
          asScalaIterable(h.endpoints).map {
            new InetSocketAddress(_, port)
          }
        }.toSeq
        val newSet = hosts.toSet
        (newSet &~ oldSet, oldSet &~ newSet)
      }
      added foreach { host => appendChange(FCluster.Add(host)) }
      removed foreach { host => appendChange(FCluster.Rem(host)) }
    } onFailure { error =>
      log.error(error, "error mapping ring")
      statsReceiver.counter("ClusterRemapFailure." + error.getClass().getName()).incr
    }
  }

  private[this] def appendChange(change: FCluster.Change[SocketAddress]) = {
    val newTail = new Promise[Spool[FCluster.Change[SocketAddress]]]
    changes() = Return(change *:: newTail)
    changes = newTail
  }

  def close = timer.stop()

  def snap: (Seq[SocketAddress], Future[Spool[FCluster.Change[SocketAddress]]]) = (hosts, changes)

  private[this] def fetchHosts(hosts: Seq[SocketAddress]) = {
    val ccp = new ClusterClientProvider(
      new SocketAddressCluster(hosts),
      keyspace,
      retries = 10 * seeds.size,
      maxConnectionsPerHost = 1,
      statsReceiver = statsReceiver
    )
    ccp map {
      log.info("Mapping cluster...")
      _.describe_ring(keyspace)
    } ensure {
      ccp.close()
    }
  }
}
