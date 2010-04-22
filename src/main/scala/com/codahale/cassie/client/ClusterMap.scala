package com.codahale.cassie.client

import util.parsing.json.JSON
import java.io.IOException
import java.net.InetSocketAddress
import com.codahale.logula.Logging

/**
 * Given a seed host and port, returns a set of nodes in the cluster.
 *
 * @author coda
 */
class ClusterMap(seedHost: String, seedPort: Int) extends Logging {
  private val seed = new SingleClientProvider(new InetSocketAddress(seedHost, seedPort))

  /**
   * Returns a set of nodes in the cluster.
   */
  def hosts(): Set[InetSocketAddress] = {
    log.info("Mapping cluster...")
    val json = seed.map { _.get_string_property("token map") }
    log.fine("Received: %s", json)
    JSON.parse(json) match {
      case Some(keysAndNodes: List[_]) =>
        val nodes = keysAndNodes.map { h =>
          new InetSocketAddress(h.asInstanceOf[(String, String)]._2, seedPort)
        }.toSet
        log.info("Found %d nodes: %s", nodes.size, nodes)
        nodes
      case None => throw new IOException("Unable to map the cluster.")
    }
  }
}
