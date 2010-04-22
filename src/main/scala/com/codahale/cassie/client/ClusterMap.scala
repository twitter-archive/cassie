package com.codahale.cassie.client

import util.parsing.json.JSON
import java.io.IOException
import java.net.InetSocketAddress
import com.codahale.logula.Logging

/**
 * Given a ClientProvider, returns a set of nodes in the cluster.
 *
 * @author coda
 */
class ClusterMap(seed: ClientProvider, defaultPort: Int) extends Logging {

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
          new InetSocketAddress(h.asInstanceOf[(String, String)]._2, defaultPort)
        }.toSet
        log.info("Found %d nodes: %s", nodes.size, nodes)
        nodes
      case None => throw new IOException("Unable to map the cluster.")
    }
  }
}
