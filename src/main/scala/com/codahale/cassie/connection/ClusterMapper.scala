package com.codahale.cassie.connection

import util.parsing.json.JSON
import java.io.IOException
import java.net.InetSocketAddress
import com.codahale.logula.Logging

/**
 * Given a seed host and port, returns a set of nodes in the cluster.
 *
 * @param seedHost the hostname of the seed node
 * @param seedPort the Thrift port of the seed node
 * @author coda
 */
class ClusterMapper(seedHost: String, seedPort: Int) extends Logging {
  private val factory = new ClientFactory(new InetSocketAddress(seedHost, seedPort))

  /**
   * Returns the set of nodes in the cluster.
   */
  def hosts(): Set[InetSocketAddress] = {
    log.info("Mapping cluster...")
    val client = factory.build()
    val json = client.get_string_property("token map")
    factory.destroy(client)
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
