package com.codahale.cassie

import connection.ClientFactory
import scala.util.parsing.json.JSON
import java.io.IOException
import java.net.InetSocketAddress
import com.codahale.logula.Logging
import scalaj.collection.Imports._

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
private class ClusterMapper(keyspace: String, seedHost: String, seedPort: Int = 9160, timeoutMS: Int = 10000) extends Logging {
  private val factory = new ClientFactory(new InetSocketAddress(seedHost, seedPort), timeoutMS)

  /**
   * Returns the set of addresses of the nodes in the cluster.
   */
  def hosts(): Set[InetSocketAddress] = {
    log.info("Mapping cluster...")
    val client = factory.build()
    val ring = client.describe_ring(keyspace)
    factory.destroy(client)
    log.debug("Received: %s", ring)
    ring.asScala.flatMap{ _.endpoints.asScala.map{ host =>
      new InetSocketAddress(host, seedPort) } }.toSet
  }
}
