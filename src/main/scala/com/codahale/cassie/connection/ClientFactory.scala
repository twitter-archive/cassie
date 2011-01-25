package com.codahale.cassie.connection

import org.apache.cassandra.thrift.Cassandra.Client
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.transport.TSocket
import java.net.InetSocketAddress
import com.codahale.logula.Logging

/**
 * A class which builds Cassandra clients and destroys existing clients.
 *
 * @author coda
 */
class ClientFactory(val host: InetSocketAddress, val keyspace: String, val timeoutMS: Int) extends Logging {

  /**
   * Opens a new client connection to `host`.
   *
   * @return a `Cassandra.Client` instance!
   */
  def build() = {
    log.debug("Opening a new socket to %s", host)
    val socket = new TSocket(host.getHostName, host.getPort)
    socket.setTimeout(timeoutMS)
    socket.open()
    val client = new Client(new TBinaryProtocol(socket))
    client.set_keyspace(keyspace)
    client
  }

  /**
   * Closes a `Client`.
   *
   * @param client the `Client` instance to close
   */
  def destroy(client: Client) {
    try {
      log.debug("Disconnecting from %s", host)
      client.getOutputProtocol.getTransport.close()
    } catch {
      case e: Exception => log.trace(e, "Error disconnecting from %s", host)
    }
  }

  override def toString = "ClientFactory(%s)".format(host)
}
