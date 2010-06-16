package com.codahale.cassie.connection

import org.apache.cassandra.thrift.Cassandra.Client
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.transport.TSocket
import java.net.InetSocketAddress


/**
 * An abstract class which builds Cassandra clients and destroys existing
 * clients.
 *
 * @author coda
 */
class ClientFactory(val host: InetSocketAddress) {

  /**
   * Opens a new client connection to `host`.
   *
   * @return a `Cassandra.Client` instance
   */
  def build = {
    val socket = new TSocket(host.getHostName, host.getPort)
    socket.open()
    new Client(new TBinaryProtocol(socket))
  }

  /**
   * Closes a `Client`.
   *
   * @param client the `Client` instance to close
   */
  def destroy(client: Client) {
    try {
      client.getOutputProtocol.getTransport.close()
    } catch {
      case e: Exception => // ignore
    }
  }

  override def toString = "ClientFactory(%s)".format(host)
}
