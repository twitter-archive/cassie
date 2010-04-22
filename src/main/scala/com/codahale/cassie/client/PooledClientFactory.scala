package com.codahale.cassie.client

import org.apache.commons.pool.BasePoolableObjectFactory
import org.apache.thrift.transport.TSocket
import org.apache.cassandra.thrift.Cassandra.Client
import org.apache.thrift.protocol.TBinaryProtocol
import com.codahale.logula.Logging

/**
 * Creates Cassandra connections for the nodes specified by a HostSelector.
 *
 * @author coda
 */
class PooledClientFactory(selector: HostSelector)
        extends BasePoolableObjectFactory with Logging {

  override def validateObject(obj: Any) = {
    obj match {
      case client: Client =>
        log.finer("Validating connection: %s", client)
        try {
          client.describe_version()
          true
        } catch {
          case e: Exception =>
            log.warning(e, "Bad connection: %s", client)
            false
        }
      case _ => false
    }
  }

  override def destroyObject(obj: Any) = {
    obj match {
      case client: Client =>
        log.finer("Closing connection: %s", client)
        val transport = client.getOutputProtocol.getTransport
        if (transport.isOpen) {
          transport.close()
        }
    }
  }

  def makeObject = {
    val host = selector.next
    log.finer("Opening connection: %s", host)
    val socket = new TSocket(host.getHostName, host.getPort)
    socket.open()
    new Client(new TBinaryProtocol(socket))
  }
}
