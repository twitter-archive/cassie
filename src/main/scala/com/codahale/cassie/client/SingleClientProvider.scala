package com.codahale.cassie.client

import org.apache.cassandra.thrift.Cassandra.Client
import org.apache.thrift.transport.TSocket
import org.apache.thrift.protocol.TBinaryProtocol
import com.codahale.logula.Logging
import java.net.InetSocketAddress

class SingleClientProvider(address: InetSocketAddress)
        extends ClientProvider with Logging {
  def map[A](f: (Client) => A): A = {
    log.fine("Connecting to %s", address)
    val socket = new TSocket(address.getAddress.getHostAddress, address.getPort)
    try {
      socket.open()
      val client = new Client(new TBinaryProtocol(socket))
      return f(client)
    } finally {
      log.fine("Disconnecting from %s", address)
      if (socket.isOpen) {
        socket.close()
      }
    }
  }
}
