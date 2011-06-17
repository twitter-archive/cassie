package com.twitter.cassie.tests.util


import java.util.concurrent.atomic.AtomicInteger
import org.mockito.Mockito.mock
import org.apache.cassandra.finagle.thrift
import org.apache.thrift.transport.{TServerSocket, TFramedTransport}
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.server.TThreadPoolServer

object MockCassandraServer {
  protected val lastPort = new AtomicInteger(new util.Random().nextInt(10000) + 40000)
  def choosePort() = lastPort.incrementAndGet
}

/**
 * Runs a fake Cassandra server on a real Thrift server on a randomly-chosen
 * port.
 */
class MockCassandraServer(val port: Int) {
  val cassandra = mock(classOf[thrift.Cassandra.Iface])
  val thread = new FakeCassandra.ServerThread(cassandra, port)

  def start() {
    thread.start()
    thread.latch.await()
  }

  def stop() {
    thread.server.stop()
  }
}
