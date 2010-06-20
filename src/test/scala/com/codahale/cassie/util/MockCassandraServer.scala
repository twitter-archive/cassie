package com.codahale.cassie.tests.util


import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicInteger
import org.mockito.Mockito.mock
import org.apache.cassandra.thrift.Cassandra
import org.apache.thrift.transport.TServerSocket
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.server.TThreadPoolServer

object MockCassandraServer {
  protected val lastPort = new AtomicInteger(new util.Random().nextInt(10000) + 40000)
  def choosePort() = lastPort.incrementAndGet

  class ServerThread(cassandra: Cassandra.Iface, port: Int) extends Thread {
    setDaemon(true)
    val serverTransport = new TServerSocket(port)
    val protFactory = new TBinaryProtocol.Factory(true, true)
    val processor = new Cassandra.Processor(cassandra)
    val server = new TThreadPoolServer(processor, serverTransport, protFactory)
    val latch = new CountDownLatch(1)

    override def run {
      latch.countDown()
      server.serve()
    }
  }
}

/**
 * Runs a fake Cassandra server on a real Thrift server on a randomly-chosen
 * port.
 */
class MockCassandraServer(val port: Int) {
  val cassandra = mock(classOf[Cassandra.Iface])
  val thread = new MockCassandraServer.ServerThread(cassandra, port)

  def start() {
    thread.start()
    thread.latch.await()
  }

  def stop() {
    thread.server.stop()
  }
}
