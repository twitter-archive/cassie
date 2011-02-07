package com.twitter.cassie.tests

import org.scalatest.matchers.MustMatchers
import org.mockito.Mockito.when
import org.scalatest.{BeforeAndAfterAll, Spec}
import com.twitter.cassie.tests.util.MockCassandraServer
import java.net.{SocketAddress, InetSocketAddress}
import org.apache.cassandra.thrift

import com.twitter.cassie.ClusterMapper
import com.codahale.logula.Logging
import org.apache.log4j.Level
import scala.collection.JavaConversions._

class ClusterMapperTest extends Spec with MustMatchers with BeforeAndAfterAll {
  val server = new MockCassandraServer(MockCassandraServer.choosePort())
  val ring = tr("start", "end", "c1.example.com") ::
    tr("start", "end", "c2.example.com") :: Nil
  when(server.cassandra.describe_ring("keyspace")).thenReturn(asJavaList(ring))

  def tr(start: String, end: String, endpoints: String*): thrift.TokenRange = {
    val tr = new thrift.TokenRange()
    tr.setStart_token(start)
    tr.setEnd_token(end)
    tr.setEndpoints(asJavaList(endpoints))
  }

  override protected def beforeAll() {
    server.start()
  }

  override protected def afterAll() {
    server.stop()
  }

  describe("mapping a cluster") {
    Logging.configure(_.level = Level.OFF)
    it("returns the set of nodes in the cluster") {
      val mapper = new ClusterMapper("keyspace", "127.0.0.1", server.port)

      val mapped = mapper.mapHosts{h => h}.toSet

      mapped must equal(Set(
        addr("c1.example.com", server.port), addr("c2.example.com", server.port)
      ))
    }
  }

  def addr(host: String, port: Int) = new InetSocketAddress(host, port)
}
