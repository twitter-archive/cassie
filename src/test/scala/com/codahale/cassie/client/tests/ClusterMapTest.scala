package com.codahale.cassie.client.tests

import org.scalatest.matchers.MustMatchers
import org.scalatest.mock.MockitoSugar
import com.codahale.cassie.client.ClusterMap
import org.mockito.Mockito.when
import java.net.InetSocketAddress
import com.codahale.logula.Log
import java.util.logging.Level
import org.scalatest.{BeforeAndAfterAll, Spec}
import com.codahale.cassie.tests.util.MockCassandraServer

class ClusterMapTest extends Spec
        with MustMatchers with MockitoSugar with  BeforeAndAfterAll {

  Log.forClass(classOf[ClusterMap]).level = Level.OFF

  val server = new MockCassandraServer(MockCassandraServer.choosePort())
  when(server.cassandra.get_string_property("token map")).thenReturn("""{"blahblah":"c1.example.com","bleeblee":"c2.example.com"}""")

  override protected def beforeAll() {
    server.start()
  }

  override protected def afterAll() {
    server.stop()
  }

  describe("mapping a cluster") {
    val map = new ClusterMap("127.0.0.1", server.port)

    it("returns a set of nodes in the cluster") {
      map.hosts() must equal(Set(addr("c1.example.com", server.port), addr("c2.example.com", server.port)))
    }
  }

  def addr(host: String, port: Int) = new InetSocketAddress(host, port)
}
