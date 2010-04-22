package com.codahale.cassie.client.tests

import org.scalatest.matchers.MustMatchers
import org.scalatest.mock.MockitoSugar
import org.scalatest.{OneInstancePerTest, Spec}
import com.codahale.cassie.client.{ClusterMap, ClientProvider}
import org.mockito.Mockito.when
import org.apache.cassandra.thrift.Cassandra.Client
import java.net.InetSocketAddress
import com.codahale.logula.Log
import java.util.logging.Level

class ClusterMapTest extends Spec
        with MustMatchers with MockitoSugar with OneInstancePerTest {

  Log.forClass(classOf[ClusterMap]).level = Level.OFF

  describe("mapping a cluster") {
    val client = mock[Client]
    when(client.get_string_property("token map")).thenReturn("""{"blahblah":"c1.example.com","bleeblee":"c2.example.com"}""")
    val provider = new ClientProvider {
      def map[A](f: (Client) => A) = f(client)
    }
    val map = new ClusterMap(provider, 9160)

    it("returns a set of nodes in the cluster") {
      map.hosts() must equal(Set(addr("c1.example.com", 9160), addr("c2.example.com", 9160)))
    }
  }

  def addr(host: String, port: Int) = new InetSocketAddress(host, port)
}
