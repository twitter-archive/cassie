// Copyright 2012 Twitter, Inc.

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

// http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.twitter.cassie.tests

import com.twitter.cassie.ClusterRemapper
import com.twitter.conversions.time._
import com.twitter.logging.Logger
import java.net.{ SocketAddress, InetSocketAddress }
import org.apache.cassandra.finagle.thrift
import org.mockito.Mockito.when
import org.scalatest.matchers.MustMatchers
import org.scalatest.{ BeforeAndAfterAll, Spec }
import scala.collection.JavaConversions._

class ClusterRemapperTest extends Spec with MustMatchers with BeforeAndAfterAll {
  // val server = new MockCassandraServer(MockCassandraServer.choosePort())
  // val ring = tr("start", "end", "c1.example.com") ::
  //   tr("start", "end", "c2.example.com") :: Nil
  // when(server.cassandra.describe_ring("keyspace")).thenReturn(asJavaList(ring))
  //
  // def tr(start: String, end: String, endpoints: String*): thrift.TokenRange = {
  //   val tr = new thrift.TokenRange()
  //   tr.setStart_token(start)
  //   tr.setEnd_token(end)
  //   tr.setEndpoints(asJavaList(endpoints))
  // }
  //
  // override protected def beforeAll() {
  //   server.start()
  // }
  //
  // override protected def afterAll() {
  //   server.stop()
  // }

  // describe("mapping a cluster") {
  //   it("returns the set of nodes in the cluster") {
  //     val mapper = new ClusterRemapper("keyspace", "127.0.0.1", 10.minutes, server.port)
  //
  //     val mapped = mapper.fetchHosts(Seq(new InetSocketAddress("127.0.0.1", server.port)))
  //
  //     mapped must equal(List(
  //       addr("c1.example.com", server.port), addr("c2.example.com", server.port)
  //     ))
  //   }
  // }
  //
  // def addr(host: String, port: Int) = new InetSocketAddress(host, port)
}
