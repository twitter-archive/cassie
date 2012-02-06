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

package com.twitter.cassie.tests.util

import com.twitter.cassie.codecs._
import com.twitter.cassie.connection.CCluster
import com.twitter.cassie._
import com.twitter.conversions.time._
import com.twitter.finagle.stats.NullStatsReceiver
import com.twitter.logging.Logger
import java.net.{ SocketAddress, InetSocketAddress }
import java.util.HashSet
import org.apache.cassandra.finagle.thrift
import org.mockito.Mockito.when
import org.scalatest.matchers.MustMatchers
import org.scalatest._
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

class FakeCassandraTest extends Spec with MustMatchers with BeforeAndAfterAll with BeforeAndAfterEach {
  def factory() = new FakeCassandra
  var server: FakeCassandra = null
  var client: Cluster = null
  var connections = new ListBuffer[Keyspace]

  implicit val stringCodec = Utf8Codec

  override protected def beforeAll() {
    server = factory()
    server.start()
    Thread.sleep(100)
    client = new Cluster(Set("localhost"), server.port.get, NullStatsReceiver)
  }

  def keyspace() = {
    val keyspace = client.mapHostsEvery(0.seconds).keyspace("foo").connect()
    connections.append(keyspace)
    keyspace
  }

  override protected def afterAll() {
    server.stop()
    connections.map(_.close)
  }

  override protected def beforeEach() {
    server.reset()
  }

  describe("a fake cassandra") {

    it("should be able to connect to an arbitrary columnfamily and read and write") {
      val cf = keyspace().columnFamily[String, String, String]("bar", Utf8Codec, Utf8Codec, Utf8Codec)
      cf.insert("k", Column("b", "c"))()
      cf.getRow("k")().size must equal(1)
      cf.getColumn("k", "b")().size must equal(1)
    }

    it("should be able to multiget_slice") {
      val cf = keyspace().columnFamily[String, String, String]("bar", Utf8Codec, Utf8Codec, Utf8Codec)
      var batch = cf.batch
      batch.insert("a", Column("b", "c"))
      batch.insert("a", Column("d", "e"))
      batch.insert("b", Column("d", "e"))
      batch.insert("c", Column("d", "e"))
      batch.execute()()

      val keys = new HashSet[String]
      keys.add("a")
      keys.add("b")
      val columnNames = new HashSet[String]
      columnNames.add("d")

      val response = cf.multigetColumns(keys, columnNames)()
      response.size() must equal(2)
    }

    it("should be able to batch mutate") {
      val cf = keyspace().columnFamily[String, String, String]("bar", Utf8Codec, Utf8Codec, Utf8Codec)
      var batch = cf.batch
      batch.insert("k", Column("b", "c"))
      batch.insert("k", Column("d", "e"))
      batch.execute()()
      cf.getRow("k")().size() must equal(2)

      batch = cf.batch
      batch.removeColumn("k", "b")
      batch.removeColumn("k", "d")
      batch.execute()()
      cf.getRow("k")().size() must equal(0)
    }

    it("should handle counters") {
      var cf = keyspace().counterColumnFamily("counters", Utf8Codec, Utf8Codec)
      cf.add("1", CounterColumn("2", 3))()
      cf.getColumn("1", "2")() must equal(Some(CounterColumn("2", 3)))
      cf.add("1", CounterColumn("2", 4))()
      cf.getColumn("1", "2")() must equal(Some(CounterColumn("2", 7)))
    }

    it("should support out-of-order inserts") {
      val cf = keyspace().columnFamily[String, String, String]("foo", Utf8Codec, Utf8Codec, Utf8Codec)
      cf.insert("x", Column("y", "z").timestamp(2))()
      cf.insert("x", Column("y", "ZZzzz").timestamp(1))()

      cf.getColumn("x", "y").map(_.map(_.value must equal("z")))()
    }

    it("should support super column families") {
      val cf = keyspace().superColumnFamily[String, String, String, String]("super", Utf8Codec, Utf8Codec, Utf8Codec, Utf8Codec)
      cf.insert("a", "b", Column("c", "d").timestamp(1))()
      val row = cf.getRow("a")()
      row.size must equal(1)
      row must equal(Seq(("b", Seq(Column("c", "d").timestamp(1)))))
    }

    it("should support out-of-order inserts to super column families") {
      val cf = keyspace().superColumnFamily[String, String, String, String]("super", Utf8Codec, Utf8Codec, Utf8Codec, Utf8Codec)
      cf.insert("a", "b", Column("c", "d").timestamp(2))()
      cf.insert("a", "b", Column("c", "D").timestamp(1))()
      val row = cf.getRow("a")()
      row.size must equal(1)
      row must equal(Seq(("b", Seq(Column("c", "d").timestamp(2)))))
    }
  }
}
