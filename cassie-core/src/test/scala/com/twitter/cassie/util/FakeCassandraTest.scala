package com.twitter.cassie.tests.util

import org.scalatest.matchers.MustMatchers
import org.mockito.Mockito.when
import org.scalatest.{BeforeAndAfterAll, Spec}
import java.net.{SocketAddress, InetSocketAddress}
import org.apache.cassandra.finagle.thrift

import com.twitter.cassie._
import codecs._
import com.twitter.logging.Logger
import scala.collection.JavaConversions._
import com.twitter.conversions.time._
import scala.collection.mutable.ListBuffer
import com.twitter.cassie.connection.CCluster

class FakeCassandraTest extends Spec with MustMatchers with BeforeAndAfterAll {
  val port = 1359
  def factory() = new FakeCassandra(port)
  var server: FakeCassandra = null
  var client: Cluster = null
  var connections = new ListBuffer[Keyspace]

  implicit val stringCodec = Utf8Codec

  override protected def beforeAll() {
    server = factory()
    server.start()
    Thread.sleep(100)
    client = new Cluster(Set("localhost"), port)
  }

  def keyspace() = {
    val keyspace = client.keyspace("foo").mapHostsEvery(0.seconds).connect()
    connections.append(keyspace)
    keyspace
  }

  override protected def afterAll() {
    server.stop()
    connections.map(_.close)
  }

  describe("a fake cassandra") {
    it("should be able to connect to an arbitrary columnfamily and read and write") {
      val cf = keyspace().columnFamily[String, String, String]("bar")
      cf.insert("k", Column("b", "c")).get()
      cf.getRow("k").get().size() must equal(1)
    }
  }
}
