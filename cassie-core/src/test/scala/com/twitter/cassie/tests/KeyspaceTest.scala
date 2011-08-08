package com.twitter.cassie.tests

import org.scalatest.{Spec, BeforeAndAfterEach}
import org.scalatest.matchers.MustMatchers
import org.mockito.Matchers.{anyObject}
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import com.twitter.cassie.codecs.Utf8Codec
import com.twitter.cassie.{WriteConsistency, ReadConsistency, Keyspace}
import com.twitter.cassie.connection.ClientProvider
import com.twitter.cassie.Column
import com.twitter.cassie.clocks.Clock
import org.apache.cassandra.finagle.thrift.Cassandra.ServiceToClient
import com.twitter.util.Future
import java.util.{HashMap, Map => JMap, List => JList, ArrayList => JArrayList}
import java.nio.ByteBuffer
import org.apache.cassandra.finagle.thrift
import com.twitter.finagle.stats.NullStatsReceiver

class KeyspaceTest extends Spec with MustMatchers with MockitoSugar with BeforeAndAfterEach {

  case class DumbClientProvider(stc: ServiceToClient) extends ClientProvider {
    def map[A](f: ServiceToClient => Future[A]) = f(stc)
  }

  object StaticClock extends Clock {
    def timestamp: Long = 123456
  }

  var stc: ServiceToClient = null
  var provider: ClientProvider = null
  var keyspace: Keyspace = null

  override def beforeEach {
    stc = mock[ServiceToClient]
    provider = DumbClientProvider(stc)
    keyspace = new Keyspace("MyApp", provider, NullStatsReceiver)
  }

  describe("a keyspace") {

    it("builds a column family with the same ClientProvider") {
      val cf = keyspace.columnFamily[String, String, String]("People", Utf8Codec,Utf8Codec, Utf8Codec)
      cf.keyspace must equal("MyApp")
      cf.name must equal("People")
      cf.readConsistency must equal(ReadConsistency.Quorum)
      cf.writeConsistency must equal(WriteConsistency.Quorum)
      cf.keyCodec must equal(Utf8Codec)
      cf.nameCodec must equal(Utf8Codec)
      cf.valueCodec must equal(Utf8Codec)
      cf.provider must equal(provider)
    }

    it("executes empty batch") {
      keyspace.execute(Seq(), WriteConsistency.One).get()
    }

    it("executes multiple batches") {
      val void = Future(null.asInstanceOf[Void])
      val a = keyspace.columnFamily[String, String, String]("People", Utf8Codec,Utf8Codec, Utf8Codec)
      val b = keyspace.columnFamily[String, String, String]("Dogs", Utf8Codec,Utf8Codec, Utf8Codec)

      // Hard to check equality of separately constructed mutations while the clock is moving
      // out from under us
      a.clock = StaticClock
      b.clock = StaticClock

      val aBatch = a.batch()
      val bBatch = b.batch()

      val tmp = a.batch()
      tmp.insert("foo", Column("bar", "baz"))

      // java.util.Map[ByteBuffer, java.util.Map[String, java.util.List[Mutation]]]
      val expectedMutations = tmp.mutations
      val tmpMap = new JArrayList[JMap[String, JList[thrift.Mutation]]](expectedMutations.values).get(0)
      val col = new JArrayList[JList[thrift.Mutation]](tmpMap.values).get(0)
      tmpMap.put("Dogs", col)

      aBatch.insert("foo", Column("bar", "baz"))
      bBatch.insert("foo", Column("bar", "baz"))
      when(stc.batch_mutate(anyObject(), anyObject())).thenReturn(void);
      keyspace.execute(Seq(aBatch, bBatch), WriteConsistency.Quorum).get()
      verify(stc).batch_mutate(expectedMutations, WriteConsistency.Quorum.level)
    }
  }
}
