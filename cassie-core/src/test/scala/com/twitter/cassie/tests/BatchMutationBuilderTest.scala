package com.twitter.cassie.tests

import scala.collection.JavaConversions._

import com.twitter.cassie.codecs.Utf8Codec
import org.scalatest.Spec
import org.scalatest.matchers.MustMatchers
import org.scalatest.mock.MockitoSugar
import com.twitter.cassie._
import clocks.{MicrosecondEpochClock, Clock}
import com.twitter.cassie.MockCassandraClient.SimpleProvider

class BatchMutationBuilderTest extends Spec with MustMatchers with MockitoSugar {
  val mcc = new MockCassandraClient
  val cf = new ColumnFamily("ks", "People", new SimpleProvider(mcc.client),
        MicrosecondEpochClock.get(), Utf8Codec.get(), Utf8Codec.get(), Utf8Codec.get(),
        ReadConsistency.Quorum, WriteConsistency.Quorum)

  def setup() = new BatchMutationBuilder(cf)
  def enc(string: String) = Utf8Codec.encode(string)

  describe("inserting a column") {
    val builder = setup()
    builder.insert("key", Column("name", "value").timestamp(234))
    val mutations = Mutations(builder)

    it("adds an insertion mutation") {
      val mutation = mutations.get(enc("key")).get("People").get(0)
      val col = mutation.getColumn_or_supercolumn.getColumn
      Utf8Codec.decode(col.name) must equal("name")
      Utf8Codec.decode(col.value) must equal("value")
      col.getTimestamp must equal(234)
    }
  }

  describe("removing a column with an implicit timestamp") {
    val builder = setup()
    builder.removeColumn("key", "column")
    val mutations = Mutations(builder)

    it("adds a deletion mutation") {
      val mutation = mutations.get(enc("key")).get("People").get(0)
      val deletion = mutation.getDeletion

      deletion.getPredicate.getColumn_names.map { Utf8Codec.decode(_) } must equal(List("column"))
    }
  }

  describe("removing a set of columns with an implicit timestamp") {
    val builder = setup()
    builder.removeColumns("key", Set("one", "two"))
    val mutations = Mutations(builder)

    it("adds a deletion mutation") {
      val mutation = mutations.get(enc("key")).get("People").get(0)
      val deletion = mutation.getDeletion

      deletion.getPredicate.getColumn_names.map { Utf8Codec.decode(_) }.sortWith { _ < _ } must equal(List("one", "two"))
    }
  }
  
  describe("removing a set of columns with an explicit timestamp") {
    val builder = setup()
    builder.removeColumns("key", Set("one", "two"), 17)
    val mutations = Mutations(builder)

    it("adds a deletion mutation at the specified time") {
      val mutation = mutations.get(enc("key")).get("People").get(0)
      val deletion = mutation.getDeletion

      deletion.getTimestamp must equal(17)
    }
  }
}
