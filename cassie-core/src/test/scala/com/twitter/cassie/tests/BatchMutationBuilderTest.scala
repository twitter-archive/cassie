package com.twitter.cassie.tests

import scala.collection.JavaConversions._
import com.twitter.cassie.codecs.Utf8Codec
import org.scalatest.Spec
import org.scalatest.matchers.MustMatchers
import org.scalatest.mock.MockitoSugar
import com.twitter.cassie._
import com.twitter.cassie.util.ColumnFamilyTestHelper


class BatchMutationBuilderTest extends Spec with MustMatchers with MockitoSugar with ColumnFamilyTestHelper {

  val (client, cf) = setup

  def setupBuilder() = new BatchMutationBuilder(cf)
  def enc(string: String) = Utf8Codec.encode(string)

  describe("inserting a column") {
    val builder = setupBuilder()
    builder.insert("key", Column("name", "value").timestamp(234))
    val mutations = Mutations(builder)

    it("adds an insertion mutation") {
      val mutation = mutations.get(enc("key")).get("cf").get(0)
      val col = mutation.getColumn_or_supercolumn.getColumn
      Utf8Codec.decode(col.name) must equal("name")
      Utf8Codec.decode(col.value) must equal("value")
      col.getTimestamp must equal(234)
    }
  }

  describe("removing a column with an implicit timestamp") {
    val builder = setupBuilder()
    builder.removeColumn("key", "column")
    val mutations = Mutations(builder)

    it("adds a deletion mutation") {
      val mutation = mutations.get(enc("key")).get("cf").get(0)
      val deletion = mutation.getDeletion

      deletion.getPredicate.getColumn_names.map { Utf8Codec.decode(_) } must equal(List("column"))
    }
  }

  describe("removing a set of columns with an implicit timestamp") {
    val builder = setupBuilder()
    builder.removeColumns("key", Set("one", "two"))
    val mutations = Mutations(builder)

    it("adds a deletion mutation") {
      val mutation = mutations.get(enc("key")).get("cf").get(0)
      val deletion = mutation.getDeletion

      deletion.getPredicate.getColumn_names.map { Utf8Codec.decode(_) }.sortWith { _ < _ } must equal(List("one", "two"))
    }
  }

  describe("removing a set of columns with an explicit timestamp") {
    val builder = setupBuilder()
    builder.removeColumns("key", Set("one", "two"), 17)
    val mutations = Mutations(builder)

    it("adds a deletion mutation at the specified time") {
      val mutation = mutations.get(enc("key")).get("cf").get(0)
      val deletion = mutation.getDeletion

      deletion.getTimestamp must equal(17)
    }
  }
}
