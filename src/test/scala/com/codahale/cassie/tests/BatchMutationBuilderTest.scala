package com.codahale.cassie.tests

import scalaj.collection.Imports._
import org.scalatest.Spec
import org.scalatest.matchers.MustMatchers
import org.scalatest.mock.MockitoSugar
import com.codahale.cassie.{Mutations, Column, BatchMutationBuilder}
import com.codahale.cassie.clocks.Clock

class BatchMutationBuilderTest extends Spec with MustMatchers with MockitoSugar {
  implicit val clock = new Clock {
    def timestamp = 445
  }

  describe("inserting a column") {
    val builder = new BatchMutationBuilder("People")
    builder.insert("key", Column("name", "value", 234))
    val mutations = Mutations(builder)

    it("adds an insertion mutation") {
      val mutation = mutations.get("key").get("People").get(0)
      val col = mutation.getColumn_or_supercolumn.getColumn
      new String(col.getName) must equal("name")
      new String(col.getValue) must equal("value")
      col.getTimestamp must equal(234)
    }
  }

  describe("removing a column with an implicit timestamp") {
    val builder = new BatchMutationBuilder("People")
    builder.removeColumn("key", "column")
    val mutations = Mutations(builder)

    it("adds a deletion mutation") {
      val mutation = mutations.get("key").get("People").get(0)
      val deletion = mutation.getDeletion

      deletion.getTimestamp must equal(445)
      deletion.getPredicate.getColumn_names.asScala.map { new String(_) } must equal(List("column"))
    }
  }

  describe("removing a column with an explicit timestamp") {
    val builder = new BatchMutationBuilder("People")
    builder.removeColumnWithTimestamp("key", "column", 22)
    val mutations = Mutations(builder)

    it("adds a deletion mutation") {
      val mutation = mutations.get("key").get("People").get(0)
      val deletion = mutation.getDeletion

      deletion.getTimestamp must equal(22)
      deletion.getPredicate.getColumn_names.asScala.map { new String(_) } must equal(List("column"))
    }
  }

  describe("removing a set of columns with an implicit timestamp") {
    val builder = new BatchMutationBuilder("People")
    builder.removeColumns("key", Set("one", "two"))
    val mutations = Mutations(builder)

    it("adds a deletion mutation") {
      val mutation = mutations.get("key").get("People").get(0)
      val deletion = mutation.getDeletion

      deletion.getTimestamp must equal(445)
      deletion.getPredicate.getColumn_names.asScala.map { new String(_) }.sortWith { _ < _ } must equal(List("one", "two"))
    }
  }

  describe("removing a set of columns with an explicit timestamp") {
    val builder = new BatchMutationBuilder("People")
    builder.removeColumnsWithTimestamp("key", Set("one", "two"), 22)
    val mutations = Mutations(builder)

    it("adds a deletion mutation") {
      val mutation = mutations.get("key").get("People").get(0)
      val deletion = mutation.getDeletion

      deletion.getTimestamp must equal(22)
      deletion.getPredicate.getColumn_names.asScala.map { new String(_) }.sortWith { _ < _ } must equal(List("one", "two"))
    }
  }
}
