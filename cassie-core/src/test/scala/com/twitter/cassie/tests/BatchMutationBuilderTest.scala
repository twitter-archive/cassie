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

import com.twitter.cassie.codecs.Utf8Codec
import com.twitter.cassie.util.ColumnFamilyTestHelper
import com.twitter.cassie._
import org.scalatest.matchers.MustMatchers
import org.scalatest.mock.MockitoSugar
import org.scalatest.Spec
import scala.collection.JavaConversions._

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
