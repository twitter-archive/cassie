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
import com.twitter.util.Future
import java.nio.ByteBuffer
import java.util.{ ArrayList => JArrayList }
import org.apache.cassandra.finagle.thrift
import org.mockito.ArgumentCaptor
import org.mockito.Matchers.{ eq => matchEq, anyListOf }
import org.mockito.Mockito.{ when, verify }
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.matchers.MustMatchers
import org.scalatest.mock.MockitoSugar
import org.scalatest.Spec
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

@RunWith(classOf[JUnitRunner])
class CounterColumnFamilyTest extends Spec with MustMatchers with MockitoSugar with ColumnFamilyTestHelper {

  describe("getting a columns for a key") {
    val (client, cf) = setupCounters

    it("performs a get_counter_slice with a set of column names") {
      cf.getColumn("key", "name")

      val cp = new thrift.ColumnParent("cf")

      val pred = ArgumentCaptor.forClass(classOf[thrift.SlicePredicate])

      verify(client).get_slice(matchEq(b("key")), matchEq(cp), pred.capture, matchEq(thrift.ConsistencyLevel.QUORUM))

      pred.getValue.getColumn_names.map { Utf8Codec.decode(_) } must equal(List("name"))
    }

    it("returns none if the column doesn't exist") {
      when(client.get_slice(anyByteBuffer(), anyColumnParent(), anySlicePredicate(),
        anyConsistencyLevel()))
        .thenReturn(Future.value(new JArrayList[thrift.ColumnOrSuperColumn]()))
      cf.getColumn("key", "name")() must equal(None)
    }

    it("returns a option of a column if it exists") {
      val columns = Seq(cc("cats", 2L))

      when(client.get_slice(anyByteBuffer, anyColumnParent, anySlicePredicate, anyConsistencyLevel)).thenReturn(Future.value[ColumnList](columns))

      cf.getColumn("key", "cats")() must equal(Some(CounterColumn("cats", 2L)))
    }
  }

  describe("getting a row") {
    val (client, cf) = setupCounters

    it("performs a get_slice with a maxed-out count") {
      val cols = Seq(cc("a", 1), cc("b", 2))
      val cp = new thrift.ColumnParent("cf")

      val pred1 = pred("", "", Int.MaxValue, Order.Normal)

      when(client.get_slice(b("key"), cp, pred1, thrift.ConsistencyLevel.QUORUM)).thenReturn(Future.value[ColumnList](cols))

      cf.getRow("key")
    }

    it("returns a map of column names to columns") {
      val columns = Seq(cc("cats", 2L),
        cc("dogs", 4L))

      when(client.get_slice(anyByteBuffer, anyColumnParent, anySlicePredicate, anyConsistencyLevel)).thenReturn(Future.value[ColumnList](columns))

      cf.getRow("key")() must equal(asJavaMap(Map(
        "cats" -> CounterColumn("cats", 2L),
        "dogs" -> CounterColumn("dogs", 4L)
      )))
    }
  }

  describe("getting a set of columns for a key") {
    val (client, cf) = setupCounters

    it("performs a get_counter_slice with a set of column names") {
      cf.getColumns("key", Set("name", "age"))

      val cp = new thrift.ColumnParent("cf")

      val pred = ArgumentCaptor.forClass(classOf[thrift.SlicePredicate])

      verify(client).get_slice(matchEq(b("key")), matchEq(cp), pred.capture, matchEq(thrift.ConsistencyLevel.QUORUM))

      pred.getValue.getColumn_names.map { Utf8Codec.decode(_) } must equal(List("name", "age"))
    }

    it("returns a map of column names to columns") {
      val columns = Seq(cc("cats", 2L),
        cc("dogs", 3L))

      when(client.get_slice(anyByteBuffer, anyColumnParent, anySlicePredicate, anyConsistencyLevel)).thenReturn(Future.value[ColumnList](columns))

      cf.getColumns("key", Set("cats", "dogs"))() must equal(asJavaMap(Map(
        "cats" -> CounterColumn("cats", 2L),
        "dogs" -> CounterColumn("dogs", 3L)
      )))
    }
  }

  describe("getting a column for a set of keys") {
    val (client, cf) = setupCounters

    it("performs a multiget_counter_slice with a column name") {
      cf.consistency(ReadConsistency.One).multigetColumn(Set("key1", "key2"), "name")

      val keys = List("key1", "key2").map { Utf8Codec.encode(_) }
      val cp = new thrift.ColumnParent("cf")
      val pred = ArgumentCaptor.forClass(classOf[thrift.SlicePredicate])

      verify(client).multiget_slice(matchEq(keys), matchEq(cp), pred.capture, matchEq(thrift.ConsistencyLevel.ONE))

      pred.getValue.getColumn_names.map { Utf8Codec.decode(_) } must equal(List("name"))
    }

    it("returns a map of keys to a map of column names to columns") {
      val results = Map(
        b("us") -> asJavaList(Seq(cc("cats", 2L))),
        b("jp") -> asJavaList(Seq(cc("cats", 4L)))
      )

      when(client.multiget_slice(anyListOf(classOf[ByteBuffer]), anyColumnParent, anySlicePredicate, anyConsistencyLevel)).thenReturn(Future.value[KeyColumnMap](results))

      cf.multigetColumn(Set("us", "jp"), "cats")() must equal(asJavaMap(Map(
        "us" -> CounterColumn("cats", 2L),
        "jp" -> CounterColumn("cats", 4L)
      )))
    }

    it("does not explode when the column doesn't exist for a key") {
      val results = Map(
        b("us") -> asJavaList(Seq(cc("cats", 2L))),
        b("jp") -> (asJavaList(Seq()): ColumnList)
      )

      when(client.multiget_slice(anyListOf(classOf[ByteBuffer]), anyColumnParent, anySlicePredicate, anyConsistencyLevel)).thenReturn(Future.value[KeyColumnMap](results))

      cf.multigetColumn(Set("us", "jp"), "cats")() must equal(asJavaMap(Map(
        "us" -> CounterColumn("cats", 2L)
      )))
    }
  }

  describe("getting a set of columns for a set of keys") {
    val (client, cf) = setupCounters

    it("performs a multiget_counter_slice with a set of column names") {
      cf.consistency(ReadConsistency.One).multigetColumns(Set("us", "jp"), Set("cats", "dogs"))

      val keys = List("us", "jp").map { b(_) }
      val cp = new thrift.ColumnParent("cf")
      val pred = ArgumentCaptor.forClass(classOf[thrift.SlicePredicate])

      verify(client).multiget_slice(matchEq(keys), matchEq(cp), pred.capture, matchEq(thrift.ConsistencyLevel.ONE))

      pred.getValue.getColumn_names.map { Utf8Codec.decode(_) } must equal(List("cats", "dogs"))
    }

    it("returns a map of keys to a map of column names to columns") {
      val results = Map(
        b("us") -> asJavaList(Seq(cc("cats", 2L),
          cc("dogs", 9L))),
        b("jp") -> asJavaList(Seq(cc("cats", 4L),
          cc("dogs", 1L)))
      )

      when(client.multiget_slice(anyListOf(classOf[ByteBuffer]), anyColumnParent, anySlicePredicate, anyConsistencyLevel)).thenReturn(Future.value[KeyColumnMap](results))

      cf.multigetColumns(Set("us", "jp"), Set("cats", "dogs"))() must equal(asJavaMap(Map(
        "us" -> asJavaMap(Map(
          "cats" -> CounterColumn("cats", 2L),
          "dogs" -> CounterColumn("dogs", 9L)
        )),
        "jp" -> asJavaMap(Map(
          "cats" -> CounterColumn("cats", 4L),
          "dogs" -> CounterColumn("dogs", 1L)
        ))
      )))
    }
  }

  describe("adding a column") {
    val (client, cf) = setupCounters

    it("performs an add") {
      cf.add("key", CounterColumn("cats", 55))

      val cp = ArgumentCaptor.forClass(classOf[thrift.ColumnParent])
      val col = cc("cats", 55).counter_column

      verify(client).add(matchEq(b("key")), cp.capture, matchEq(col), matchEq(thrift.ConsistencyLevel.ONE))

      cp.getValue.getColumn_family must equal("cf")
    }
  }

  describe("performing a batch mutation") {
    val (client, cf) = setupCounters

    it("performs a batch_mutate") {
      cf.batch()
        .insert("key", CounterColumn("cats", 201))
        .execute()

      val map = ArgumentCaptor.forClass(classOf[java.util.Map[ByteBuffer, java.util.Map[String, java.util.List[thrift.Mutation]]]])

      verify(client).batch_mutate(map.capture, matchEq(thrift.ConsistencyLevel.ONE))

      val mutations = map.getValue
      val mutation = mutations.get(b("key")).get("cf").get(0)
      val col = mutation.getColumn_or_supercolumn.getCounter_column
      Utf8Codec.decode(col.name) must equal("cats")
      col.value must equal(201)
    }
  }

  describe("paging through columns") {
    val (client, cf) = setupCounters

    it("can return exactly one column") {
      val key = "trance"
      val cp = new thrift.ColumnParent("cf")

      val columns1 = Seq(cc("a", 1))

      val range1 = new thrift.SliceRange(b(""), b(""), false, 2)
      val pred1 = pred("", "", 2, Order.Normal)
      pred1.setSlice_range(range1)
      when(client.get_slice(b(key), cp, pred1, thrift.ConsistencyLevel.QUORUM)).thenReturn(Future.value[ColumnList](columns1))

      val l = new ListBuffer[String]
      cf.columnsIteratee(2, key).foreach { c => l.append(c.name) }
      l must equal(List("a"))
    }

    it("fetches multiple slices") {
      val key = "trance"
      val cp = new thrift.ColumnParent("cf")

      val columns1 = Seq(cc("cat", 1), cc("name", 1))
      val columns2 = Seq(cc("name", 1), cc("radish", 1), cc("sofa", 1))
      val columns3 = Seq(cc("sofa", 1), cc("xray", 1))

      val pred1 = pred("", "", 2, Order.Normal)
      when(client.get_slice(b(key), cp, pred1, thrift.ConsistencyLevel.QUORUM)).thenReturn(Future.value[ColumnList](columns1))

      val pred2 = pred("name", "", 3, Order.Normal)
      when(client.get_slice(b(key), cp, pred2, thrift.ConsistencyLevel.QUORUM)).thenReturn(Future.value[ColumnList](columns2))

      val pred3 = pred("sofa", "", 3, Order.Normal)
      when(client.get_slice(b(key), cp, pred3, thrift.ConsistencyLevel.QUORUM)).thenReturn(Future.value[ColumnList](columns3))

      val l = new ListBuffer[String]
      val done = cf.columnsIteratee(2, key).foreach { c => l.append(c.name) }
      done()
      l must equal(List("cat", "name", "radish", "sofa", "xray"))
    }
  }

}
