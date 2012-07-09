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

import com.twitter.cassie.util.ColumnFamilyTestHelper
import com.twitter.cassie._
import com.twitter.util.Future
import java.util.{ List => JList, HashSet => JHashSet, ArrayList => JArrayList }
import org.apache.cassandra.finagle.thrift
import org.junit.runner.RunWith
import org.mockito.Matchers.{ eq => matchEq }
import org.mockito.Mockito.{ when, inOrder => inOrderVerify }
import org.scalatest.junit.JUnitRunner
import org.scalatest.matchers.MustMatchers
import org.scalatest.mock.MockitoSugar
import org.scalatest.{ FunSpec, OneInstancePerTest }
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

@RunWith(classOf[JUnitRunner])
class RowsIterateeTest extends FunSpec with MustMatchers with MockitoSugar with OneInstancePerTest with ColumnFamilyTestHelper {

  def co(name: String, value: String, timestamp: Long) = {
    new Column(name, value, Some(timestamp), None)
  }

  def keyRange(start: String, end: String, count: Int) = {
    new thrift.KeyRange().setStart_key(b(start)).setEnd_key(b(end)).setCount(count)
  }

  def keySlice(cf: ColumnFamily[String, String, String], key: String, columns: Seq[Column[String, String]]) = {
    new thrift.KeySlice()
      .setKey(b(key))
      .setColumns(
        seqAsJavaList(columns.map(c => new thrift.ColumnOrSuperColumn().setColumn(Column.convert(cf.nameCodec, cf.valueCodec, cf.clock, c))))
      )
  }

  describe("iterating through an empty column family") {
    val (client, cf) = setup

    when(client.get_range_slices(anyColumnParent, anySlicePredicate, anyKeyRange, anyConsistencyLevel)).thenReturn(
      Future.value(new JArrayList[thrift.KeySlice]())
    )

    val iteratee = cf.rowsIteratee(5, new JHashSet[String]())

    it("doesn't throw an error") {
      val f = iteratee.foreach { case (key, columns) => () }
      f()
    }
  }

  describe("iterating through the columns of a range of keys") {
    val (client, cf) = setup

    when(client.get_range_slices(anyColumnParent, anySlicePredicate, anyKeyRange, anyConsistencyLevel)).thenReturn(
      Future.value(
        seqAsJavaList(List(
          keySlice(cf, "start", List(co("name", "value", 1), co("name1", "value1", 2))),
          keySlice(cf, "start1", List(co("name", "value", 1), co("name1", "value1", 2))),
          keySlice(cf, "start2", List(co("name", "value", 1), co("name1", "value1", 2))),
          keySlice(cf, "start3", List(co("name", "value", 1), co("name1", "value1", 2)))))),
      Future.value(seqAsJavaList(List(keySlice(cf, "start3", List(co("name", "value", 1), co("name1", "value1", 2))))))
    )

    val iterator = cf.rowsIteratee("start", "end", 5, new JHashSet())

    val data = new ListBuffer[(String, JList[Column[String, String]])]()
    val f = iterator.foreach {
      case (key, columns) =>
        data += ((key, columns))
    }
    f()

    it("does a buffered iteration over the columns in the rows in the range") {
      data must equal(ListBuffer(
        ("start", seqAsJavaList(List(co("name", "value", 1), co("name1", "value1", 2)))),
        ("start1", seqAsJavaList(List(co("name", "value", 1), co("name1", "value1", 2)))),
        ("start2", seqAsJavaList(List(co("name", "value", 1), co("name1", "value1", 2)))),
        ("start3", seqAsJavaList(List(co("name", "value", 1), co("name1", "value1", 2))))
      ))
    }

    it("requests data using the last key as the start key until the end is detected") {
      val f = iterator.foreach { case (key, columns) => () }
      f()
      val cp = new thrift.ColumnParent(cf.name)
      val inOrder = inOrderVerify(client)
      inOrder.verify(client).get_range_slices(matchEq(cp), anySlicePredicate, matchEq(keyRange("start", "end", 5)), anyConsistencyLevel)
      inOrder.verify(client).get_range_slices(matchEq(cp), anySlicePredicate, matchEq(keyRange("start3", "end", 6)), anyConsistencyLevel)
    }
  }

  describe("map") {
    val (client, cf) = setup

    when(client.get_range_slices(anyColumnParent, anySlicePredicate, anyKeyRange, anyConsistencyLevel)).thenReturn(
      Future.value(
        seqAsJavaList(List(
          keySlice(cf, "start", List(co("name", "value", 1), co("name1", "value1", 2))),
          keySlice(cf, "start1", List(co("name", "value", 1), co("name1", "value1", 2))),
          keySlice(cf, "start2", List(co("name", "value", 1), co("name1", "value1", 2))),
          keySlice(cf, "start3", List(co("name", "value", 1), co("name1", "value1", 2)))))),
      Future.value(seqAsJavaList(List(keySlice(cf, "start3", List(co("name", "value", 1), co("name1", "value1", 2))))))
    )

    val data = cf.rowsIteratee("start", "end", 5, new JHashSet()).map{(k, c) => (k + "foo", c)}()

    it("it maps over the data") {
      data must equal(ListBuffer(
        ("startfoo", seqAsJavaList(List(co("name", "value", 1), co("name1", "value1", 2)))),
        ("start1foo", seqAsJavaList(List(co("name", "value", 1), co("name1", "value1", 2)))),
        ("start2foo", seqAsJavaList(List(co("name", "value", 1), co("name1", "value1", 2)))),
        ("start3foo", seqAsJavaList(List(co("name", "value", 1), co("name1", "value1", 2))))
      ))
    }
  }
}
