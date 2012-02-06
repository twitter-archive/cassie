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
import org.mockito.Matchers.{ eq => matchEq }
import org.mockito.Mockito.{ when, inOrder => inOrderVerify, verify, atMost }
import org.scalatest.matchers.MustMatchers
import org.scalatest.mock.MockitoSugar
import org.scalatest.{ OneInstancePerTest, Spec }
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

class ColumnsIterateeTest extends Spec with MustMatchers with MockitoSugar with OneInstancePerTest with ColumnFamilyTestHelper {

  def co(name: String, value: String, timestamp: Long) = {
    new Column(name, value, Some(timestamp), None)
  }

  describe("iterating through an empty row") {
    val (client, cf) = setup

    when(client.get_slice(matchEq(b("foo")), anyColumnParent, matchEq(pred("", "", 100)), matchEq(cf.readConsistency.level))).thenReturn(
      Future.value(new JArrayList[thrift.ColumnOrSuperColumn]())
    )

    it("doesn't throw an error") {
      val f = cf.columnsIteratee("foo").foreach { case (column) => () }
      f()
    }
  }

  describe("iterating through the columns of a row") {
    val (client, cf) = setup

    val columns = asJavaList(List(
      co("first", "1", 1),
      co("second", "2", 2),
      co("third", "3", 3),
      co("fourth", "4", 4)
    ))

    val coscs = asJavaList(columns.map { c => cosc(cf, c) })

    when(client.get_slice(matchEq(b("bar")), anyColumnParent,
      matchEq(pred("", "", 4)), matchEq(cf.readConsistency.level))).thenReturn(
      Future.value(coscs)
    )

    when(client.get_slice(matchEq(b("bar")), anyColumnParent,
      matchEq(pred("fourth", "", 5)), matchEq(cf.readConsistency.level))).thenReturn(
      Future.value(asJavaList(List(coscs.get(3))))
    )

    val data2 = new ListBuffer[Column[String, String]]()

    val f = cf.columnsIteratee(4, "bar").foreach { column =>
      data2 += column
    }
    f()

    it("does a buffered iteration over the columns in the rows in the range") {
      asJavaList(data2) must equal(columns)
    }

    it("requests data using the last key as the start key until the end is detected") {
      val cp = new thrift.ColumnParent(cf.name)
      val inOrder = inOrderVerify(client)
      inOrder.verify(client).get_slice(matchEq(b("bar")), anyColumnParent, matchEq(pred("", "", 4)), matchEq(cf.readConsistency.level))
      inOrder.verify(client).get_slice(matchEq(b("bar")), anyColumnParent, matchEq(pred("fourth", "", 5)), matchEq(cf.readConsistency.level))

      verify(client, atMost(2)).get_slice(anyByteBuffer, anyColumnParent, anySlicePredicate, anyConsistencyLevel)
    }
  }

  describe("iterating through the columns of a row, with a limit and reversed order") {
    val (client, cf) = setup

    val columns = asJavaList(List(
      co("first", "1", 1),
      co("second", "2", 2),
      co("third", "3", 3),
      co("fourth", "4", 4)
    ))

    val expectedColumns = asJavaList(List(
      co("third", "3", 3),
      co("second", "2", 2)
    ))

    val coscs = asJavaList(columns.map { c => cosc(cf, c) })

    when(client.get_slice(matchEq(b("bar")), anyColumnParent,
      matchEq(pred("third", "", 1, Order.Reversed)), matchEq(cf.readConsistency.level))).thenReturn(
      Future.value(asJavaList(List(coscs.get(2))))
    )

    when(client.get_slice(matchEq(b("bar")), anyColumnParent,
      matchEq(pred("third", "", 2, Order.Reversed)), matchEq(cf.readConsistency.level))).thenReturn(
      Future.value(asJavaList(List(coscs.get(2), coscs.get(1))))
    )

    val data2 = new ListBuffer[Column[String, String]]()

    val f = cf.columnsIteratee(1, "bar", Some("third"), None, 2, Order.Reversed).foreach { column =>
      data2 += column
    }
    f()

    it("does a buffered iteration over the columns in the rows in the range") {
      asJavaList(data2) must equal(expectedColumns)
    }
  }

  describe("iterating through the columns of a row, skipping anything outside start and end") {
    val (client, cf) = setup

    val columns1 = asJavaList(List(
      co("first", "1", 1),
      co("second", "2", 2)
    ))
    val columns2 = asJavaList(List(
      co("second", "2", 2),
      co("third", "3", 3),
      co("fourth", "4", 4)
    ))
    val expectedColumns = List(
      co("first", "1", 1),
      co("second", "2", 2),
      co("third", "3", 3),
      co("fourth", "4", 4)
    )

    val coscs1 = asJavaList(columns1.map { c => cosc(cf, c) })
    val coscs2 = asJavaList(columns2.map { c => cosc(cf, c) })

    when(client.get_slice(matchEq(b("bar")), anyColumnParent,
      matchEq(pred("first", "fourth", 2)), matchEq(cf.readConsistency.level))).thenReturn(
      Future.value(coscs1)
    )

    when(client.get_slice(matchEq(b("bar")), anyColumnParent,
      matchEq(pred("second", "fourth", 3)), matchEq(cf.readConsistency.level))).thenReturn(
      Future.value(coscs2)
    )

    when(client.get_slice(matchEq(b("bar")), anyColumnParent,
      matchEq(pred("fourth", "fourth", 3)), matchEq(cf.readConsistency.level))).thenReturn(
      Future.value(asJavaList(List(coscs2.get(2))))
    )

    val data2 = new ListBuffer[Column[String, String]]()

    val f = cf.columnsIteratee(2, "bar", Some("first"), Some("fourth")).foreach { column =>
      data2 += column
    }
    f()

    it("does a buffered iteration over the columns in the rows in the range") {
      data2 must equal(expectedColumns)
    }

    it("requests data using the last key as the start key until the end is detected") {
      val inOrder = inOrderVerify(client)
      inOrder.verify(client).get_slice(matchEq(b("bar")), anyColumnParent, matchEq(pred("first", "fourth", 2)), matchEq(cf.readConsistency.level))
      inOrder.verify(client).get_slice(matchEq(b("bar")), anyColumnParent, matchEq(pred("second", "fourth", 3)), matchEq(cf.readConsistency.level))
      inOrder.verify(client).get_slice(matchEq(b("bar")), anyColumnParent, matchEq(pred("fourth", "fourth", 3)), matchEq(cf.readConsistency.level))

      verify(client, atMost(3)).get_slice(anyByteBuffer, anyColumnParent, anySlicePredicate, anyConsistencyLevel)
    }
  }
}