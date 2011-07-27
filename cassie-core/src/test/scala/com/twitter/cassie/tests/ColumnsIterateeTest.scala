package com.twitter.cassie.tests

import java.util.{List => JList, HashSet => JHashSet, ArrayList => JArrayList}
import org.scalatest.matchers.MustMatchers
import org.scalatest.mock.MockitoSugar
import org.scalatest.{OneInstancePerTest, Spec}
import com.twitter.cassie._
import org.mockito.Mockito.{when, inOrder => inOrderVerify, verify, atMost}
import org.mockito.Matchers.{eq => matchEq}
import org.apache.cassandra.finagle.thrift
import scala.collection.JavaConversions._
import com.twitter.util.Future
import scala.collection.mutable.ListBuffer
import com.twitter.cassie.util.ColumnFamilyTestHelper


class ColumnsIterateeTest extends Spec with MustMatchers with MockitoSugar with OneInstancePerTest with ColumnFamilyTestHelper{

  def co(name: String, value: String, timestamp: Long) = {
    new Column(name, value, Some(timestamp), None)
  }

  describe("iterating through an empty row") {
    val (client, cf) = setup

    when(client.get_slice(matchEq(b("foo")), anyColumnParent , matchEq(pred("", "", 100)), matchEq(cf.readConsistency.level))).thenReturn(
      Future.value(new JArrayList[thrift.ColumnOrSuperColumn]())
    )

    it("doesn't throw an error") {
      val f = cf.columnsIteratee("foo").foreach { case(column) => () }
      f()
    }
  }

  describe("iterating through the columns of a row") {
    val (client, cf) = setup

    val columns = asJavaList(List(
      co("first",  "1", 1),
      co("second", "2", 2),
      co("third",  "3", 3),
      co("fourth", "4", 4)
    ))

    val coscs = asJavaList(columns.map{c => cosc(cf, c)})

    when(client.get_slice(matchEq(b("bar")), anyColumnParent,
        matchEq(pred("", "", 4)) , matchEq(cf.readConsistency.level))).thenReturn(
      Future.value(coscs)
    )

    when(client.get_slice(matchEq(b("bar")), anyColumnParent,
        matchEq(pred("fourth", "", 5)) , matchEq(cf.readConsistency.level))).thenReturn(
      Future.value(asJavaList(List(coscs.get(3))))
    )

    val data2 = new ListBuffer[Column[String, String]]()

    val f = cf.columnsIteratee(4, "bar").foreach{ column =>
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
}