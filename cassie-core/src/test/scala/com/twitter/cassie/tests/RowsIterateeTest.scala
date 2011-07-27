package com.twitter.cassie.tests

import java.util.{List => JList, HashSet => JHashSet, ArrayList => JArrayList}
import org.scalatest.matchers.MustMatchers
import org.scalatest.mock.MockitoSugar
import org.scalatest.{OneInstancePerTest, Spec}
import com.twitter.cassie._
import org.mockito.Mockito.{when, inOrder => inOrderVerify}
import org.mockito.Matchers.{eq => matchEq}
import org.apache.cassandra.finagle.thrift
import scala.collection.JavaConversions._
import com.twitter.util.Future
import scala.collection.mutable.ListBuffer
import com.twitter.cassie.util.ColumnFamilyTestHelper

class RowsIterateeTest extends Spec with MustMatchers with MockitoSugar with OneInstancePerTest with ColumnFamilyTestHelper{

  def co(name: String, value: String, timestamp: Long) = {
    new Column(name, value, Some(timestamp), None)
  }

  def keyRange(start: String, end:String, count:Int) = {
    new thrift.KeyRange().setStart_key(b(start)).setEnd_key(b(end)).setCount(count)
  }

  def keySlice(cf: ColumnFamily[String, String, String], key: String, columns: Seq[Column[String, String]]) = {
    new thrift.KeySlice()
      .setKey(b(key))
      .setColumns(
        asJavaList(columns.map(c => new thrift.ColumnOrSuperColumn().setColumn(Column.convert(cf.nameCodec, cf.valueCodec, cf.clock, c))))
      )
  }

  describe("iterating through an empty column family") {
    val (client, cf) = setup

    when(client.get_range_slices(anyColumnParent, anySlicePredicate , anyKeyRange, anyConsistencyLevel)).thenReturn(
      Future.value(new JArrayList[thrift.KeySlice]())
    )

    val iteratee = cf.rowsIteratee(5, new JHashSet[String]())

    it("doesn't throw an error") {
      val f = iteratee.foreach { case(key, columns) => () }
      f()
    }
  }

  describe("iterating through the columns of a range of keys") {
    val (client, cf) = setup

    when(client.get_range_slices(anyColumnParent, anySlicePredicate , anyKeyRange, anyConsistencyLevel)).thenReturn(
      Future.value(
        asJavaList(List(
          keySlice(cf, "start",  List(co("name", "value", 1), co("name1", "value1", 2))),
          keySlice(cf, "start1", List(co("name", "value", 1), co("name1", "value1", 2))),
          keySlice(cf, "start2", List(co("name", "value", 1), co("name1", "value1", 2))),
          keySlice(cf, "start3", List(co("name", "value", 1), co("name1", "value1", 2)))))),
      Future.value(asJavaList(List(keySlice(cf, "start3", List(co("name", "value", 1), co("name1", "value1", 2))))))
    )

    val iterator = cf.rowsIteratee("start", "end", 5, new JHashSet())

    val data = new ListBuffer[(String, JList[Column[String, String]])]()
    val f = iterator.foreach{ case(key, columns) =>
      data += ((key, columns))
    }
    f()

    it("does a buffered iteration over the columns in the rows in the range") {
      data must equal(ListBuffer(
        ("start",  asJavaList(List(co("name", "value", 1), co("name1", "value1", 2)))),
        ("start1", asJavaList(List(co("name", "value", 1), co("name1", "value1", 2)))),
        ("start2", asJavaList(List(co("name", "value", 1), co("name1", "value1", 2)))),
        ("start3", asJavaList(List(co("name", "value", 1), co("name1", "value1", 2))))
      ))
    }

    it("requests data using the last key as the start key until the end is detected") {
      val f = iterator.foreach { case(key, columns) => () }
      f()
      val cp = new thrift.ColumnParent(cf.name)
      val inOrder = inOrderVerify(client)
      inOrder.verify(client).get_range_slices(matchEq(cp), anySlicePredicate, matchEq(keyRange("start", "end", 5)), anyConsistencyLevel)
      inOrder.verify(client).get_range_slices(matchEq(cp), anySlicePredicate, matchEq(keyRange("start3", "end", 6)), anyConsistencyLevel)
    }
  }
}