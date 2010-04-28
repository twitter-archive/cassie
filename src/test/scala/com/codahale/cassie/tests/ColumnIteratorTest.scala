package com.codahale.cassie.tests

import scalaj.collection.Imports._
import org.scalatest.matchers.MustMatchers
import org.scalatest.mock.MockitoSugar
import org.scalatest.{OneInstancePerTest, Spec}
import collection.mutable.ArrayBuffer
import com.codahale.cassie.{Column, ReadConsistency, ColumnFamily, ColumnIterator}
import org.mockito.Mockito.{when, inOrder => inOrderVerify}
import org.mockito.Matchers.{eq => matchEq, anyString, anyInt}
import org.apache.cassandra.thrift
import com.codahale.cassie.codecs.{Utf8Codec}

class ColumnIteratorTest extends Spec with MustMatchers with MockitoSugar with OneInstancePerTest {
  def newColumn(name: String, value: String, timestamp: Long) = {
    val cosc = new thrift.ColumnOrSuperColumn
    cosc.setColumn(new thrift.Column(name.getBytes, value.getBytes, timestamp))
    cosc
  }

  describe("iterating through the columns of a range of keys") {
    val slice = new thrift.KeySlice()
    slice.setKey("start")
    slice.setColumns(List(newColumn("name", "value", 1), newColumn("name1", "value1", 2)).asJava)

    val slice1 = new thrift.KeySlice()
    slice1.setKey("start1")
    slice1.setColumns(List(newColumn("name", "value", 1), newColumn("name1", "value1", 2)).asJava)

    val slice2 = new thrift.KeySlice()
    slice2.setKey("start2")
    slice2.setColumns(List(newColumn("name", "value", 1), newColumn("name1", "value1", 2)).asJava)

    val slice3 = new thrift.KeySlice()
    slice3.setKey("start3")
    slice3.setColumns(List(newColumn("name", "value", 1), newColumn("name1", "value1", 2)).asJava)

    val predicate = mock[thrift.SlicePredicate]

    val cf = mock[ColumnFamily[String, String]]
    when(cf.getRangeSlice(anyString, anyString, anyInt, matchEq(predicate), matchEq(ReadConsistency.Quorum))).thenReturn(
      List(slice),
      List(slice1),
      List(slice2),
      List(slice3),
      List(slice3)
    )

    val iterator = new ColumnIterator(cf, "start", "end", 5, predicate, ReadConsistency.Quorum, Utf8Codec, Utf8Codec)

    it("does a buffered iteration over the columns in the rows in the range") {
      val results = new ArrayBuffer[(String, Column[String, String])]
      for ((key, col) <- iterator) {
        results += ((key, col))
      }
      results must equal(List(
        ("start", Column("name", "value", 1)),
        ("start", Column("name1", "value1", 2)),
        ("start1", Column("name", "value", 1)),
        ("start1", Column("name1", "value1", 2)),
        ("start2", Column("name", "value", 1)),
        ("start2", Column("name1", "value1", 2)),
        ("start3", Column("name", "value", 1)),
        ("start3", Column("name1", "value1", 2))
      ))
    }

    it("requests data using the last key as the start key until a cycle is detected") {
      iterator.foreach { _ => () }

      val inOrder = inOrderVerify(cf)
      inOrder.verify(cf).getRangeSlice("start", "end", 6, predicate, ReadConsistency.Quorum)
      inOrder.verify(cf).getRangeSlice("start", "end", 5, predicate, ReadConsistency.Quorum)
      inOrder.verify(cf).getRangeSlice("start1", "end", 5, predicate, ReadConsistency.Quorum)
      inOrder.verify(cf).getRangeSlice("start2", "end", 5, predicate, ReadConsistency.Quorum)
      inOrder.verify(cf).getRangeSlice("start3", "end", 5, predicate, ReadConsistency.Quorum)
    }
  }
}
