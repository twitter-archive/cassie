package com.twitter.cassie.tests

import java.nio.ByteBuffer
import java.util.{ArrayList, Arrays}


import org.scalatest.matchers.MustMatchers
import org.scalatest.mock.MockitoSugar
import org.scalatest.{OneInstancePerTest, Spec}
import com.twitter.cassie.{Column, ColumnFamily, RowsIteratee}
import org.mockito.Mockito.{when, inOrder => inOrderVerify}
import org.mockito.Matchers.{eq => matchEq, any, anyString, anyInt}
import org.apache.cassandra.finagle.thrift
import com.twitter.cassie.codecs.{Utf8Codec}
import scala.collection.JavaConversions._

import com.twitter.cassie.MockCassandraClient.Fulfillment

class ColumnIteratorTest extends Spec with MustMatchers with MockitoSugar with OneInstancePerTest {
  def newColumn(name: String, value: String, timestamp: Long) = {
    val cosc = new thrift.ColumnOrSuperColumn
    val col = new thrift.Column(b(name))
    col.setValue(b(value))
    col.setTimestamp(timestamp)
    cosc.setColumn(col)
    cosc
  }

  def anyByteBuffer = any(classOf[ByteBuffer])
  def b(string: String) = ByteBuffer.wrap(string.getBytes)

  describe("iterating through an empty column family") {
    val slice = new thrift.KeySlice()
    slice.setKey(b("start"))
    slice.setColumns(asJavaList(List[thrift.ColumnOrSuperColumn]()))

    val predicate = mock[thrift.SlicePredicate]

    val cf = mock[ColumnFamily[String, String, String]]
    when(cf.getRangeSlice(anyByteBuffer, anyByteBuffer, anyInt, matchEq(predicate))).thenReturn(new Fulfillment(new ArrayList[thrift.KeySlice]()))

    val iterator = new RowsIteratee(cf, b("start"), b("end"), 5, predicate, Utf8Codec, Utf8Codec, Utf8Codec).iterator()

    it("doesn't throw an error") {
      iterator.foreach { _ => () }
    }
  }

  describe("iterating through the columns of a range of keys") {
    val slice = new thrift.KeySlice()
    slice.setKey(b("start"))
    slice.setColumns(asJavaList(List(newColumn("name", "value", 1), newColumn("name1", "value1", 2))))

    val slice1 = new thrift.KeySlice()
    slice1.setKey(b("start1"))
    slice1.setColumns(asJavaList(List(newColumn("name", "value", 1), newColumn("name1", "value1", 2))))

    val slice2 = new thrift.KeySlice()
    slice2.setKey(b("start2"))
    slice2.setColumns(asJavaList(List(newColumn("name", "value", 1), newColumn("name1", "value1", 2))))

    val slice3 = new thrift.KeySlice()
    slice3.setKey(b("start3"))
    slice3.setColumns(asJavaList(List(newColumn("name", "value", 1), newColumn("name1", "value1", 2))))

    val predicate = mock[thrift.SlicePredicate]

    val cf = mock[ColumnFamily[String, String, String]]
    when(cf.getRangeSlice(anyByteBuffer, anyByteBuffer, anyInt, matchEq(predicate))).thenReturn(
      new Fulfillment(new ArrayList(Arrays.asList(slice))),
      new Fulfillment(new ArrayList(Arrays.asList(slice1))),
      new Fulfillment(new ArrayList(Arrays.asList(slice2))),
      new Fulfillment(new ArrayList(Arrays.asList(slice3))),
      new Fulfillment(new ArrayList(Arrays.asList(slice3)))
    )

    val iterator = new RowsIteratee(cf, b("start"), b("end"), 5, predicate, Utf8Codec, Utf8Codec, Utf8Codec).iterator()

    it("does a buffered iteration over the columns in the rows in the range") {
      iterator.toList must equal(List(
        ("start", Column("name", "value").timestamp(1)),
        ("start", Column("name1", "value1").timestamp(2)),
        ("start1", Column("name", "value").timestamp(1)),
        ("start1", Column("name1", "value1").timestamp(2)),
        ("start2", Column("name", "value").timestamp(1)),
        ("start2", Column("name1", "value1").timestamp(2)),
        ("start3", Column("name", "value").timestamp(1)),
        ("start3", Column("name1", "value1").timestamp(2))
      ))
    }

    it("requests data using the last key as the start key until a cycle is detected") {
      iterator.foreach { _ => () }

      val inOrder = inOrderVerify(cf)
      inOrder.verify(cf).getRangeSlice(b("start"), b("end"), 6, predicate)
      inOrder.verify(cf).getRangeSlice(b("start"), b("end"), 5, predicate)
      inOrder.verify(cf).getRangeSlice(b("start1"), b("end"), 5, predicate)
      inOrder.verify(cf).getRangeSlice(b("start2"), b("end"), 5, predicate)
      inOrder.verify(cf).getRangeSlice(b("start3"), b("end"), 5, predicate)
    }
  }
}
