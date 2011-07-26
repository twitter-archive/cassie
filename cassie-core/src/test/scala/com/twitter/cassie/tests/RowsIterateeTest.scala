package com.twitter.cassie.tests

import java.nio.ByteBuffer
import java.util.{List => JList, HashSet => JHashSet, ArrayList => JArrayList}
import org.scalatest.matchers.MustMatchers
import org.scalatest.mock.MockitoSugar
import org.scalatest.{OneInstancePerTest, Spec}
import com.twitter.cassie._
import org.mockito.Mockito.{when, inOrder => inOrderVerify, spy}
import org.mockito.Matchers.{eq => matchEq, any, anyString, anyInt}
import org.apache.cassandra.finagle.thrift
import com.twitter.cassie.codecs.{Utf8Codec}
import scala.collection.JavaConversions._
import com.twitter.cassie.MockCassandraClient.SimpleProvider
import com.twitter.util.Future
import scala.collection.mutable.ListBuffer

class RowsIterateeTest extends Spec with MustMatchers with MockitoSugar with OneInstancePerTest {

  def newCf() = {
    val mcc = new MockCassandraClient
    val cf = new ColumnFamily("ks", "People", new SimpleProvider(mcc.client),
          Utf8Codec, Utf8Codec, Utf8Codec,
          ReadConsistency.Quorum, WriteConsistency.Quorum)
    (mcc, cf)
  }

  def c(name: String, value: String, timestamp: Long) = {
    new Column(name, value, Some(timestamp), None)
  }
  def anyByteBuffer = any(classOf[ByteBuffer])
  def anyString = any(classOf[String])
  def anyInt = any(classOf[Int])
  def b(string: String) = ByteBuffer.wrap(string.getBytes)

  describe("iterating through an empty column family") {
    val (mcc, cf) = newCf

    when(mcc.client.get_range_slices(any(classOf[thrift.ColumnParent]), any(classOf[thrift.SlicePredicate]) , any(classOf[thrift.KeyRange]), any(classOf[thrift.ConsistencyLevel]))).thenReturn(
      Future.value(new JArrayList[thrift.KeySlice]())
    )

    val iteratee = cf.rowsIteratee(5, new JHashSet[String]())

    it("doesn't throw an error") {
      val f = iteratee.foreach { case(key, columns) => () }
      f()
    }
  }

  def keySlice(cf: ColumnFamily[String, String, String], key: String, columns: Seq[Column[String, String]]) = {
    new thrift.KeySlice()
      .setKey(b(key))
      .setColumns(
        asJavaList(columns.map(c => new thrift.ColumnOrSuperColumn().setColumn(Column.convert(cf.nameCodec, cf.valueCodec, cf.clock, c))))
      )
  }

  describe("iterating through the columns of a range of keys") {
    val (mcc, cf) = newCf()

    when(mcc.client.get_range_slices(any(classOf[thrift.ColumnParent]), any(classOf[thrift.SlicePredicate]) , any(classOf[thrift.KeyRange]), any(classOf[thrift.ConsistencyLevel]))).thenReturn(
      Future.value(
        asJavaList(List(
          keySlice(cf, "start",  List(c("name", "value", 1), c("name1", "value1", 2))),
          keySlice(cf, "start1", List(c("name", "value", 1), c("name1", "value1", 2))),
          keySlice(cf, "start2", List(c("name", "value", 1), c("name1", "value1", 2))),
          keySlice(cf, "start3", List(c("name", "value", 1), c("name1", "value1", 2)))))),
      Future.value(asJavaList(List(keySlice(cf, "start3", List(c("name", "value", 1), c("name1", "value1", 2))))))
    )

    val iterator = cf.rowsIteratee("start", "end", 5, new JHashSet())

    val data = new ListBuffer[(String, JList[Column[String, String]])]()
    val f = iterator.foreach{ case(key, columns) =>
      data += ((key, columns))
    }
    f()

    it("does a buffered iteration over the columns in the rows in the range") {
      data must equal(ListBuffer(
        ("start",  asJavaList(List(c("name", "value", 1), c("name1", "value1", 2)))),
        ("start1", asJavaList(List(c("name", "value", 1), c("name1", "value1", 2)))),
        ("start2", asJavaList(List(c("name", "value", 1), c("name1", "value1", 2)))),
        ("start3", asJavaList(List(c("name", "value", 1), c("name1", "value1", 2))))
      ))
    }

    def keyRange(start: String, end:String, count:Int) = {
      new thrift.KeyRange().setStart_key(b(start)).setEnd_key(b(end)).setCount(count)
    }

    it("requests data using the last key as the start key until the end is detected") {
      val f = iterator.foreach { case(key, columns) => () }
      f()
      val cp = new thrift.ColumnParent(cf.name)
      val inOrder = inOrderVerify(mcc.client)
      inOrder.verify(mcc.client).get_range_slices(matchEq(cp), any(classOf[thrift.SlicePredicate]), matchEq(keyRange("start", "end", 5)), any(classOf[thrift.ConsistencyLevel]))
      inOrder.verify(mcc.client).get_range_slices(matchEq(cp), any(classOf[thrift.SlicePredicate]), matchEq(keyRange("start3", "end", 6)), any(classOf[thrift.ConsistencyLevel]))
    }
  }
}