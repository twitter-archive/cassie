package com.twitter.cassie.tests

import java.nio.ByteBuffer
import java.util.{List => JList, HashSet => JHashSet, ArrayList => JArrayList}
import org.scalatest.matchers.MustMatchers
import org.scalatest.mock.MockitoSugar
import org.scalatest.{OneInstancePerTest, Spec}
import com.twitter.cassie._
import org.mockito.Mockito.{when, inOrder => inOrderVerify, spy, verify, atMost}
import org.mockito.Matchers.{eq => matchEq, any, anyString, anyInt}
import org.apache.cassandra.finagle.thrift
import com.twitter.cassie.codecs.{Utf8Codec}
import scala.collection.JavaConversions._
import com.twitter.cassie.MockCassandraClient.SimpleProvider
import com.twitter.util.Future
import scala.collection.mutable.ListBuffer

class ColumnsIterateeTest extends Spec with MustMatchers with MockitoSugar with OneInstancePerTest {

  def newCf() = {
    val mcc = new MockCassandraClient
    val cf = new ColumnFamily("ks", "People", new SimpleProvider(mcc.client),
          Utf8Codec, Utf8Codec, Utf8Codec,
          ReadConsistency.Quorum, WriteConsistency.Quorum)
    (mcc, cf)
  }

  def cosc(cf: ColumnFamily[String, String, String], c: Column[String, String]) = {
    new thrift.ColumnOrSuperColumn().setColumn(Column.convert(cf.nameCodec, cf.valueCodec, cf.clock, c))
  }
  def c(name: String, value: String, timestamp: Long) = {
    new Column(name, value, Some(timestamp), None)
  }
  def b(string: String) = ByteBuffer.wrap(string.getBytes)

  def pred(start: String, end: String, count: Int) =
    new thrift.SlicePredicate().setSlice_range(
      new thrift.SliceRange().setStart(b(start)).setFinish(b(end))
        .setReversed(false).setCount(count))

  describe("iterating through an empty row") {
    val (mcc, cf) = newCf

    when(mcc.client.get_slice(matchEq(b("foo")), any(classOf[thrift.ColumnParent]) , matchEq(pred("", "", 100)), matchEq(cf.readConsistency.level))).thenReturn(
      Future.value(new JArrayList[thrift.ColumnOrSuperColumn]())
    )

    it("doesn't throw an error") {
      val f = cf.columnsIteratee("foo").foreach { case(column) => () }
      f()
    }
  }

  describe("iterating through the columns of a row") {
    val (mcc, cf) = newCf()

    val columns = asJavaList(List(
      c("first",  "1", 1),
      c("second", "2", 2),
      c("third",  "3", 3),
      c("fourth", "4", 4)
    ))

    val coscs = asJavaList(columns.map{c => cosc(cf, c)})

    when(mcc.client.get_slice(matchEq(b("bar")), any(classOf[thrift.ColumnParent]),
        matchEq(pred("", "", 4)) , matchEq(cf.readConsistency.level))).thenReturn(
      Future.value(coscs)
    )

    when(mcc.client.get_slice(matchEq(b("bar")), any(classOf[thrift.ColumnParent]),
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
      val inOrder = inOrderVerify(mcc.client)
      inOrder.verify(mcc.client).get_slice(matchEq(b("bar")), any(classOf[thrift.ColumnParent]), matchEq(pred("", "", 4)), matchEq(cf.readConsistency.level))
      inOrder.verify(mcc.client).get_slice(matchEq(b("bar")), any(classOf[thrift.ColumnParent]), matchEq(pred("", "", 5)), matchEq(cf.readConsistency.level))

      verify(mcc.client, atMost(2)).get_slice(any(classOf[ByteBuffer]), any(classOf[thrift.ColumnParent]), any(classOf[thrift.SlicePredicate]), any(classOf[thrift.ConsistencyLevel]))
    }
  }
}