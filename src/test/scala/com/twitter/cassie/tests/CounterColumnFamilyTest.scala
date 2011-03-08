package com.twitter.cassie.tests

import scala.collection.JavaConversions._
import org.scalatest.Spec
import org.scalatest.matchers.MustMatchers
import org.scalatest.mock.MockitoSugar
import com.twitter.cassie.codecs.Utf8Codec
import org.mockito.Mockito.{when, verify}
import org.mockito.Matchers.{eq => matchEq, anyListOf}
import org.apache.cassandra.thrift
import org.mockito.ArgumentCaptor
import java.nio.ByteBuffer
import com.twitter.cassie._

import MockCassandraClient._

/**
 * Note that almost all calls on a ColumnFamily would normally be asynchronous.
 * But in this case, MockCassandraClient takes asynchronicity out of the equation.
 */
class CounterColumnFamilyTest extends Spec with MustMatchers with MockitoSugar {

  type ColumnList = java.util.List[thrift.Counter]
  type KeyColumnMap = java.util.Map[java.nio.ByteBuffer,ColumnList]

  def newColumn(name: String, value: Long) = {
    val counter = new thrift.Counter
    counter.setColumn(new thrift.CounterColumn(Utf8Codec.encode(name), value))
    counter
  }
  def b(keyString: String) = ByteBuffer.wrap(keyString.getBytes)

  def setup = {
    val mcc = new MockCassandraClient
    val cf = new CounterColumnFamily("ks", "cf", new SimpleProvider(mcc.client),
        Utf8Codec.get(), Utf8Codec.get(), ReadConsistency.Quorum)
    (mcc.client, cf)
  }

  describe("getting a columns for a key") {
    val (client, cf) = setup

    it("performs a get_counter_slice with a set of column names") {
      cf.getColumn("key", "name")

      val cp = new thrift.ColumnParent("cf")

      val pred = ArgumentCaptor.forClass(classOf[thrift.SlicePredicate])

      verify(client).get_counter_slice(matchEq(b("key")), matchEq(cp), pred.capture, matchEq(thrift.ConsistencyLevel.QUORUM))

      pred.getValue.getColumn_names.map { Utf8Codec.decode(_) } must equal(List("name"))
    }

    it("returns none if the column doesn't exist") {
      cf.getColumn("key", "name")() must equal(None)
    }

    it("returns a option of a column if it exists") {
      val columns = Seq(newColumn("cats", 2L))

      when(client.get_counter_slice(anyByteBuffer, anyColumnParent, anySlicePredicate, anyConsistencyLevel)).thenReturn(new Fulfillment[ColumnList](columns))

      cf.getColumn("key", "cats")() must equal(Some(CounterColumn("cats", 2L)))
    }
  }

  describe("getting a row") {
    val (client, cf) = setup

    it("performs a get_slice with a maxed-out count") {
      cf.getRow("key")

      val cp = new thrift.ColumnParent("cf")

      val range = new thrift.SliceRange(b(""), b(""), false, Int.MaxValue)
      val pred = new thrift.SlicePredicate()
      pred.setSlice_range(range)

      verify(client).get_counter_slice(b("key"), cp, pred, thrift.ConsistencyLevel.QUORUM)
    }

    it("returns a map of column names to columns") {
      val columns = Seq(newColumn("cats", 2L),
                        newColumn("dogs", 4L))

      when(client.get_counter_slice(anyByteBuffer, anyColumnParent, anySlicePredicate, anyConsistencyLevel)).thenReturn(new Fulfillment[ColumnList](columns))

      cf.getRow("key")() must equal(asJavaMap(Map(
        "cats" -> CounterColumn("cats", 2L),
        "dogs" -> CounterColumn("dogs", 4L)
      )))
    }
  }

  describe("getting a set of columns for a key") {
    val (client, cf) = setup

    it("performs a get_counter_slice with a set of column names") {
      cf.getColumns("key", Set("name", "age"))

      val cp = new thrift.ColumnParent("cf")

      val pred = ArgumentCaptor.forClass(classOf[thrift.SlicePredicate])

      verify(client).get_counter_slice(matchEq(b("key")), matchEq(cp), pred.capture, matchEq(thrift.ConsistencyLevel.QUORUM))

      pred.getValue.getColumn_names.map { Utf8Codec.decode(_) } must equal(List("name", "age"))
    }

    it("returns a map of column names to columns") {
      val columns = Seq(newColumn("cats", 2L),
                        newColumn("dogs", 3L))

      when(client.get_counter_slice(anyByteBuffer, anyColumnParent, anySlicePredicate, anyConsistencyLevel)).thenReturn(new Fulfillment[ColumnList](columns))

      cf.getColumns("key", Set("cats", "dogs"))() must equal(asJavaMap(Map(
        "cats" -> CounterColumn("cats", 2L),
        "dogs" -> CounterColumn("dogs", 3L)
      )))
    }
  }

  describe("getting a column for a set of keys") {
    val (client, cf) = setup

    it("performs a multiget_counter_slice with a column name") {
      cf.consistency(ReadConsistency.One).multigetColumn(Set("key1", "key2"), "name")

      val keys = List("key1", "key2").map{Utf8Codec.encode(_)}
      val cp = new thrift.ColumnParent("cf")
      val pred = ArgumentCaptor.forClass(classOf[thrift.SlicePredicate])

      verify(client).multiget_counter_slice(matchEq(keys), matchEq(cp), pred.capture, matchEq(thrift.ConsistencyLevel.ONE))

      pred.getValue.getColumn_names.map { Utf8Codec.decode(_) } must equal(List("name"))
    }

    it("returns a map of keys to a map of column names to columns") {
      val results = Map(
        b("us") -> asJavaList(Seq(newColumn("cats", 2L))),
        b("jp") -> asJavaList(Seq(newColumn("cats", 4L)))
      )

      when(client.multiget_counter_slice(anyListOf(classOf[ByteBuffer]), anyColumnParent, anySlicePredicate, anyConsistencyLevel)).thenReturn(new Fulfillment[KeyColumnMap](results))

      cf.multigetColumn(Set("us", "jp"), "cats")() must equal(asJavaMap(Map(
        "us" -> CounterColumn("cats", 2L),
        "jp" -> CounterColumn("cats", 4L)
      )))
    }

    it("does not explode when the column doesn't exist for a key") {
      val results = Map(
        b("us") -> asJavaList(Seq(newColumn("cats", 2L))),
        b("jp") -> (asJavaList(Seq()): ColumnList)
      )

      when(client.multiget_counter_slice(anyListOf(classOf[ByteBuffer]), anyColumnParent, anySlicePredicate, anyConsistencyLevel)).thenReturn(new Fulfillment[KeyColumnMap](results))

      cf.multigetColumn(Set("us", "jp"), "cats")() must equal(asJavaMap(Map(
        "us" -> CounterColumn("cats", 2L)
      )))
    }
  }

  describe("getting a set of columns for a set of keys") {
    val (client, cf) = setup

    it("performs a multiget_counter_slice with a set of column names") {
      cf.consistency(ReadConsistency.One).multigetColumns(Set("us", "jp"), Set("cats", "dogs"))

      val keys = List("us", "jp").map{ b(_) }
      val cp = new thrift.ColumnParent("cf")
      val pred = ArgumentCaptor.forClass(classOf[thrift.SlicePredicate])

      verify(client).multiget_counter_slice(matchEq(keys), matchEq(cp), pred.capture, matchEq(thrift.ConsistencyLevel.ONE))

      pred.getValue.getColumn_names.map { Utf8Codec.decode(_) } must equal(List("cats", "dogs"))
    }

    it("returns a map of keys to a map of column names to columns") {
      val results = Map(
        b("us") -> asJavaList(Seq(newColumn("cats", 2L),
                                newColumn("dogs", 9L))),
        b("jp") -> asJavaList(Seq(newColumn("cats", 4L),
                                newColumn("dogs", 1L)))
      )

      when(client.multiget_counter_slice(anyListOf(classOf[ByteBuffer]), anyColumnParent, anySlicePredicate, anyConsistencyLevel)).thenReturn(new Fulfillment[KeyColumnMap](results))

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
    val (client, cf) = setup

    it("performs an add") {
      cf.add("key", CounterColumn("cats", 55))

      val cp = ArgumentCaptor.forClass(classOf[thrift.ColumnParent])
      val col = newColumn("cats", 55).column

      verify(client).add(matchEq(b("key")), cp.capture, matchEq(col), matchEq(thrift.ConsistencyLevel.ONE))

      cp.getValue.getColumn_family must equal("cf")
    }
  }

  /* TODO: Not implemented
  describe("performing a batch mutation") {
    val (client, cf) = setup

    it("performs a batch_mutate") {
      cf.consistency(WriteConsistency.All).batch()
        .insert("key", Column("name", "value", 201))
        .execute()

      val map = ArgumentCaptor.forClass(classOf[java.util.Map[ByteBuffer, java.util.Map[String, java.util.List[Mutation]]]])

      verify(client).batch_mutate(map.capture, matchEq(thrift.ConsistencyLevel.ALL))

      val mutations = map.getValue
      val mutation = mutations.get(b("key")).get("cf").get(0)
      val col = mutation.getColumn_or_supercolumn.getColumn
      Utf8Codec.decode(col.name) must equal("name")
      Utf8Codec.decode(col.value) must equal("value")
      col.getTimestamp must equal(201)
    }
  }

  describe("iterating through all columns of all rows") {
    val (client, cf) = setup

    it("returns a ColumnIterator with an all-column predicate") {
      val iterator = cf.rowIteratee(16)

      iterator.cf must equal(cf)
      iterator.startKey must equal(b(""))
      iterator.endKey must equal(b(""))
      iterator.batchSize must equal(16)
      iterator.predicate.getColumn_names must be(null)
      iterator.predicate.getSlice_range.getStart must equal(Array[Byte]())
      iterator.predicate.getSlice_range.getFinish must equal(Array[Byte]())
      iterator.predicate.getSlice_range.getCount must equal(Int.MaxValue)
    }
  }

  describe("iterating through one column of all rows") {
    val (client, cf) = setup

    it("returns a ColumnIterator with a single-column predicate") {
      val iterator = cf.columnIteratee(16, "name")

      iterator.cf must equal(cf)
      iterator.startKey must equal(b(""))
      iterator.endKey must equal(b(""))
      iterator.batchSize must equal(16)
      iterator.predicate.getColumn_names.map { Utf8Codec.decode(_) } must be(List("name"))
    }
  }

  describe("iterating through a set of columns of all rows") {
    val (client, cf) = setup

    it("returns a ColumnIterator with a column-list predicate") {
      val iterator = cf.columnsIteratee(16, Set("name", "motto"))

      iterator.cf must equal(cf)
      iterator.startKey must equal(b(""))
      iterator.endKey must equal(b(""))
      iterator.batchSize must equal(16)
      iterator.predicate.getColumn_names.map { Utf8Codec.decode(_) }.toSet must be(Set("name", "motto"))
    }
  }
  */
}
