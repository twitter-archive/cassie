package com.twitter.cassie.tests

import scala.collection.JavaConversions._
import org.scalatest.Spec
import org.scalatest.matchers.MustMatchers
import org.scalatest.mock.MockitoSugar
import org.apache.cassandra.thrift.Cassandra.ServiceToClient
import com.twitter.cassie.codecs.Utf8Codec
import org.mockito.Mockito.{when, verify}
import org.mockito.Matchers.{anyString, any, eq => matchEq, anyListOf}
import org.apache.cassandra.thrift
import org.mockito.ArgumentCaptor
import java.nio.ByteBuffer
import com.twitter.cassie.clocks.Clock
import thrift.Mutation
import com.twitter.cassie._

import MockCassandraClient._
import com.twitter.cassie.util.ColumnFamilyTestHelper

/**
 * Note that almost all calls on a ColumnFamily would normally be asynchronous.
 * But in this case, MockCassandraClient takes asynchronicity out of the equation.
 */
class CounterColumnFamilyTest extends Spec with MustMatchers with MockitoSugar with ColumnFamilyTestHelper {
  val counterColumnFamily = new CounterColumnFamily("ks", "cf", new SimpleProvider(mockCassandraClient.client),
        Utf8Codec.get(), Utf8Codec.get(),
        ReadConsistency.Quorum)
  
  type ColumnList = java.util.List[thrift.Counter]
  type KeyColumnMap = java.util.Map[java.nio.ByteBuffer,ColumnList]

  def newColumn(name: String, value: Long) = {
    val counter = new thrift.Counter
    counter.setColumn(new thrift.CounterColumn(Utf8Codec.encode(name), value))
    counter
  }
  def b(keyString: String) = ByteBuffer.wrap(keyString.getBytes)


  describe("getting a columns for a key") {
    val client = mockCassandraClient.client

    it("performs a get_slice with a set of column names") {
      columnFamily.getColumn("key", "name")

      val cp = new thrift.ColumnParent("cf")

      val pred = ArgumentCaptor.forClass(classOf[thrift.SlicePredicate])

      verify(client).get_slice(matchEq(b("key")), matchEq(cp), pred.capture, matchEq(thrift.ConsistencyLevel.QUORUM))

      pred.getValue.getColumn_names.map { Utf8Codec.decode(_) } must equal(List("name"))
    }

    it("returns none if the column doesn't exist") {
      columnFamily.getColumn("key", "cats")() must equal(None)
    }

    it("returns a option of a column if it exists") {
      val columns = Seq(newColumn("cats", 2L))

      when(client.get_counter_slice(anyByteBuffer, anyColumnParent, anySlicePredicate, anyConsistencyLevel)).thenReturn(new Fulfillment[ColumnList](columns))

      columnFamily.getColumn("key", "cats")() must equal(Some(CounterColumn("cats", 2L)))
    }
  }
/*
  describe("getting a row") {
    val client = mockCassandraClient.client

    it("performs a get_slice with a maxed-out count") {
      columnFamily.getRow("key")

      val cp = new thrift.ColumnParent("cf")

      val range = new thrift.SliceRange(b(""), b(""), false, Int.MaxValue)
      val pred = new thrift.SlicePredicate()
      pred.setSlice_range(range)

      verify(client).get_slice(b("key"), cp, pred, thrift.ConsistencyLevel.QUORUM)
    }

    it("returns a map of column names to columns") {
      val columns = Seq(newColumn("name", "Coda", 2292L),
                        newColumn("age", "old", 11919L))

      when(client.get_slice(anyByteBuffer, anyColumnParent, anySlicePredicate, anyConsistencyLevel)).thenReturn(new Fulfillment[ColumnList](columns))

      columnFamily.getRow("key")() must equal(asJavaMap(Map(
        "name" -> Column("name", "Coda", 2292L),
        "age" -> Column("age", "old", 11919L)
      )))
    }
  }

  describe("getting a row slice") {
    val client = mockCassandraClient.client

    it("performs a get_slice with the specified count, reversedness, start column name and end column name") {
      val startColumnName = "somewhere"
      val endColumnName   = "overTheRainbow"
      columnFamily.getRowSlice("key", Some(startColumnName), Some(endColumnName), 100, Order.Reversed)

      val cp = new thrift.ColumnParent("cf")

      val range = new thrift.SliceRange(Utf8Codec.encode(startColumnName), Utf8Codec.encode(endColumnName), true, 100)
      val pred  = new thrift.SlicePredicate()
      pred.setSlice_range(range)

      verify(client).get_slice(b("key"), cp, pred, thrift.ConsistencyLevel.QUORUM)
    }
  }

  describe("getting a set of columns for a key") {
    val client = mockCassandraClient.client

    it("performs a get_slice with a set of column names") {
      columnFamily.getColumns("key", Set("name", "age"))

      val cp = new thrift.ColumnParent("cf")

      val pred = ArgumentCaptor.forClass(classOf[thrift.SlicePredicate])

      verify(client).get_slice(matchEq(b("key")), matchEq(cp), pred.capture, matchEq(thrift.ConsistencyLevel.QUORUM))

      pred.getValue.getColumn_names.map { Utf8Codec.decode(_) } must equal(List("name", "age"))
    }

    it("returns a map of column names to columns") {
      val columns = Seq(newColumn("name", "Coda", 2292L),
                        newColumn("age", "old", 11919L))

      when(client.get_slice(anyByteBuffer, anyColumnParent, anySlicePredicate, anyConsistencyLevel)).thenReturn(new Fulfillment[ColumnList](columns))

      columnFamily.getColumns("key", Set("name", "age"))() must equal(asJavaMap(Map(
        "name" -> Column("name", "Coda", 2292L),
        "age" -> Column("age", "old", 11919L)
      )))
    }
  }

  describe("getting a column for a set of keys") {
    val client = mockCassandraClient.client

    it("performs a multiget_slice with a column name") {
      columnFamily.consistency(ReadConsistency.One).multigetColumn(Set("key1", "key2"), "name")

      val keys = List("key1", "key2").map{Utf8Codec.encode(_)}
      val cp = new thrift.ColumnParent("cf")
      val pred = ArgumentCaptor.forClass(classOf[thrift.SlicePredicate])

      verify(client).multiget_slice(matchEq(keys), matchEq(cp), pred.capture, matchEq(thrift.ConsistencyLevel.ONE))

      pred.getValue.getColumn_names.map { Utf8Codec.decode(_) } must equal(List("name"))
    }

    it("returns a map of keys to a map of column names to columns") {
      val results = Map(
        b("key1") -> asJavaList(Seq(newColumn("name", "Coda", 2292L))),
        b("key2") -> asJavaList(Seq(newColumn("name", "Niki", 422L)))
      )

      when(client.multiget_slice(anyListOf(classOf[ByteBuffer]), anyColumnParent, anySlicePredicate, anyConsistencyLevel)).thenReturn(new Fulfillment[KeyColumnMap](results))

      columnFamily.multigetColumn(Set("key1", "key2"), "name")() must equal(asJavaMap(Map(
        "key1" -> Column("name", "Coda", 2292L),
        "key2" -> Column("name", "Niki", 422L)
      )))
    }

    it("does not explode when the column doesn't exist for a key") {
      val results = Map(
        b("key1") -> asJavaList(Seq(newColumn("name", "Coda", 2292L))),
        b("key2") -> (asJavaList(Seq()): ColumnList)
      )

      when(client.multiget_slice(anyListOf(classOf[ByteBuffer]), anyColumnParent, anySlicePredicate, anyConsistencyLevel)).thenReturn(new Fulfillment[KeyColumnMap](results))

      columnFamily.multigetColumn(Set("key1", "key2"), "name")() must equal(asJavaMap(Map(
        "key1" -> Column("name", "Coda", 2292L)
      )))
    }
  }

  describe("getting a set of columns for a set of keys") {
    val client = mockCassandraClient.client

    it("performs a multiget_slice with a set of column names") {
      columnFamily.consistency(ReadConsistency.One).multigetColumns(Set("key1", "key2"), Set("name", "age"))

      val keys = List("key1", "key2").map{ b(_) }
      val cp = new thrift.ColumnParent("cf")
      val pred = ArgumentCaptor.forClass(classOf[thrift.SlicePredicate])

      verify(client).multiget_slice(matchEq(keys), matchEq(cp), pred.capture, matchEq(thrift.ConsistencyLevel.ONE))

      pred.getValue.getColumn_names.map { Utf8Codec.decode(_) } must equal(List("name", "age"))
    }

    it("returns a map of keys to a map of column names to columns") {
      val results = Map(
        b("key1") -> asJavaList(Seq(newColumn("name", "Coda", 2292L),
                                newColumn("age", "old", 11919L))),
        b("key2") -> asJavaList(Seq(newColumn("name", "Niki", 422L),
                                newColumn("age", "lithe", 129L)))
      )

      when(client.multiget_slice(anyListOf(classOf[ByteBuffer]), anyColumnParent, anySlicePredicate, anyConsistencyLevel)).thenReturn(new Fulfillment[KeyColumnMap](results))

      columnFamily.multigetColumns(Set("key1", "key2"), Set("name", "age"))() must equal(asJavaMap(Map(
        "key1" -> asJavaMap(Map(
          "name" -> Column("name", "Coda", 2292L),
          "age" -> Column("age", "old", 11919L)
        )),
        "key2" -> asJavaMap(Map(
          "name" -> Column("name", "Niki", 422L),
          "age" -> Column("age", "lithe", 129L)
        ))
      )))
    }
  }

  describe("inserting a column") {
    val client = mockCassandraClient.client

    it("performs an insert") {
      columnFamily.insert("key", Column("name", "Coda", 55))

      val cp = ArgumentCaptor.forClass(classOf[thrift.ColumnParent])
      val col = newColumn("name", "Coda", 55).column

      verify(client).insert(matchEq(b("key")), cp.capture, matchEq(col), matchEq(thrift.ConsistencyLevel.QUORUM))

      cp.getValue.getColumn_family must equal("cf")
    }
  }

  describe("removing a row with a specific timestamp") {
    val client = mockCassandraClient.client

    it("performs a remove") {
      columnFamily.removeRowWithTimestamp("key", 55)

      val cp = ArgumentCaptor.forClass(classOf[thrift.ColumnPath])
      verify(client).remove(matchEq(b("key")), cp.capture, matchEq(55L), matchEq(thrift.ConsistencyLevel.QUORUM))

      cp.getValue.column_family must equal("cf")
      cp.getValue.column must be(null)
    }
  }

  describe("performing a batch mutation") {
    val client = mockCassandraClient.client

    it("performs a batch_mutate") {
      columnFamily.consistency(WriteConsistency.All).batch()
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
    val client = mockCassandraClient.client

    it("returns a ColumnIterator with an all-column predicate") {
      val iterator = columnFamily.rowIteratee(16)

      iterator.cf must equal(columnFamily)
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
    val client = mockCassandraClient.client

    it("returns a ColumnIterator with a single-column predicate") {
      val iterator = columnFamily.columnIteratee(16, "name")

      iterator.cf must equal(columnFamily)
      iterator.startKey must equal(b(""))
      iterator.endKey must equal(b(""))
      iterator.batchSize must equal(16)
      iterator.predicate.getColumn_names.map { Utf8Codec.decode(_) } must be(List("name"))
    }
  }

  describe("iterating through a set of columns of all rows") {
    val client = mockCassandraClient.client

    it("returns a ColumnIterator with a column-list predicate") {
      val iterator = columnFamily.columnsIteratee(16, Set("name", "motto"))

      iterator.cf must equal(columnFamily)
      iterator.startKey must equal(b(""))
      iterator.endKey must equal(b(""))
      iterator.batchSize must equal(16)
      iterator.predicate.getColumn_names.map { Utf8Codec.decode(_) }.toSet must be(Set("name", "motto"))
    }
  }
  */
}