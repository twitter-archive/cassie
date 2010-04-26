package com.codahale.cassie.tests

import org.scalatest.Spec
import org.scalatest.matchers.MustMatchers
import org.scalatest.mock.MockitoSugar
import org.apache.cassandra.thrift.Cassandra.Client
import com.codahale.cassie.client.ClientProvider
import com.codahale.cassie.codecs.Utf8Codec
import org.mockito.Mockito.{when, verify}
import org.mockito.Matchers.{anyString, any, eq => matchEq, anyListOf}
import org.apache.cassandra.thrift
import scalaj.collection.Imports._
import org.mockito.ArgumentCaptor
import java.util.ArrayList
import com.codahale.cassie.clocks.Clock
import thrift.Mutation
import com.codahale.cassie._

class ColumnFamilyTest extends Spec with MustMatchers with MockitoSugar {
  case class SimpleProvider(client: Client) extends ClientProvider {
    def map[A](f: (Client) => A) = f(client)
  }

  def anyColumnParent = any(classOf[thrift.ColumnParent])
  def anySlicePredicate = any(classOf[thrift.SlicePredicate])
  def anyConsistencyLevel = any(classOf[thrift.ConsistencyLevel])
  def newColumn(name: Array[Byte], value: Array[Byte], timestamp: Long) = {
    val cosc = new thrift.ColumnOrSuperColumn
    cosc.setColumn(new thrift.Column(name, value, timestamp))
    cosc
  }

  def setup = {
    val client = mock[Client]
    val provider = SimpleProvider(client)
    val cf = new ColumnFamily("ks", "cf", provider, Utf8Codec, Utf8Codec)

    (client, cf)
  }

  describe("getting a columns for a key") {
    val (client, cf) = setup

    it("performs a get_slice with a set of column names") {
      cf.get("key", "name", ReadConsistency.Quorum)

      val cp = new thrift.ColumnParent("cf")

      val pred = ArgumentCaptor.forClass(classOf[thrift.SlicePredicate])

      verify(client).get_slice(matchEq("ks"), matchEq("key"), matchEq(cp), pred.capture, matchEq(thrift.ConsistencyLevel.QUORUM))

      pred.getValue.getColumn_names.asScala.map { new String(_) } must equal(List("name"))
    }

    it("returns a option of a column if it exists") {
      val columns = newColumn("name".getBytes, "Coda".getBytes, 2292L) :: Nil

      when(client.get_slice(anyString, anyString, anyColumnParent, anySlicePredicate, anyConsistencyLevel)).thenReturn(columns.asJava)

      cf.get("key", "name", ReadConsistency.Quorum) must equal(Some(Column("name", "Coda", 2292L)))
    }

    it("returns none if the column doesn't exist") {
      when(client.get_slice(anyString, anyString, anyColumnParent, anySlicePredicate, anyConsistencyLevel)).thenReturn(new ArrayList[thrift.ColumnOrSuperColumn])

      cf.get("key", "name", ReadConsistency.Quorum) must equal(None)
    }
  }

  describe("getting all columns for a key") {
    val (client, cf) = setup

    it("performs a get_slice with a maxed-out count") {
      cf.get("key", ReadConsistency.Quorum)

      val cp = new thrift.ColumnParent("cf")

      val range = new thrift.SliceRange(Array(), Array(), false, Int.MaxValue)
      val pred = new thrift.SlicePredicate()
      pred.setSlice_range(range)

      verify(client).get_slice("ks", "key", cp, pred, thrift.ConsistencyLevel.QUORUM)
    }

    it("returns a map of column names to columns") {
      val columns = newColumn("name".getBytes, "Coda".getBytes, 2292L) ::
                    newColumn("age".getBytes, "old".getBytes, 11919L) :: Nil

      when(client.get_slice(anyString, anyString, anyColumnParent, anySlicePredicate, anyConsistencyLevel)).thenReturn(columns.asJava)

      cf.get("key", ReadConsistency.Quorum) must equal(Map(
        "name" -> Column("name", "Coda", 2292L),
        "age" -> Column("age", "old", 11919L)
      ))
    }
  }

  describe("getting a set of columns for a key") {
    val (client, cf) = setup

    it("performs a get_slice with a set of column names") {
      cf.get("key", Set("name", "age"), ReadConsistency.Quorum)

      val cp = new thrift.ColumnParent("cf")

      val pred = ArgumentCaptor.forClass(classOf[thrift.SlicePredicate])

      verify(client).get_slice(matchEq("ks"), matchEq("key"), matchEq(cp), pred.capture, matchEq(thrift.ConsistencyLevel.QUORUM))

      pred.getValue.getColumn_names.asScala.map { new String(_) } must equal(List("name", "age"))
    }

    it("returns a map of column names to columns") {
      val columns = newColumn("name".getBytes, "Coda".getBytes, 2292L) ::
                    newColumn("age".getBytes, "old".getBytes, 11919L) :: Nil

      when(client.get_slice(anyString, anyString, anyColumnParent, anySlicePredicate, anyConsistencyLevel)).thenReturn(columns.asJava)

      cf.get("key", Set("name", "age"), ReadConsistency.Quorum) must equal(Map(
        "name" -> Column("name", "Coda", 2292L),
        "age" -> Column("age", "old", 11919L)
      ))
    }
  }

  describe("getting a column for a set of keys") {
    val (client, cf) = setup

    it("performs a multiget_slice with a column name") {
      cf.multiget(Set("key1", "key2"), "name", ReadConsistency.One)

      val keys = List("key1", "key2").asJava
      val cp = new thrift.ColumnParent("cf")
      val pred = ArgumentCaptor.forClass(classOf[thrift.SlicePredicate])

      verify(client).multiget_slice(matchEq("ks"), matchEq(keys), matchEq(cp), pred.capture, matchEq(thrift.ConsistencyLevel.ONE))

      pred.getValue.getColumn_names.asScala.map { new String(_) } must equal(List("name"))
    }

    it("returns a map of keys to a map of column names to columns") {
      val results = Map(
        "key1" -> (newColumn("name".getBytes, "Coda".getBytes, 2292L) :: Nil).asJava,
        "key2" -> (newColumn("name".getBytes, "Niki".getBytes, 422L) :: Nil).asJava
      ).asJava

      when(client.multiget_slice(anyString, anyListOf(classOf[String]), anyColumnParent, anySlicePredicate, anyConsistencyLevel)).thenReturn(results)

      cf.multiget(Set("key1", "key2"), "name", ReadConsistency.Quorum) must equal(Map(
        "key1" -> Column("name", "Coda", 2292L),
        "key2" -> Column("name", "Niki", 422L)
      ))
    }
  }

  describe("getting a set of columns for a set of keys") {
    val (client, cf) = setup

    it("performs a multiget_slice with a set of column names") {
      cf.multiget(Set("key1", "key2"), Set("name", "age"), ReadConsistency.One)

      val keys = List("key1", "key2").asJava
      val cp = new thrift.ColumnParent("cf")
      val pred = ArgumentCaptor.forClass(classOf[thrift.SlicePredicate])

      verify(client).multiget_slice(matchEq("ks"), matchEq(keys), matchEq(cp), pred.capture, matchEq(thrift.ConsistencyLevel.ONE))

      pred.getValue.getColumn_names.asScala.map { new String(_) } must equal(List("name", "age"))
    }

    it("returns a map of keys to a map of column names to columns") {
      val results = Map(
        "key1" -> (newColumn("name".getBytes, "Coda".getBytes, 2292L) ::
                    newColumn("age".getBytes, "old".getBytes, 11919L) :: Nil).asJava,
        "key2" -> (newColumn("name".getBytes, "Niki".getBytes, 422L) ::
                    newColumn("age".getBytes, "lithe".getBytes, 129L) :: Nil).asJava
      ).asJava

      when(client.multiget_slice(anyString, anyListOf(classOf[String]), anyColumnParent, anySlicePredicate, anyConsistencyLevel)).thenReturn(results)

      cf.multiget(Set("key1", "key2"), Set("name", "age"), ReadConsistency.Quorum) must equal(Map(
        "key1" -> Map(
          "name" -> Column("name", "Coda", 2292L),
          "age" -> Column("age", "old", 11919L)
        ),
        "key2" -> Map(
          "name" -> Column("name", "Niki", 422L),
          "age" -> Column("age", "lithe", 129L)
        )
      ))
    }
  }

  describe("inserting a column") {
    val (client, cf) = setup

    it("performs an insert") {
      cf.insert("key", Column("name", "Coda", 55), WriteConsistency.Quorum)

      val cp = ArgumentCaptor.forClass(classOf[thrift.ColumnPath])
      val value = ArgumentCaptor.forClass(classOf[Array[Byte]])

      verify(client).insert(matchEq("ks"), matchEq("key"), cp.capture, value.capture, matchEq(55), matchEq(thrift.ConsistencyLevel.QUORUM))

      cp.getValue.getColumn_family must equal("cf")
      new String(cp.getValue.getColumn) must equal("name")

      new String(value.getValue) must equal("Coda")
    }
  }

  describe("removing a column with an implicit timestamp") {
    val (client, cf) = setup
    implicit val clock = new Clock {
      def timestamp = 445
    }

    it("performs a remove") {
      cf.remove("key", "age", WriteConsistency.Quorum)

      val cp = ArgumentCaptor.forClass(classOf[thrift.ColumnPath])
      verify(client).remove(matchEq("ks"), matchEq("key"), cp.capture, matchEq(445), matchEq(thrift.ConsistencyLevel.QUORUM))

      cp.getValue.getColumn_family must equal("cf")
      new String(cp.getValue.getColumn) must equal("age")
    }
  }

  describe("removing a column with a specific timestamp") {
    val (client, cf) = setup

    it("performs a remove") {
      cf.remove("key", "age", 55, WriteConsistency.Quorum)

      val cp = ArgumentCaptor.forClass(classOf[thrift.ColumnPath])
      verify(client).remove(matchEq("ks"), matchEq("key"), cp.capture, matchEq(55), matchEq(thrift.ConsistencyLevel.QUORUM))

      cp.getValue.getColumn_family must equal("cf")
      new String(cp.getValue.getColumn) must equal("age")
    }
  }

  describe("removing a row with a specific timestamp") {
    val (client, cf) = setup

    it("performs a remove") {
      cf.remove("key", 55, WriteConsistency.Quorum)

      val cp = ArgumentCaptor.forClass(classOf[thrift.ColumnPath])
      verify(client).remove(matchEq("ks"), matchEq("key"), cp.capture, matchEq(55), matchEq(thrift.ConsistencyLevel.QUORUM))

      cp.getValue.getColumn_family must equal("cf")
      cp.getValue.getColumn must be(null)
    }
  }

  describe("removing a row with an implicit timestamp") {
    val (client, cf) = setup
    implicit val clock = new Clock {
      def timestamp = 445
    }

    it("performs a remove") {
      cf.remove("key", WriteConsistency.Quorum)

      val cp = ArgumentCaptor.forClass(classOf[thrift.ColumnPath])
      verify(client).remove(matchEq("ks"), matchEq("key"), cp.capture, matchEq(445), matchEq(thrift.ConsistencyLevel.QUORUM))

      cp.getValue.getColumn_family must equal("cf")
      cp.getValue.getColumn must be(null)
    }
  }

  describe("removing a set of columns from a row with an implicit timestamp") {
    val (client, cf) = setup
    implicit val clock = new Clock {
      def timestamp = 445
    }

    it("performs a batch mutate") {
      cf.remove("key", Set("one", "two"), WriteConsistency.Quorum)

      val map = ArgumentCaptor.forClass(classOf[java.util.Map[String, java.util.Map[String, java.util.List[Mutation]]]])

      verify(client).batch_mutate(matchEq("ks"), map.capture, matchEq(thrift.ConsistencyLevel.QUORUM))

      val mutations = map.getValue
      val mutation = mutations.get("key").get("cf").get(0)
      val deletion = mutation.getDeletion

      deletion.getTimestamp must equal(445)
      deletion.getPredicate.getColumn_names.asScala.map { new String(_) }.sortWith { _ < _ } must equal(List("one", "two"))
    }
  }

  describe("removing a set of columns from a row with an explicit timestamp") {
    val (client, cf) = setup

    it("performs a batch mutate") {
      cf.remove("key", Set("one", "two"), 33, WriteConsistency.Quorum)

      val map = ArgumentCaptor.forClass(classOf[java.util.Map[String, java.util.Map[String, java.util.List[Mutation]]]])

      verify(client).batch_mutate(matchEq("ks"), map.capture, matchEq(thrift.ConsistencyLevel.QUORUM))

      val mutations = map.getValue
      val mutation = mutations.get("key").get("cf").get(0)
      val deletion = mutation.getDeletion

      deletion.getTimestamp must equal(33)
      deletion.getPredicate.getColumn_names.asScala.map { new String(_) }.sortWith { _ < _ } must equal(List("one", "two"))
    }
  }

  describe("performing a batch mutation") {
    val (client, cf) = setup

    it("performs a batch_mutate") {
      cf.batch(WriteConsistency.All) { batch =>
        batch.insert("key", Column("name", "value", 201))
      }

      val map = ArgumentCaptor.forClass(classOf[java.util.Map[String, java.util.Map[String, java.util.List[Mutation]]]])

      verify(client).batch_mutate(matchEq("ks"), map.capture, matchEq(thrift.ConsistencyLevel.ALL))

      val mutations = map.getValue
      val mutation = mutations.get("key").get("cf").get(0)
      val col = mutation.getColumn_or_supercolumn.getColumn
      new String(col.getName) must equal("name")
      new String(col.getValue) must equal("value")
      col.getTimestamp must equal(201)
    }
  }

  describe("iterating through all columns of all rows") {
    val (client, cf) = setup

    it("returns a ColumnIterator with an all-column predicate") {
      val iterator = cf.iterator(16, ReadConsistency.Quorum).asInstanceOf[ColumnIterator[String, String]]

      iterator.cf must equal(cf)
      iterator.startKey must equal("")
      iterator.endKey must equal("")
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
      val iterator = cf.iterator(16, "name", ReadConsistency.Quorum).asInstanceOf[ColumnIterator[String, String]]

      iterator.cf must equal(cf)
      iterator.startKey must equal("")
      iterator.endKey must equal("")
      iterator.batchSize must equal(16)
      iterator.predicate.getColumn_names.asScala.map { Utf8Codec.decode(_) } must be(List("name"))
    }
  }

  describe("iterating through a set of columns of all rows") {
    val (client, cf) = setup

    it("returns a ColumnIterator with a column-list predicate") {
      val iterator = cf.iterator(16, Set("name", "motto"), ReadConsistency.Quorum).asInstanceOf[ColumnIterator[String, String]]

      iterator.cf must equal(cf)
      iterator.startKey must equal("")
      iterator.endKey must equal("")
      iterator.batchSize must equal(16)
      iterator.predicate.getColumn_names.asScala.map { Utf8Codec.decode(_) }.toSet must be(Set("name", "motto"))
    }
  }
}
