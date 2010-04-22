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
import com.codahale.cassie.{WriteConsistency, Column, ReadConsistency, ColumnFamilyImpl}

class ColumnFamilyImplTest extends Spec with MustMatchers with MockitoSugar {
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
    val cf = new ColumnFamilyImpl("ks", "cf", provider, Utf8Codec, Utf8Codec)

    (client, cf)
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

  describe("removing a column") {
    val (client, cf) = setup

    it("performs a remove") {
      cf.remove("key", "age", 55, WriteConsistency.Quorum)

      val cp = ArgumentCaptor.forClass(classOf[thrift.ColumnPath])
      verify(client).remove(matchEq("ks"), matchEq("key"), cp.capture, matchEq(55), matchEq(thrift.ConsistencyLevel.QUORUM))

      cp.getValue.getColumn_family must equal("cf")
      new String(cp.getValue.getColumn) must equal("age")
    }
  }

  describe("removing a row") {
    val (client, cf) = setup

    it("performs a remove") {
      cf.remove("key", 55, WriteConsistency.Quorum)

      val cp = ArgumentCaptor.forClass(classOf[thrift.ColumnPath])
      verify(client).remove(matchEq("ks"), matchEq("key"), cp.capture, matchEq(55), matchEq(thrift.ConsistencyLevel.QUORUM))

      cp.getValue.getColumn_family must equal("cf")
      cp.getValue.getColumn must be(null)
    }
  }
}
