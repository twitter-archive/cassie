package com.codahale.cassie

import client.ClientProvider
import clocks.Clock
import codecs.Codec
import scalaj.collection.Imports._
import org.apache.cassandra.thrift
import com.codahale.logula.Logging

/**
 * A readable, writable column family with batching capabilities.
 *
 * @author coda
 */
class ColumnFamily[Name, Value](val keyspace: String,
                                val name: String,
                                val provider: ClientProvider,
                                val columnCodec: Codec[Name],
                                val valueCodec: Codec[Value]) extends Logging {

  /**
   * Returns the optional value of a given column for a given key.
   */
  def get(key: String,
          columnName: Name,
          consistency: ReadConsistency): Option[Column[Name, Value]] = {
    get(key, Set(columnName), consistency).get(columnName)
  }

  /**
   * Returns a map of all column names to the columns for a given key. If your
   * rows contain a huge number of columns, this will be slow and horrible.
   */
  def get(key: String, consistency: ReadConsistency): Map[Name, Column[Name, Value]] = {
    val cp = new thrift.ColumnParent(name)
    val pred = new thrift.SlicePredicate()
    pred.setSlice_range(new thrift.SliceRange(Array(), Array(), false, Int.MaxValue))
    log.fine("get_slice(%s, %s, %s, %s, %s)", keyspace, key, cp, pred, consistency.level)
    val result = provider.map { _.get_slice(keyspace, key, cp, pred, consistency.level) }
    result.asScala.map { r => val x = convert(r); (x.name, x) }.toMap
  }

  /**
   * Returns a map of the given column names to the columns for a given key.
   */
  def get(key: String, columnNames: Set[Name], consistency: ReadConsistency): Map[Name, Column[Name, Value]] = {
    val cp = new thrift.ColumnParent(name)
    val pred = new thrift.SlicePredicate()
    pred.setColumn_names(columnNames.toList.map { columnCodec.encode(_) }.asJava)
    log.fine("get_slice(%s, %s, %s, %s, %s)", keyspace, key, cp, pred, consistency.level)
    val result = provider.map { _.get_slice(keyspace, key, cp, pred, consistency.level) }
    result.asScala.map { r => val x = convert(r); (x.name, x) }.toMap
  }

  /**
   * Returns a map of keys to given column for a set of keys.
   */
  def multiget(keys: Set[String],
               columnName: Name,
               consistency: ReadConsistency): Map[String, Column[Name, Value]] = {
    multiget(keys, Set(columnName), consistency).map { case (k, v) => (k, v.valuesIterator.next) }
  }

  /**
   * Returns a map of keys to a map of column names to the columns for a given
   * set of keys and columns.
   */
  def multiget(keys: Set[String], columnNames: Set[Name], consistency: ReadConsistency): Map[String, Map[Name, Column[Name, Value]]] = {
    val cp = new thrift.ColumnParent(name)
    val pred = new thrift.SlicePredicate()
    pred.setColumn_names(columnNames.toList.map { columnCodec.encode(_) }.asJava)
    log.fine("multiget_slice(%s, %s, %s, %s, %s)", keyspace, keys, cp, pred, consistency.level)
    val result = provider.map { _.multiget_slice(keyspace, keys.toList.asJava, cp, pred, consistency.level).asScala }
    return result.map { case (k, v) => (k, v.asScala.map { r => val x = convert(r); (x.name, x) }.toMap) }.toMap
  }

  /**
   * Inserts a column.
   */
  def insert(key: String, column: Column[Name, Value], consistency: WriteConsistency) {
    val cp = new thrift.ColumnPath(name)
    cp.setColumn(columnCodec.encode(column.name))
    log.fine("insert(%s, %s, %s, %s, %d, %s)", keyspace, key, cp, column.value, column.timestamp, consistency.level)
    provider.map { _.insert(keyspace, key, cp, valueCodec.encode(column.value), column.timestamp, consistency.level) }
  }

  /**
   * Removes a column from a key.
   */
  def remove(key: String,
             columnName: Name,
             consistency: WriteConsistency)
            (implicit clock: Clock) {
    remove(key, columnName, clock.timestamp, consistency)
  }

  /**
   * Removes a column from a key with a specific timestamp.
   */
  def remove(key: String, columnName: Name, timestamp: Long, consistency: WriteConsistency) {
    val cp = new thrift.ColumnPath(name)
    cp.setColumn(columnCodec.encode(columnName))
    log.fine("remove(%s, %s, %s, %d, %s)", keyspace, key, cp, timestamp, consistency.level)
    provider.map { _.remove(keyspace, key, cp, timestamp, consistency.level) }
  }

  /**
   * Removes a key.
   */
  def remove(key: String,
             consistency: WriteConsistency)
            (implicit clock: Clock) {
    remove(key, clock.timestamp, consistency)
  }

  /**
   * Removes a key with a specific timestamp.
   */
  def remove(key: String, timestamp: Long, consistency: WriteConsistency) {
    val cp = new thrift.ColumnPath(name)
    log.fine("remove(%s, %s, %s, %d, %s)", keyspace, key, cp, timestamp, consistency.level)
    provider.map { _.remove(keyspace, key, cp, timestamp, consistency.level) }
  }

  private def convert(colOrSCol: thrift.ColumnOrSuperColumn): Column[Name, Value] = {
    Column(
      columnCodec.decode(colOrSCol.column.name),
      valueCodec.decode(colOrSCol.column.value),
      colOrSCol.column.timestamp
    )
  }
}
