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
                                val nameCodec: Codec[Name],
                                val valueCodec: Codec[Value]) extends Logging {

  // TODO: refactor the hell out of the way I'm interacting with Thrift here.
  // The maid needs a maid, man.

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
  def get(key: String,
          consistency: ReadConsistency): Map[Name, Column[Name, Value]] = {
    val pred = new thrift.SlicePredicate()
    pred.setSlice_range(new thrift.SliceRange(Array(), Array(), false, Int.MaxValue))
    getSlice(key, pred, consistency)
  }

  /**
   * Returns a map of the given column names to the columns for a given key.
   */
  def get(key: String,
          columnNames: Set[Name],
          consistency: ReadConsistency): Map[Name, Column[Name, Value]] = {
    val pred = new thrift.SlicePredicate()
    pred.setColumn_names(columnNames.toList.map { nameCodec.encode(_) }.asJava)
    getSlice(key, pred, consistency)

  }

  /**
   * Returns a map of keys to given column for a set of keys.
   */
  def multiget(keys: Set[String],
               columnName: Name,
               consistency: ReadConsistency): Map[String, Column[Name, Value]] = {
    multiget(keys, Set(columnName), consistency).map {
      case (k, v) => (k, v.valuesIterator.next)
    }
  }

  /**
   * Returns a map of keys to a map of column names to the columns for a given
   * set of keys and columns.
   */
  def multiget(keys: Set[String],
               columnNames: Set[Name],
               consistency: ReadConsistency): Map[String, Map[Name, Column[Name, Value]]] = {
    val cp = new thrift.ColumnParent(name)
    val pred = new thrift.SlicePredicate()
    pred.setColumn_names(columnNames.toList.map { nameCodec.encode(_) }.asJava)
    log.fine("multiget_slice(%s, %s, %s, %s, %s)", keyspace, keys, cp, pred, consistency.level)
    val result = provider.map {
      _.multiget_slice(keyspace, keys.toList.asJava, cp, pred, consistency.level).asScala
    }
    return result.map {
      case (k, v) => (k, v.asScala.map { r => convert(r).pair }.toMap)
    }.toMap
  }

  /**
   * Inserts a column.
   */
  def insert(key: String,
             column: Column[Name, Value],
             consistency: WriteConsistency) {
    val cp = new thrift.ColumnPath(name)
    cp.setColumn(nameCodec.encode(column.name))
    log.fine("insert(%s, %s, %s, %s, %d, %s)", keyspace, key, cp, column.value,
      column.timestamp, consistency.level)
    provider.map {
      _.insert(keyspace, key, cp, valueCodec.encode(column.value), column.timestamp, consistency.level)
    }
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
  def remove(key: String,
             columnName: Name,
             timestamp: Long,
             consistency: WriteConsistency) {
    val cp = new thrift.ColumnPath(name)
    cp.setColumn(nameCodec.encode(columnName))
    log.fine("remove(%s, %s, %s, %d, %s)", keyspace, key, cp, timestamp, consistency.level)
    provider.map { _.remove(keyspace, key, cp, timestamp, consistency.level) }
  }

  /**
   * Removes a set of columns from a key.
   */
  def remove(key: String,
             columnNames: Set[Name],
             consistency: WriteConsistency)
            (implicit clock: Clock) {
    remove(key, columnNames, clock.timestamp, consistency)
  }

  /**
   * Removes a set of columns from a key with a specific timestamp.
   */
  def remove(key: String,
             columnNames: Set[Name],
             timestamp: Long,
             consistency: WriteConsistency) {
    batch(consistency) { cf =>
      cf.remove(key, columnNames, timestamp)
    }
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
  def remove(key: String,
             timestamp: Long,
             consistency: WriteConsistency) {
    val cp = new thrift.ColumnPath(name)
    log.fine("remove(%s, %s, %s, %d, %s)", keyspace, key, cp, timestamp, consistency.level)
    provider.map { _.remove(keyspace, key, cp, timestamp, consistency.level) }
  }

  /**
   * Performs a series of actions in a single request.
   */
  def batch(consistency: WriteConsistency)
           (mutation: BatchMutationBuilder[Name, Value] => Unit) {
    val builder = new BatchMutationBuilder(this)
    mutation(builder)
    val mutations = builder.mutations
    log.fine("batch_mutate(%s, %s, %s", keyspace, mutations, consistency.level)
    provider.map { _.batch_mutate(keyspace, mutations, consistency.level) }
  }

  /**
   * Returns a column iterator which iterates over all columns of all rows in
   * the column family with the given batch size and consistency level.
   */
  def iterator(batchSize: Int,
               consistency: ReadConsistency): Iterator[(String, Column[Name, Value])] = {
    val pred = new thrift.SlicePredicate
    pred.setSlice_range(new thrift.SliceRange(Array(), Array(), false, Int.MaxValue))
    new ColumnIterator(this, "", "", batchSize, pred, consistency)
  }

  /**
   * Returns a column iterator which iterates over the given column of all rows
   * in the column family with the given batch size and consistency level.
   */
  def iterator(batchSize: Int,
               columnName: Name,
               consistency: ReadConsistency): Iterator[(String, Column[Name, Value])] =
    iterator(batchSize, Set(columnName), consistency)

  /**
   * Returns a column iterator which iterates over the given columns of all rows
   * in the column family with the given batch size and consistency level.
   */
  def iterator(batchSize: Int,
               columnNames: Set[Name],
               consistency: ReadConsistency): Iterator[(String, Column[Name, Value])] = {
    val pred = new thrift.SlicePredicate
    pred.setColumn_names(columnNames.toList.map { nameCodec.encode(_) }.asJava)
    new ColumnIterator(this, "", "", batchSize, pred, consistency)
  }

  private def getSlice(key: String,
                       pred: thrift.SlicePredicate,
                       consistency: ReadConsistency) = {
    val cp = new thrift.ColumnParent(name)
    log.fine("get_slice(%s, %s, %s, %s, %s)", keyspace, key, cp, pred, consistency.level)
    val result = provider.map { _.get_slice(keyspace, key, cp, pred, consistency.level) }
    result.asScala.map { r => convert(r).pair }.toMap
  }

  private[cassie] def getRangeSlice(startKey: String,
                                    endKey: String,
                                    count: Int,
                                    predicate: thrift.SlicePredicate,
                                    consistency: ReadConsistency) = {
    val cp = new thrift.ColumnParent(name)
    val range = new thrift.KeyRange(count)
    range.setStart_key(startKey)
    range.setEnd_key(endKey)
    log.fine("get_range_slices(%s, %s, %s, %s, %s)", keyspace, cp, predicate, range, consistency.level)
    provider.map { _.get_range_slices(keyspace, cp, predicate, range, consistency.level) }.asScala
  }

  private[cassie] def convert(colOrSCol: thrift.ColumnOrSuperColumn): Column[Name, Value] = {
    Column(
      nameCodec.decode(colOrSCol.column.name),
      valueCodec.decode(colOrSCol.column.value),
      colOrSCol.column.timestamp
    )
  }
}
