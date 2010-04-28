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
                   val defaultNameCodec: Codec[Name],
                   val defaultValueCodec: Codec[Value]) extends Logging {

  /**
   * Returns the optional value of a given column for a given key as the given
   * types.
   */
  def getAs[A, B](key: String,
                  columnName: A,
                  consistency: ReadConsistency)
                 (implicit nameCodec: Codec[A], valueCodec: Codec[B]): Option[Column[A, B]] = {
    getAs(key, Set(columnName), consistency)(nameCodec, valueCodec).get(columnName)
  }

  /**
   * Returns the optional value of a given column for a given key as the default
   * types.
   */
  def get(key: String,
          columnName: Name,
          consistency: ReadConsistency): Option[Column[Name, Value]] = {
    getAs[Name, Value](key, columnName, consistency)(defaultNameCodec, defaultValueCodec)
  }

  /**
   * Returns a map of all column names to the columns for a given key as the
   * given types. If your rows contain a huge number of columns, this will be
   * slow and horrible.
   */
  def getAs[A, B](key: String,
                  consistency: ReadConsistency)
                 (implicit nameCodec: Codec[A], valueCodec: Codec[B]): Map[A, Column[A, B]] = {
    val pred = new thrift.SlicePredicate()
    pred.setSlice_range(new thrift.SliceRange(Array(), Array(), false, Int.MaxValue))
    getSlice(key, pred, consistency, nameCodec, valueCodec)
  }

  /**
   * Returns a map of all column names to the columns for a given key as the
   * default types. If your rows contain a huge number of columns, this will be
   * slow and horrible.
   */
  def get(key: String,
          consistency: ReadConsistency): Map[Name, Column[Name, Value]] = {
    getAs[Name, Value](key, consistency)(defaultNameCodec, defaultValueCodec)
  }

  /**
   * Returns a map of the given column names to the columns for a given key as
   * the given types.
   */
  def getAs[A, B](key: String,
                  columnNames: Set[A],
                  consistency: ReadConsistency)
                 (implicit nameCodec: Codec[A], valueCodec: Codec[B]): Map[A, Column[A, B]] = {
    val pred = new thrift.SlicePredicate()
    pred.setColumn_names(columnNames.toList.map { nameCodec.encode(_) }.asJava)
    getSlice(key, pred, consistency, nameCodec, valueCodec)
  }

  /**
   * Returns a map of the given column names to the columns for a given key as
   * the default types.
   */
  def get(key: String,
          columnNames: Set[Name],
          consistency: ReadConsistency): Map[Name, Column[Name, Value]] = {
    getAs[Name, Value](key, columnNames, consistency)(defaultNameCodec, defaultValueCodec)
  }


  /**
   * Returns a map of keys to given column for a set of keys as the given types.
   */
  def multigetAs[A, B](keys: Set[String],
                       columnName: A,
                       consistency: ReadConsistency)
                      (implicit nameCodec: Codec[A], valueCodec: Codec[B]): Map[String, Column[A, B]] = {
    multigetAs[A, B](keys, Set(columnName), consistency)(nameCodec, valueCodec).map {
      case (k, v) => (k, v.valuesIterator.next)
    }
  }

  /**
   * Returns a map of keys to given column for a set of keys as the default
   * types.
   */
  def multiget(keys: Set[String],
               columnName: Name,
               consistency: ReadConsistency): Map[String, Column[Name, Value]] = {
    multigetAs[Name, Value](keys, columnName, consistency)(defaultNameCodec, defaultValueCodec)
  }

  /**
   * Returns a map of keys to a map of column names to the columns for a given
   * set of keys and columns as the given types.
   */
  def multigetAs[A, B](keys: Set[String],
                       columnNames: Set[A],
                       consistency: ReadConsistency)
                      (implicit nameCodec: Codec[A], valueCodec: Codec[B]): Map[String, Map[A, Column[A, B]]] = {
    val cp = new thrift.ColumnParent(name)
    val pred = new thrift.SlicePredicate()
    pred.setColumn_names(columnNames.toList.map { nameCodec.encode(_) }.asJava)
    log.fine("multiget_slice(%s, %s, %s, %s, %s)", keyspace, keys, cp, pred, consistency.level)
    val result = provider.map {
      _.multiget_slice(keyspace, keys.toList.asJava, cp, pred, consistency.level).asScala
    }
    return result.map {
      case (k, v) => (k, v.asScala.map { r => convert(nameCodec, valueCodec, r).pair }.toMap)
    }.toMap
  }

  /**
   * Returns a map of keys to a map of column names to the columns for a given
   * set of keys and columns as the default types.
   */
  def multiget(keys: Set[String],
               columnNames: Set[Name],
               consistency: ReadConsistency): Map[String, Map[Name, Column[Name, Value]]] = {
    multigetAs[Name, Value](keys, columnNames, consistency)(defaultNameCodec, defaultValueCodec)
  }

  /**
   * Inserts a column.
   */
  def insert[A, B](key: String,
                   column: Column[A, B],
                   consistency: WriteConsistency)
                  (implicit nameCodec: Codec[A], valueCodec: Codec[B]) {
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
  def remove[A](key: String,
                columnName: A,
                consistency: WriteConsistency)
               (implicit clock: Clock, nameCodec: Codec[A]) {
    remove(key, columnName, clock.timestamp, consistency)(nameCodec)
  }

  /**
   * Removes a column from a key with a specific timestamp.
   */
  def remove[A](key: String,
                columnName: A,
                timestamp: Long,
                consistency: WriteConsistency)(implicit nameCodec: Codec[A]) {
    val cp = new thrift.ColumnPath(name)
    cp.setColumn(nameCodec.encode(columnName))
    log.fine("remove(%s, %s, %s, %d, %s)", keyspace, key, cp, timestamp, consistency.level)
    provider.map { _.remove(keyspace, key, cp, timestamp, consistency.level) }
  }

  /**
   * Removes a set of columns from a key.
   */
  def remove[A](key: String,
                columnNames: Set[A],
                consistency: WriteConsistency)
               (implicit clock: Clock, nameCodec: Codec[A]) {
    remove(key, columnNames, clock.timestamp, consistency)(nameCodec)
  }

  /**
   * Removes a set of columns from a key with a specific timestamp.
   */
  def remove[A](key: String,
                columnNames: Set[A],
                timestamp: Long,
                consistency: WriteConsistency)
               (implicit nameCodec: Codec[A]) {
    batch(consistency) { cf =>
      cf.remove(key, columnNames, timestamp)(nameCodec)
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
           (build: BatchMutationBuilder => Unit) {
    val builder = new BatchMutationBuilder(name)
    build(builder)
    val mutations = builder.mutations
    log.fine("batch_mutate(%s, %s, %s", keyspace, mutations, consistency.level)
    provider.map { _.batch_mutate(keyspace, mutations, consistency.level) }
  }

  /**
   * Returns a column iterator which iterates over all columns of all rows in
   * the column family with the given batch size and consistency level as the
   * given types.
   */
  def iteratorAs[A, B](batchSize: Int,
                       consistency: ReadConsistency)
                      (implicit nameCodec: Codec[A], valueCodec: Codec[B]): Iterator[(String, Column[A, B])] = {
    val pred = new thrift.SlicePredicate
    pred.setSlice_range(new thrift.SliceRange(Array(), Array(), false, Int.MaxValue))
    new ColumnIterator(this, "", "", batchSize, pred, consistency, nameCodec, valueCodec)
  }

  /**
   * Returns a column iterator which iterates over all columns of all rows in
   * the column family with the given batch size and consistency level as the
   * default types.
   */
  def iterator(batchSize: Int,
               consistency: ReadConsistency): Iterator[(String, Column[Name, Value])] = {
    iteratorAs[Name, Value](batchSize, consistency)(defaultNameCodec, defaultValueCodec)
  }


  /**
   * Returns a column iterator which iterates over the given column of all rows
   * in the column family with the given batch size and consistency level as the
   * given types.
   */
  def iteratorAs[A, B](batchSize: Int,
                       columnName: A,
                       consistency: ReadConsistency)
                      (implicit nameCodec: Codec[A], valueCodec: Codec[B]): Iterator[(String, Column[A, B])] =
    iteratorAs(batchSize, Set(columnName), consistency)(nameCodec, valueCodec)

  /**
   * Returns a column iterator which iterates over the given column of all rows
   * in the column family with the given batch size and consistency level as the
   * default types.
   */
  def iterator(batchSize: Int,
               columnName: Name,
               consistency: ReadConsistency): Iterator[(String, Column[Name, Value])] =
    iteratorAs[Name, Value](batchSize, columnName, consistency)(defaultNameCodec, defaultValueCodec)

  /**
   * Returns a column iterator which iterates over the given columns of all rows
   * in the column family with the given batch size and consistency level as the
   * given types.
   */
  def iteratorAs[A, B](batchSize: Int,
                       columnNames: Set[A],
                       consistency: ReadConsistency)
                      (implicit nameCodec: Codec[A], valueCodec: Codec[B]): Iterator[(String, Column[A, B])] = {
    val pred = new thrift.SlicePredicate
    pred.setColumn_names(columnNames.toList.map { nameCodec.encode(_) }.asJava)
    new ColumnIterator(this, "", "", batchSize, pred, consistency, nameCodec, valueCodec)
  }

  /**
   * Returns a column iterator which iterates over the given columns of all rows
   * in the column family with the given batch size and consistency level as the
   * default types.
   */
  def iterator(batchSize: Int,
               columnNames: Set[Name],
               consistency: ReadConsistency): Iterator[(String, Column[Name, Value])] = {
    iteratorAs[Name, Value](batchSize, columnNames, consistency)(defaultNameCodec, defaultValueCodec)
  }

  private def getSlice[A, B](key: String,
                       pred: thrift.SlicePredicate,
                       consistency: ReadConsistency, nameCodec: Codec[A], valueCodec: Codec[B]) = {
    val cp = new thrift.ColumnParent(name)
    log.fine("get_slice(%s, %s, %s, %s, %s)", keyspace, key, cp, pred, consistency.level)
    val result = provider.map { _.get_slice(keyspace, key, cp, pred, consistency.level) }
    result.asScala.map { r => convert(nameCodec, valueCodec, r).pair }.toMap
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

  private def convert[A, B](nameCodec: Codec[A], valueCodec: Codec[B], colOrSCol: thrift.ColumnOrSuperColumn): Column[A, B] = {
    Column(
      nameCodec.decode(colOrSCol.column.name),
      valueCodec.decode(colOrSCol.column.value),
      colOrSCol.column.timestamp
    )
  }
}
