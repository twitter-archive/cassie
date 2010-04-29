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
                                val defaultValueCodec: Codec[Value],
                                val defaultReadConsistency: ReadConsistency,
                                val defaultWriteConsistency: WriteConsistency)
        extends Logging {

  /**
   * Returns the optional value of a given column for a given key as the given
   * types.
   */
  def getColumnAs[A, B](key: String,
                        columnName: A,
                        consistency: ReadConsistency = defaultReadConsistency)
                       (implicit nameCodec: Codec[A], valueCodec: Codec[B]): Option[Column[A, B]] = {
    getColumnsAs(key, Set(columnName), consistency)(nameCodec, valueCodec).get(columnName)
  }

  /**
   * Returns the optional value of a given column for a given key as the default
   * types.
   */
  def getColumn(key: String,
                columnName: Name,
                consistency: ReadConsistency = defaultReadConsistency): Option[Column[Name, Value]] = {
    getColumnAs[Name, Value](key, columnName, consistency)(defaultNameCodec, defaultValueCodec)
  }

  /**
   * Returns a map of all column names to the columns for a given key as the
   * given types. If your rows contain a huge number of columns, this will be
   * slow and horrible.
   */
  def getRowAs[A, B](key: String, consistency: ReadConsistency = defaultReadConsistency)
                    (implicit nameCodec: Codec[A], valueCodec: Codec[B]): Map[A, Column[A, B]] = {
    getRowSliceAs[A, B](key, None, None, Int.MaxValue, Order.Normal, consistency)(nameCodec, valueCodec)
  }

  /**
   * Returns a map of all column names to the columns for a given key as the
   * default types. If your rows contain a huge number of columns, this will be
   * slow and horrible.
   */
  def getRow(key: String, consistency: ReadConsistency  = defaultReadConsistency): Map[Name, Column[Name, Value]] = {
    getRowAs[Name, Value](key, consistency)(defaultNameCodec, defaultValueCodec)
  }

  /**
   * Returns a slice of all columns of a row as the given types.
   */
  def getRowSliceAs[A, B](key: String,
                          startColumnName: Option[A],
                          endColumnName: Option[A],
                          count: Int,
                          order: Order,
                          consistency: ReadConsistency = defaultReadConsistency)
                         (implicit nameCodec: Codec[A], valueCodec: Codec[B]): Map[A, Column[A, B]] = {
    val startBytes = startColumnName.map { c => nameCodec.encode(c) }.getOrElse(Array[Byte]())
    val endBytes = endColumnName.map { c => nameCodec.encode(c) }.getOrElse(Array[Byte]())
    val pred = new thrift.SlicePredicate()
    pred.setSlice_range(new thrift.SliceRange(startBytes, endBytes, order.reversed, count))
    getSlice(key, pred, consistency, nameCodec, valueCodec)
  }

  /**
   * Returns a slice of all columns of a row as the default types.
   */
  def getRowSlice(key: String,
                  startColumnName: Option[Name],
                  endColumnName: Option[Name],
                  count: Int,
                  order: Order,
                  consistency: ReadConsistency = defaultReadConsistency): Map[Name, Column[Name, Value]] = {
    getRowSliceAs[Name, Value](key, startColumnName, endColumnName, count, order, consistency)(defaultNameCodec, defaultValueCodec)
  }

  /**
   * Returns a map of the given column names to the columns for a given key as
   * the given types.
   */
  def getColumnsAs[A, B](key: String,
                         columnNames: Set[A],
                         consistency: ReadConsistency = defaultReadConsistency)
                        (implicit nameCodec: Codec[A], valueCodec: Codec[B]): Map[A, Column[A, B]] = {
    val pred = new thrift.SlicePredicate()
    pred.setColumn_names(columnNames.toList.map { nameCodec.encode(_) }.asJava)
    getSlice(key, pred, consistency, nameCodec, valueCodec)
  }

  /**
   * Returns a map of the given column names to the columns for a given key as
   * the default types.
   */
  def getColumns(key: String,
                 columnNames: Set[Name],
                 consistency: ReadConsistency = defaultReadConsistency): Map[Name, Column[Name, Value]] = {
    getColumnsAs[Name, Value](key, columnNames, consistency)(defaultNameCodec, defaultValueCodec)
  }


  /**
   * Returns a map of keys to given column for a set of keys as the given types.
   */
  def multigetColumnAs[A, B](keys: Set[String],
                             columnName: A,
                             consistency: ReadConsistency = defaultReadConsistency)
                            (implicit nameCodec: Codec[A], valueCodec: Codec[B]): Map[String, Column[A, B]] = {
    multigetColumnsAs[A, B](keys, Set(columnName), consistency)(nameCodec, valueCodec).map {
      case (k, v) => (k, v.valuesIterator.next)
    }
  }

  /**
   * Returns a map of keys to given column for a set of keys as the default
   * types.
   */
  def multigetColumn(keys: Set[String],
                     columnName: Name,
                     consistency: ReadConsistency = defaultReadConsistency): Map[String, Column[Name, Value]] = {
    multigetColumnAs[Name, Value](keys, columnName, consistency)(defaultNameCodec, defaultValueCodec)
  }

  /**
   * Returns a map of keys to a map of column names to the columns for a given
   * set of keys and columns as the given types.
   */
  def multigetColumnsAs[A, B](keys: Set[String],
                              columnNames: Set[A],
                              consistency: ReadConsistency = defaultReadConsistency)
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
  def multigetColumns(keys: Set[String],
                      columnNames: Set[Name],
                      consistency: ReadConsistency = defaultReadConsistency): Map[String, Map[Name, Column[Name, Value]]] = {
    multigetColumnsAs[Name, Value](keys, columnNames, consistency)(defaultNameCodec, defaultValueCodec)
  }

  /**
   * Inserts a column.
   */
  def insert[A, B](key: String,
                   column: Column[A, B],
                   consistency: WriteConsistency = defaultWriteConsistency)
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
  def removeColumn[A](key: String,
                      columnName: A,
                      consistency: WriteConsistency = defaultWriteConsistency)
                     (implicit clock: Clock, nameCodec: Codec[A]) {
    removeColumnWithTimestamp(key, columnName, clock.timestamp, consistency)(nameCodec)
  }

  /**
   * Removes a column from a key with a specific timestamp.
   */
  def removeColumnWithTimestamp[A](key: String,
                                   columnName: A,
                                   timestamp: Long,
                                   consistency: WriteConsistency = defaultWriteConsistency)
                                  (implicit nameCodec: Codec[A]) {
    val cp = new thrift.ColumnPath(name)
    cp.setColumn(nameCodec.encode(columnName))
    log.fine("remove(%s, %s, %s, %d, %s)", keyspace, key, cp, timestamp, consistency.level)
    provider.map { _.remove(keyspace, key, cp, timestamp, consistency.level) }
  }

  /**
   * Removes a set of columns from a key.
   */
  def removeColumns[A](key: String,
                       columnNames: Set[A],
                       consistency: WriteConsistency = defaultWriteConsistency)
                      (implicit clock: Clock, nameCodec: Codec[A]) {
    removeColumnsWithTimestamp(key, columnNames, clock.timestamp, consistency)(nameCodec)
  }

  /**
   * Removes a set of columns from a key with a specific timestamp.
   */
  def removeColumnsWithTimestamp[A](key: String,
                                    columnNames: Set[A],
                                    timestamp: Long,
                                    consistency: WriteConsistency = defaultWriteConsistency)
                                   (implicit nameCodec: Codec[A]) {
    batch(consistency) { cf =>
      cf.removeColumnsWithTimestamp(key, columnNames, timestamp)(nameCodec)
    }
  }

  /**
   * Removes a key.
   */
  def removeRow(key: String,
                consistency: WriteConsistency = defaultWriteConsistency)
               (implicit clock: Clock) {
    removeRowWithTimestamp(key, clock.timestamp, consistency)
  }

  /**
   * Removes a key with a specific timestamp.
   */
  def removeRowWithTimestamp(key: String,
                             timestamp: Long,
                             consistency: WriteConsistency = defaultWriteConsistency) {
    val cp = new thrift.ColumnPath(name)
    log.fine("remove(%s, %s, %s, %d, %s)", keyspace, key, cp, timestamp, consistency.level)
    provider.map { _.remove(keyspace, key, cp, timestamp, consistency.level) }
  }

  /**
   * Performs a series of actions in a single request.
   */
  def batch(consistency: WriteConsistency = defaultWriteConsistency)
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
  def rowIteratorAs[A, B](batchSize: Int,
                          consistency: ReadConsistency = defaultReadConsistency)
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
  def rowIterator(batchSize: Int,
                  consistency: ReadConsistency = defaultReadConsistency): Iterator[(String, Column[Name, Value])] = {
    rowIteratorAs[Name, Value](batchSize, consistency)(defaultNameCodec, defaultValueCodec)
  }


  /**
   * Returns a column iterator which iterates over the given column of all rows
   * in the column family with the given batch size and consistency level as the
   * given types.
   */
  def columnIteratorAs[A, B](batchSize: Int,
                             columnName: A,
                             consistency: ReadConsistency = defaultReadConsistency)
                            (implicit nameCodec: Codec[A], valueCodec: Codec[B]): Iterator[(String, Column[A, B])] =
    columnsIteratorAs(batchSize, Set(columnName), consistency)(nameCodec, valueCodec)

  /**
   * Returns a column iterator which iterates over the given column of all rows
   * in the column family with the given batch size and consistency level as the
   * default types.
   */
  def columnIterator(batchSize: Int,
                     columnName: Name,
                     consistency: ReadConsistency = defaultReadConsistency): Iterator[(String, Column[Name, Value])] =
    columnIteratorAs[Name, Value](batchSize, columnName, consistency)(defaultNameCodec, defaultValueCodec)

  /**
   * Returns a column iterator which iterates over the given columns of all rows
   * in the column family with the given batch size and consistency level as the
   * given types.
   */
  def columnsIteratorAs[A, B](batchSize: Int,
                              columnNames: Set[A],
                              consistency: ReadConsistency = defaultReadConsistency)
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
  def columnsIterator(batchSize: Int,
                      columnNames: Set[Name],
                      consistency: ReadConsistency = defaultReadConsistency): Iterator[(String, Column[Name, Value])] = {
    columnsIteratorAs[Name, Value](batchSize, columnNames, consistency)(defaultNameCodec, defaultValueCodec)
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
