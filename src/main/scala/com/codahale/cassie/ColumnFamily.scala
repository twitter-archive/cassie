package com.codahale.cassie

import clocks.Clock
import codecs.{Codec, Utf8Codec}
import connection.ClientProvider
import scalaj.collection.Imports._
import org.apache.cassandra.thrift
import com.codahale.logula.Logging
import java.nio.ByteBuffer

/**
 * A readable, writable column family with batching capabilities.
 *
 * @author coda
 */
class ColumnFamily[Key, Name, Value](val keyspace: String,
                                     val name: String,
                                     val provider: ClientProvider,
                                     val defaultKeyCodec: Codec[Key],
                                     val defaultNameCodec: Codec[Name],
                                     val defaultValueCodec: Codec[Value],
                                     val defaultReadConsistency: ReadConsistency,
                                     val defaultWriteConsistency: WriteConsistency)
        extends Logging {

  val EMPTY = ByteBuffer.allocate(0)

  /**
   * Returns the optional value of a given column for a given key as the given
   * types.
   */
  def getColumnAs[K, N, V](key: K,
                           columnName: N,
                           consistency: ReadConsistency = defaultReadConsistency)
                          (implicit keyCodec: Codec[K], nameCodec: Codec[N], valueCodec: Codec[V]): Option[Column[N, V]] = {
    getColumnsAs(key, Set(columnName), consistency)(keyCodec, nameCodec, valueCodec).get(columnName)
  }

  /**
   * Returns the optional value of a given column for a given key as the default
   * types.
   */
  def getColumn(key: Key,
                columnName: Name,
                consistency: ReadConsistency = defaultReadConsistency): Option[Column[Name, Value]] = {
    getColumnAs[Key, Name, Value](key, columnName, consistency)(defaultKeyCodec, defaultNameCodec, defaultValueCodec)
  }

  /**
   * Returns a map of all column names to the columns for a given key as the
   * given types. If your rows contain a huge number of columns, this will be
   * slow and horrible.
   */
  def getRowAs[K, N, V](key: K, consistency: ReadConsistency = defaultReadConsistency)
                    (implicit keyCodec: Codec[K], nameCodec: Codec[N], valueCodec: Codec[V]): Map[N, Column[N, V]] = {
    getRowSliceAs[K, N, V](key, None, None, Int.MaxValue, Order.Normal, consistency)(keyCodec, nameCodec, valueCodec)
  }

  /**
   * Returns a map of all column names to the columns for a given key as the
   * default types. If your rows contain a huge number of columns, this will be
   * slow and horrible.
   */
  def getRow(key: Key, consistency: ReadConsistency  = defaultReadConsistency): Map[Name, Column[Name, Value]] = {
    getRowAs[Key, Name, Value](key, consistency)(defaultKeyCodec, defaultNameCodec, defaultValueCodec)
  }

  /**
   * Returns a slice of all columns of a row as the given types.
   */
  def getRowSliceAs[K, N, V](key: K,
                             startColumnName: Option[N],
                             endColumnName: Option[N],
                             count: Int,
                             order: Order,
                             consistency: ReadConsistency = defaultReadConsistency)
                            (implicit keyCodec: Codec[K], nameCodec: Codec[N], valueCodec: Codec[V]): Map[N, Column[N, V]] = {
    val startBytes = startColumnName.map { c => nameCodec.encode(c) }.getOrElse(EMPTY)
    val endBytes = endColumnName.map { c => nameCodec.encode(c) }.getOrElse(EMPTY)
    val pred = new thrift.SlicePredicate()
    pred.setSlice_range(new thrift.SliceRange(startBytes, endBytes, order.reversed, count))
    getSlice(key, pred, consistency, keyCodec, nameCodec, valueCodec)
  }

  /**
   * Returns a slice of all columns of a row as the default types.
   */
  def getRowSlice(key: Key,
                  startColumnName: Option[Name],
                  endColumnName: Option[Name],
                  count: Int,
                  order: Order,
                  consistency: ReadConsistency = defaultReadConsistency): Map[Name, Column[Name, Value]] = {
    getRowSliceAs[Key, Name, Value](key, startColumnName, endColumnName, count, order, consistency)(defaultKeyCodec, defaultNameCodec, defaultValueCodec)
  }

  /**
   * Returns a map of the given column names to the columns for a given key as
   * the given types.
   */
  def getColumnsAs[K, N, V](key: K,
                            columnNames: Set[N],
                            consistency: ReadConsistency = defaultReadConsistency)
                           (implicit keyCodec: Codec[K], nameCodec: Codec[N], valueCodec: Codec[V]): Map[N, Column[N, V]] = {
    val pred = new thrift.SlicePredicate()
    pred.setColumn_names(columnNames.toList.map { nameCodec.encode(_) }.asJava)
    getSlice(key, pred, consistency, keyCodec, nameCodec, valueCodec)
  }

  /**
   * Returns a map of the given column names to the columns for a given key as
   * the default types.
   */
  def getColumns(key: Key,
                 columnNames: Set[Name],
                 consistency: ReadConsistency = defaultReadConsistency): Map[Name, Column[Name, Value]] = {
    getColumnsAs[Key, Name, Value](key, columnNames, consistency)(defaultKeyCodec, defaultNameCodec, defaultValueCodec)
  }


  /**
   * Returns a map of keys to given column for a set of keys as the given types.
   */
  def multigetColumnAs[K, N, V](keys: Set[K],
                                columnName: N,
                                consistency: ReadConsistency = defaultReadConsistency)
                               (implicit keyCodec: Codec[K], nameCodec: Codec[N], valueCodec: Codec[V]): Map[K, Column[N, V]] = {
    multigetColumnsAs[K, N, V](keys, Set(columnName), consistency)(keyCodec, nameCodec, valueCodec).flatMap { case (k, m) =>
      m.get(columnName).map { v => (k, v) }
    }
  }

  /**
   * Returns a map of keys to given column for a set of keys as the default
   * types.
   */
  def multigetColumn(keys: Set[Key],
                     columnName: Name,
                     consistency: ReadConsistency = defaultReadConsistency): Map[Key, Column[Name, Value]] = {
    multigetColumnAs[Key, Name, Value](keys, columnName, consistency)(defaultKeyCodec, defaultNameCodec, defaultValueCodec)
  }

  /**
   * Returns a map of keys to a map of column names to the columns for a given
   * set of keys and columns as the given types.
   */
  def multigetColumnsAs[K, N, V](keys: Set[K],
                              columnNames: Set[N],
                              consistency: ReadConsistency = defaultReadConsistency)
                             (implicit keyCodec: Codec[K], nameCodec: Codec[N], valueCodec: Codec[V]): Map[K, Map[N, Column[N, V]]] = {
    val cp = new thrift.ColumnParent(name)
    val pred = new thrift.SlicePredicate()
    pred.setColumn_names(columnNames.toList.map { nameCodec.encode(_) }.asJava)
    log.debug("multiget_slice(%s, %s, %s, %s, %s)", keyspace, keys, cp, pred, consistency.level)
    val encodedKeys = keys.toList.map { keyCodec.encode(_) }.asJava
    val result = provider.map {
      _.multiget_slice(encodedKeys, cp, pred, consistency.level).asScala
    }
    return result.map {
      case (k, v) => (keyCodec.decode(k), v.asScala.map { r => Column.convert(nameCodec, valueCodec, r).pair }.toMap)
    }.toMap
  }

  /**
   * Returns a map of keys to a map of column names to the columns for a given
   * set of keys and columns as the default types.
   */
  def multigetColumns(keys: Set[Key],
                      columnNames: Set[Name],
                      consistency: ReadConsistency = defaultReadConsistency): Map[Key, Map[Name, Column[Name, Value]]] = {
    multigetColumnsAs[Key, Name, Value](keys, columnNames, consistency)(defaultKeyCodec, defaultNameCodec, defaultValueCodec)
  }

  /**
   * Inserts a column.
   */
  def insert[K, N, V](key: K,
                   column: Column[N, V],
                   consistency: WriteConsistency = defaultWriteConsistency)
                  (implicit keyCodec: Codec[K], nameCodec: Codec[N], valueCodec: Codec[V]) {
    val cp = new thrift.ColumnParent(name)
    log.debug("insert(%s, %s, %s, %s, %d, %s)", keyspace, key, cp, column.value,
      column.timestamp, consistency.level)
    provider.map {
      _.insert(keyCodec.encode(key), cp, Column.convert(nameCodec, valueCodec, column), consistency.level)
    }
  }

  /**
   * Removes a column from a key.
   */
  def removeColumn[K, N](key: K,
                         columnName: N,
                         consistency: WriteConsistency = defaultWriteConsistency)
                        (implicit clock: Clock, keyCodec: Codec[K], nameCodec: Codec[N]) {
    removeColumnWithTimestamp(key, columnName, clock.timestamp, consistency)(keyCodec, nameCodec)
  }

  /**
   * Removes a column from a key with a specific timestamp.
   */
  def removeColumnWithTimestamp[K, N](key: K,
                                      columnName: N,
                                      timestamp: Long,
                                      consistency: WriteConsistency = defaultWriteConsistency)
                                     (implicit keyCodec: Codec[K], nameCodec: Codec[N]) {
    val cp = new thrift.ColumnPath(name)
    cp.setColumn(nameCodec.encode(columnName))
    log.debug("remove(%s, %s, %s, %d, %s)", keyspace, key, cp, timestamp, consistency.level)
    provider.map { _.remove(keyCodec.encode(key), cp, timestamp, consistency.level) }
  }

  /**
   * Removes a set of columns from a key.
   */
  def removeColumns[K, N](key: K,
                          columnNames: Set[N],
                          consistency: WriteConsistency = defaultWriteConsistency)
                         (implicit clock: Clock, keyCodec: Codec[K], nameCodec: Codec[N]) {
    removeColumnsWithTimestamp(key, columnNames, clock.timestamp, consistency)(keyCodec, nameCodec)
  }

  /**
   * Removes a set of columns from a key with a specific timestamp.
   */
  def removeColumnsWithTimestamp[K, N](key: K,
                                       columnNames: Set[N],
                                       timestamp: Long,
                                       consistency: WriteConsistency = defaultWriteConsistency)
                                      (implicit keyCodec: Codec[K], nameCodec: Codec[N]) {
    batch(consistency) { cf =>
      cf.removeColumnsWithTimestamp(key, columnNames, timestamp)(keyCodec, nameCodec)
    }
  }

  /**
   * Removes a key.
   */
  def removeRow(key: Key,
                consistency: WriteConsistency = defaultWriteConsistency)
               (implicit clock: Clock) {
    removeRowWithTimestamp(key, clock.timestamp, consistency)
  }

  /**
   * Removes a key with a specific timestamp.
   */
  def removeRowWithTimestamp(key: Key,
                             timestamp: Long,
                             consistency: WriteConsistency = defaultWriteConsistency) {
    val cp = new thrift.ColumnPath(name)
    log.debug("remove(%s, %s, %s, %d, %s)", keyspace, key, cp, timestamp, consistency.level)
    provider.map { _.remove(defaultKeyCodec.encode(key), cp, timestamp, consistency.level) }
  }

  /**
   * Performs a series of actions in a single request.
   */
  def batch(consistency: WriteConsistency = defaultWriteConsistency)
           (build: BatchMutationBuilder => Unit) {
    val builder = new BatchMutationBuilder(name)
    build(builder)
    val mutations = builder.mutations
    log.debug("batch_mutate(%s, %s, %s", keyspace, mutations, consistency.level)
    provider.map { _.batch_mutate(mutations, consistency.level) }
  }

  /**
   * Returns a column iterator which iterates over all columns of all rows in
   * the column family with the given batch size and consistency level as the
   * given types.
   */
  def rowIteratorAs[K, N, V](batchSize: Int,
                          consistency: ReadConsistency = defaultReadConsistency)
                         (implicit keyCodec: Codec[K], nameCodec: Codec[N], valueCodec: Codec[V]): Iterator[(K, Column[N, V])] = {
    val pred = new thrift.SlicePredicate
    pred.setSlice_range(new thrift.SliceRange(EMPTY, EMPTY, false, Int.MaxValue))
    new ColumnIterator(this, EMPTY, EMPTY, batchSize, pred, consistency, keyCodec, nameCodec, valueCodec)
  }

  /**
   * Returns a column iterator which iterates over all columns of all rows in
   * the column family with the given batch size and consistency level as the
   * default types.
   */
  def rowIterator(batchSize: Int,
                  consistency: ReadConsistency = defaultReadConsistency): Iterator[(Key, Column[Name, Value])] = {
    rowIteratorAs[Key, Name, Value](batchSize, consistency)(defaultKeyCodec, defaultNameCodec, defaultValueCodec)
  }


  /**
   * Returns a column iterator which iterates over the given column of all rows
   * in the column family with the given batch size and consistency level as the
   * given types.
   */
  def columnIteratorAs[K, N, V](batchSize: Int,
                                columnName: N,
                                consistency: ReadConsistency = defaultReadConsistency)
                               (implicit keyCodec: Codec[K], nameCodec: Codec[N], valueCodec: Codec[V]): Iterator[(K, Column[N, V])] =
    columnsIteratorAs(batchSize, Set(columnName), consistency)(keyCodec, nameCodec, valueCodec)

  /**
   * Returns a column iterator which iterates over the given column of all rows
   * in the column family with the given batch size and consistency level as the
   * default types.
   */
  def columnIterator(batchSize: Int,
                     columnName: Name,
                     consistency: ReadConsistency = defaultReadConsistency): Iterator[(Key, Column[Name, Value])] =
    columnIteratorAs[Key, Name, Value](batchSize, columnName, consistency)(defaultKeyCodec, defaultNameCodec, defaultValueCodec)

  /**
   * Returns a column iterator which iterates over the given columns of all rows
   * in the column family with the given batch size and consistency level as the
   * given types.
   */
  def columnsIteratorAs[K, N, V](batchSize: Int,
                                 columnNames: Set[N],
                                 consistency: ReadConsistency = defaultReadConsistency)
                                (implicit keyCodec: Codec[K], nameCodec: Codec[N], valueCodec: Codec[V]): Iterator[(K, Column[N, V])] = {
    val pred = new thrift.SlicePredicate
    pred.setColumn_names(columnNames.toList.map { nameCodec.encode(_) }.asJava)
    new ColumnIterator(this, EMPTY, EMPTY, batchSize, pred, consistency, keyCodec, nameCodec, valueCodec)
  }

  /**
   * Returns a column iterator which iterates over the given columns of all rows
   * in the column family with the given batch size and consistency level as the
   * default types.
   */
  def columnsIterator(batchSize: Int,
                      columnNames: Set[Name],
                      consistency: ReadConsistency = defaultReadConsistency): Iterator[(Key, Column[Name, Value])] = {
    columnsIteratorAs[Key, Name, Value](batchSize, columnNames, consistency)(defaultKeyCodec, defaultNameCodec, defaultValueCodec)
  }

  private def getSlice[K, N, V](key: K,
                                pred: thrift.SlicePredicate,
                                consistency: ReadConsistency, keyCodec: Codec[K], nameCodec: Codec[N], valueCodec: Codec[V]) = {
    val cp = new thrift.ColumnParent(name)
    log.debug("get_slice(%s, %s, %s, %s, %s)", keyspace, key, cp, pred, consistency.level)
    val result = provider.map { _.get_slice(keyCodec.encode(key), cp, pred, consistency.level) }
    result.asScala.map { r => Column.convert(nameCodec, valueCodec, r).pair }.toMap
  }

  private[cassie] def getRangeSlice(startKey: ByteBuffer,
                                    endKey: ByteBuffer,
                                    count: Int,
                                    predicate: thrift.SlicePredicate,
                                    consistency: ReadConsistency) = {
    val cp = new thrift.ColumnParent(name)
    val range = new thrift.KeyRange(count)
    range.setStart_key(startKey)
    range.setEnd_key(endKey)
    log.debug("get_range_slices(%s, %s, %s, %s, %s)", keyspace, cp, predicate, range, consistency.level)
    provider.map { _.get_range_slices(cp, predicate, range, consistency.level) }.asScala
  }
}
