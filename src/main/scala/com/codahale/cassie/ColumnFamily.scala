package com.codahale.cassie

import clocks.Clock
import codecs.{Codec, Utf8Codec}
import connection.ClientProvider
import scalaj.collection.Imports._
import org.apache.cassandra.thrift
import com.codahale.logula.Logging
import java.nio.ByteBuffer

import java.util.{ArrayList, HashMap, Iterator, List, Map}

/**
 * A readable, writable column family with batching capabilities. This is a
 * lightweight object: it inherits a connection pool from the Keyspace.
 *
 * TODO: add copy methods for codecs
 *
 * @author coda
 */
case class ColumnFamily[Key, Name, Value](
    keyspace: String,
    name: String,
    provider: ClientProvider,
    defaultKeyCodec: Codec[Key],
    defaultNameCodec: Codec[Name],
    defaultValueCodec: Codec[Value],
    readConsistency: ReadConsistency = ReadConsistency.Quorum,
    writeConsistency: WriteConsistency = WriteConsistency.Quorum)
  extends Logging {

  import ColumnFamily._

  def consistency(rc: ReadConsistency) = copy(readConsistency = rc)
  def consistency(wc: WriteConsistency) = copy(writeConsistency = wc)

  /**
   * Returns the optional value of a given column for a given key as the given
   * types.
   */
  def getColumnAs[K, N, V](key: K,
                           columnName: N)
                          (implicit keyCodec: Codec[K], nameCodec: Codec[N], valueCodec: Codec[V]): Option[Column[N, V]] = {
    Option(getColumnsAs(key, Set(columnName))(keyCodec, nameCodec, valueCodec).get(columnName))
  }

  /**
   * Returns the optional value of a given column for a given key as the default
   * types.
   */
  def getColumn(key: Key,
                columnName: Name): Option[Column[Name, Value]] = {
    getColumnAs[Key, Name, Value](key, columnName)(defaultKeyCodec, defaultNameCodec, defaultValueCodec)
  }

  /**
   * Returns a map of all column names to the columns for a given key as the
   * given types. If your rows contain a huge number of columns, this will be
   * slow and horrible.
   */
  def getRowAs[K, N, V](key: K)
                    (implicit keyCodec: Codec[K], nameCodec: Codec[N], valueCodec: Codec[V]): Map[N, Column[N, V]] = {
    getRowSliceAs[K, N, V](key, None, None, Int.MaxValue, Order.Normal)(keyCodec, nameCodec, valueCodec)
  }

  /**
   * Returns a map of all column names to the columns for a given key as the
   * default types. If your rows contain a huge number of columns, this will be
   * slow and horrible.
   */
  def getRow(key: Key): Map[Name, Column[Name, Value]] = {
    getRowAs[Key, Name, Value](key)(defaultKeyCodec, defaultNameCodec, defaultValueCodec)
  }

  /**
   * Returns a slice of all columns of a row as the given types.
   */
  def getRowSliceAs[K, N, V](key: K,
                             startColumnName: Option[N],
                             endColumnName: Option[N],
                             count: Int,
                             order: Order)
                            (implicit keyCodec: Codec[K], nameCodec: Codec[N], valueCodec: Codec[V]): Map[N, Column[N, V]] = {
    val startBytes = startColumnName.map { c => nameCodec.encode(c) }.getOrElse(EMPTY)
    val endBytes = endColumnName.map { c => nameCodec.encode(c) }.getOrElse(EMPTY)
    val pred = new thrift.SlicePredicate()
    pred.setSlice_range(new thrift.SliceRange(startBytes, endBytes, order.reversed, count))
    getSlice(key, pred, keyCodec, nameCodec, valueCodec)
  }

  /**
   * Returns a slice of all columns of a row as the default types.
   */
  def getRowSlice(key: Key,
                  startColumnName: Option[Name],
                  endColumnName: Option[Name],
                  count: Int,
                  order: Order): Map[Name, Column[Name, Value]] = {
    getRowSliceAs[Key, Name, Value](key, startColumnName, endColumnName, count, order)(defaultKeyCodec, defaultNameCodec, defaultValueCodec)
  }

  /**
   * Returns a map of the given column names to the columns for a given key as
   * the given types.
   */
  def getColumnsAs[K, N, V](key: K,
                            columnNames: Set[N])
                           (implicit keyCodec: Codec[K], nameCodec: Codec[N], valueCodec: Codec[V]): Map[N, Column[N, V]] = {
    val pred = new thrift.SlicePredicate()
    pred.setColumn_names(columnNames.toList.map { nameCodec.encode(_) }.asJava)
    getSlice(key, pred, keyCodec, nameCodec, valueCodec)
  }

  /**
   * Returns a map of the given column names to the columns for a given key as
   * the default types.
   */
  def getColumns(key: Key,
                 columnNames: Set[Name]): Map[Name, Column[Name, Value]] = {
    getColumnsAs[Key, Name, Value](key, columnNames)(defaultKeyCodec, defaultNameCodec, defaultValueCodec)
  }


  /**
   * Returns a map of keys to given column for a set of keys as the given types.
   */
  def multigetColumnAs[K, N, V](keys: Set[K],
                                columnName: N)
                               (implicit keyCodec: Codec[K], nameCodec: Codec[N], valueCodec: Codec[V]): Map[K, Column[N, V]] = {
    val rows = multigetColumnsAs[K, N, V](keys, Set(columnName))(keyCodec, nameCodec, valueCodec)
      
    val cols: Map[K, Column[N, V]] = new HashMap(rows.size)
    for (rowEntry <- rows.entrySet.asScala)
      if (!rowEntry.getValue.isEmpty)
        cols.put(rowEntry.getKey, rowEntry.getValue.get(columnName))
    cols
  }

  /**
   * Returns a map of keys to given column for a set of keys as the default
   * types.
   */
  def multigetColumn(keys: Set[Key],
                     columnName: Name): Map[Key, Column[Name, Value]] = {
    multigetColumnAs[Key, Name, Value](keys, columnName)(defaultKeyCodec, defaultNameCodec, defaultValueCodec)
  }

  /**
   * Returns a map of keys to a map of column names to the columns for a given
   * set of keys and columns as the given types.
   */
  def multigetColumnsAs[K, N, V](keys: Set[K],
                              columnNames: Set[N])
                             (implicit keyCodec: Codec[K], nameCodec: Codec[N], valueCodec: Codec[V]): Map[K, Map[N, Column[N, V]]] = {
    val cp = new thrift.ColumnParent(name)
    val pred = new thrift.SlicePredicate()
    pred.setColumn_names(columnNames.toList.map { nameCodec.encode(_) }.asJava)
    log.debug("multiget_slice(%s, %s, %s, %s, %s)", keyspace, keys, cp, pred, readConsistency.level)
    val encodedKeys = keys.toList.map { keyCodec.encode(_) }.asJava
    val result = provider.map {
      _.multiget_slice(encodedKeys, cp, pred, readConsistency.level)
    }()
    // decode result
    val rows: Map[K, Map[N, Column[N, V]]] = new HashMap(result.size)
    for (rowEntry <- result.entrySet.asScala) {
      val cols: Map[N, Column[N, V]] = new HashMap(rowEntry.getValue.size)
      for (cosc <- rowEntry.getValue.asScala) {
        val col = Column.convert(nameCodec, valueCodec, cosc)
        cols.put(col.name, col)
      }
      rows.put(keyCodec.decode(rowEntry.getKey), cols)
    }
    rows
  }

  /**
   * Returns a map of keys to a map of column names to the columns for a given
   * set of keys and columns as the default types.
   */
  def multigetColumns(keys: Set[Key],
                      columnNames: Set[Name]): Map[Key, Map[Name, Column[Name, Value]]] = {
    multigetColumnsAs[Key, Name, Value](keys, columnNames)(defaultKeyCodec, defaultNameCodec, defaultValueCodec)
  }

  /**
   * Inserts a column.
   */
  def insert[K, N, V](key: K,
                   column: Column[N, V])
                  (implicit keyCodec: Codec[K], nameCodec: Codec[N], valueCodec: Codec[V]) {
    val cp = new thrift.ColumnParent(name)
    log.debug("insert(%s, %s, %s, %s, %d, %s)", keyspace, key, cp, column.value,
      column.timestamp, writeConsistency.level)
    provider.map {
      _.insert(keyCodec.encode(key), cp, Column.convert(nameCodec, valueCodec, column), writeConsistency.level)
    }
  }

  /**
   * Removes a column from a key.
   */
  def removeColumn[K, N](key: K,
                         columnName: N)
                        (implicit clock: Clock, keyCodec: Codec[K], nameCodec: Codec[N]) {
    removeColumnWithTimestamp(key, columnName, clock.timestamp)(keyCodec, nameCodec)
  }

  /**
   * Removes a column from a key with a specific timestamp.
   */
  def removeColumnWithTimestamp[K, N](key: K,
                                      columnName: N,
                                      timestamp: Long)
                                     (implicit keyCodec: Codec[K], nameCodec: Codec[N]) {
    val cp = new thrift.ColumnPath(name)
    cp.setColumn(nameCodec.encode(columnName))
    log.debug("remove(%s, %s, %s, %d, %s)", keyspace, key, cp, timestamp, writeConsistency.level)
    provider.map { _.remove(keyCodec.encode(key), cp, timestamp, writeConsistency.level) }
  }

  /**
   * Removes a set of columns from a key.
   */
  def removeColumns[K, N](key: K,
                          columnNames: Set[N])
                         (implicit clock: Clock, keyCodec: Codec[K], nameCodec: Codec[N]) {
    removeColumnsWithTimestamp(key, columnNames, clock.timestamp)(keyCodec, nameCodec)
  }

  /**
   * Removes a set of columns from a key with a specific timestamp.
   */
  def removeColumnsWithTimestamp[K, N](key: K,
                                       columnNames: Set[N],
                                       timestamp: Long)
                                      (implicit keyCodec: Codec[K], nameCodec: Codec[N]) {
    batch() { cf =>
      cf.removeColumnsWithTimestamp(key, columnNames, timestamp)(keyCodec, nameCodec)
    }
  }

  /**
   * Removes a key.
   */
  def removeRow(key: Key)
               (implicit clock: Clock) {
    removeRowWithTimestamp(key, clock.timestamp)
  }

  /**
   * Removes a key with a specific timestamp.
   */
  def removeRowWithTimestamp(key: Key,
                             timestamp: Long) {
    val cp = new thrift.ColumnPath(name)
    log.debug("remove(%s, %s, %s, %d, %s)", keyspace, key, cp, timestamp, writeConsistency.level)
    provider.map { _.remove(defaultKeyCodec.encode(key), cp, timestamp, writeConsistency.level) }
  }

  /**
   * Performs a series of actions in a single request.
   */
  def batch()
           (build: BatchMutationBuilder => Unit) {
    val builder = new BatchMutationBuilder(name)
    build(builder)
    val mutations = builder.mutations
    log.debug("batch_mutate(%s, %s, %s", keyspace, mutations, writeConsistency.level)
    provider.map { _.batch_mutate(mutations, writeConsistency.level) }
  }

  /**
   * Returns a column iterator which iterates over all columns of all rows in
   * the column family with the given batch size as the given types.
   */
  def rowIteratorAs[K, N, V](batchSize: Int)
                         (implicit keyCodec: Codec[K], nameCodec: Codec[N], valueCodec: Codec[V]): Iterator[(K, Column[N, V])] = {
    val pred = new thrift.SlicePredicate
    pred.setSlice_range(new thrift.SliceRange(EMPTY, EMPTY, false, Int.MaxValue))
    new ColumnIterator(this, EMPTY, EMPTY, batchSize, pred, keyCodec, nameCodec, valueCodec)
  }

  /**
   * Returns a column iterator which iterates over all columns of all rows in
   * the column family with the given batch size as the default types.
   */
  def rowIterator(batchSize: Int): Iterator[(Key, Column[Name, Value])] = {
    rowIteratorAs[Key, Name, Value](batchSize)(defaultKeyCodec, defaultNameCodec, defaultValueCodec)
  }


  /**
   * Returns a column iterator which iterates over the given column of all rows
   * in the column family with the given batch size as the given types.
   */
  def columnIteratorAs[K, N, V](batchSize: Int, columnName: N)
                               (implicit keyCodec: Codec[K], nameCodec: Codec[N], valueCodec: Codec[V]): Iterator[(K, Column[N, V])] =
    columnsIteratorAs(batchSize, Set(columnName))(keyCodec, nameCodec, valueCodec)

  /**
   * Returns a column iterator which iterates over the given column of all rows
   * in the column family with the given batch size as the default types.
   */
  def columnIterator(batchSize: Int,
                     columnName: Name): Iterator[(Key, Column[Name, Value])] =
    columnIteratorAs[Key, Name, Value](batchSize, columnName)(defaultKeyCodec, defaultNameCodec, defaultValueCodec)

  /**
   * Returns a column iterator which iterates over the given columns of all rows
   * in the column family with the given batch size as the given types.
   */
  def columnsIteratorAs[K, N, V](batchSize: Int,
                                 columnNames: Set[N])
                                (implicit keyCodec: Codec[K], nameCodec: Codec[N], valueCodec: Codec[V]): Iterator[(K, Column[N, V])] = {
    val pred = new thrift.SlicePredicate
    pred.setColumn_names(columnNames.toList.map { nameCodec.encode(_) }.asJava)
    new ColumnIterator(this, EMPTY, EMPTY, batchSize, pred, keyCodec, nameCodec, valueCodec)
  }

  /**
   * Returns a column iterator which iterates over the given columns of all rows
   * in the column family with the given batch size as the default types.
   */
  def columnsIterator(batchSize: Int,
                      columnNames: Set[Name]): Iterator[(Key, Column[Name, Value])] = {
    columnsIteratorAs[Key, Name, Value](batchSize, columnNames)(defaultKeyCodec, defaultNameCodec, defaultValueCodec)
  }

  private def getSlice[K, N, V](key: K,
                                pred: thrift.SlicePredicate,
                                keyCodec: Codec[K], nameCodec: Codec[N], valueCodec: Codec[V]): Map[N,Column[N,V]] = {
    val cp = new thrift.ColumnParent(name)
    log.debug("get_slice(%s, %s, %s, %s, %s)", keyspace, key, cp, pred, readConsistency.level)
    val result = provider.map { _.get_slice(keyCodec.encode(key), cp, pred, readConsistency.level) }()

    val cols: Map[N,Column[N,V]] = new HashMap(result.size)
    for (cosc <- result.iterator) {
      val col = Column.convert(nameCodec, valueCodec, cosc)
      cols.put(col.name, col)
    }
    cols
  }

  private[cassie] def getRangeSlice(startKey: ByteBuffer,
                                    endKey: ByteBuffer,
                                    count: Int,
                                    predicate: thrift.SlicePredicate) = {
    val cp = new thrift.ColumnParent(name)
    val range = new thrift.KeyRange(count)
    range.setStart_key(startKey)
    range.setEnd_key(endKey)
    log.debug("get_range_slices(%s, %s, %s, %s, %s)", keyspace, cp, predicate, range, readConsistency.level)
    provider.map { _.get_range_slices(cp, predicate, range, readConsistency.level) }().asScala
  }
}

private object ColumnFamily
{
  val EMPTY = ByteBuffer.allocate(0)
}
