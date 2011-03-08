package com.twitter.cassie

import clocks.Clock
import codecs.{Codec, Utf8Codec}
import connection.ClientProvider

import org.apache.cassandra.finagle.thrift
import com.twitter.logging.Logger
import java.nio.ByteBuffer
import java.util.Collections.{singleton => singletonSet}

import java.util.{ArrayList, HashMap, Iterator, List, Map, Set}
import org.apache.cassandra.finagle.thrift.Mutation
import scala.collection.JavaConversions._

import com.twitter.util.Future

/**
 * A readable, writable column family with batching capabilities. This is a
 * lightweight object: it inherits a connection pool from the Keyspace.
 *
 * TODO: remove (insert/get)As methods in favor of copying the CF to allow for alternate types.
 *
 * @author coda
 */
case class ColumnFamily[Key, Name, Value](
    keyspace: String,
    name: String,
    provider: ClientProvider,
    clock: Clock,
    defaultKeyCodec: Codec[Key],
    defaultNameCodec: Codec[Name],
    defaultValueCodec: Codec[Value],
    readConsistency: ReadConsistency = ReadConsistency.Quorum,
    writeConsistency: WriteConsistency = WriteConsistency.Quorum) {

  val log = Logger.get

  import ColumnFamily._

  def clock(clock: Clock) = copy(clock = clock)
  def keysAs[K](codec: Codec[K]): ColumnFamily[K, Name, Value] = copy(defaultKeyCodec = codec)
  def namesAs[N](codec: Codec[N]): ColumnFamily[Key, N, Value] = copy(defaultNameCodec = codec)
  def valuesAs[V](codec: Codec[V]): ColumnFamily[Key, Name, V] = copy(defaultValueCodec = codec)
  def consistency(rc: ReadConsistency) = copy(readConsistency = rc)
  def consistency(wc: WriteConsistency) = copy(writeConsistency = wc)

  /**
   * @Java
   * Creates a new Column with.
   */
  def newColumn[N, V](n: N, v: V) = Column(n, v)

  /**
   * Returns the optional value of a given column for a given key as the given
   * types.
   */
  def getColumnAs[K, N, V](key: K,
                           columnName: N)
                          (implicit keyCodec: Codec[K], nameCodec: Codec[N], valueCodec: Codec[V]): Future[Option[Column[N, V]]] = {
    getColumnsAs(key, singletonSet(columnName))(keyCodec, nameCodec, valueCodec)
      .map { resmap => Option(resmap.get(columnName)) }
  }

  /**
   * Returns the optional value of a given column for a given key as the default
   * types.
   */
  def getColumn(key: Key,
                columnName: Name): Future[Option[Column[Name, Value]]] = {
    getColumnAs[Key, Name, Value](key, columnName)(defaultKeyCodec, defaultNameCodec, defaultValueCodec)
  }

  /**
   * Returns a map of all column names to the columns for a given key as the
   * given types. If your rows contain a huge number of columns, this will be
   * slow and horrible.
   */
  def getRowAs[K, N, V](key: K)
                    (implicit keyCodec: Codec[K], nameCodec: Codec[N], valueCodec: Codec[V]): Future[Map[N, Column[N, V]]] = {
    getRowSliceAs[K, N, V](key, None, None, Int.MaxValue, Order.Normal)(keyCodec, nameCodec, valueCodec)
  }

  /**
   * Returns a map of all column names to the columns for a given key as the
   * default types. If your rows contain a huge number of columns, this will be
   * slow and horrible.
   */
  def getRow(key: Key): Future[Map[Name, Column[Name, Value]]] = {
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
                            (implicit keyCodec: Codec[K], nameCodec: Codec[N], valueCodec: Codec[V]) = {
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
                  order: Order): Future[Map[Name, Column[Name, Value]]] = {
    getRowSliceAs[Key, Name, Value](key, startColumnName, endColumnName, count, order)(defaultKeyCodec, defaultNameCodec, defaultValueCodec)
  }

  /**
   * Returns a map of the given column names to the columns for a given key as
   * the given types.
   */
  def getColumnsAs[K, N, V](key: K,
                            columnNames: Set[N])
                           (implicit keyCodec: Codec[K], nameCodec: Codec[N], valueCodec: Codec[V]): Future[Map[N, Column[N, V]]] = {
    val pred = new thrift.SlicePredicate()
    pred.setColumn_names(encodeSet(columnNames))
    getSlice(key, pred, keyCodec, nameCodec, valueCodec)
  }

  /**
   * Returns a map of the given column names to the columns for a given key as
   * the default types.
   */
  def getColumns(key: Key,
                 columnNames: Set[Name]): Future[Map[Name, Column[Name, Value]]] = {
    getColumnsAs[Key, Name, Value](key, columnNames)(defaultKeyCodec, defaultNameCodec, defaultValueCodec)
  }


  /**
   * Returns a map of keys to given column for a set of keys as the given types.
   */
  def multigetColumnAs[K, N, V](keys: Set[K],
                                columnName: N)
                               (implicit keyCodec: Codec[K], nameCodec: Codec[N], valueCodec: Codec[V]): Future[Map[K, Column[N, V]]] = {
    multigetColumnsAs[K, N, V](keys, singletonSet(columnName))(keyCodec, nameCodec, valueCodec).map { rows =>
      val cols: Map[K, Column[N, V]] = new HashMap(rows.size)
      for (rowEntry <- asScalaIterable(rows.entrySet))
        if (!rowEntry.getValue.isEmpty)
          cols.put(rowEntry.getKey, rowEntry.getValue.get(columnName))
      cols
    }
  }

  /**
   * Returns a map of keys to given column for a set of keys as the default
   * types.
   */
  def multigetColumn(keys: Set[Key],
                     columnName: Name): Future[Map[Key, Column[Name, Value]]] = {
    multigetColumnAs[Key, Name, Value](keys, columnName)(defaultKeyCodec, defaultNameCodec, defaultValueCodec)
  }

  /**
   * Returns a map of keys to a map of column names to the columns for a given
   * set of keys and columns as the given types.
   */
  @throws(classOf[thrift.TimedOutException])
  @throws(classOf[thrift.UnavailableException])
  @throws(classOf[thrift.InvalidRequestException])
  def multigetColumnsAs[K, N, V](keys: Set[K],
                              columnNames: Set[N])
                             (implicit keyCodec: Codec[K], nameCodec: Codec[N], valueCodec: Codec[V]): Future[Map[K, Map[N, Column[N, V]]]] = {
    val cp = new thrift.ColumnParent(name)
    val pred = new thrift.SlicePredicate()
    pred.setColumn_names(encodeSet(columnNames))
    log.debug("multiget_slice(%s, %s, %s, %s, %s)", keyspace, keys, cp, pred, readConsistency.level)
    val encodedKeys = encodeSet(keys)
    provider.map {
      _.multiget_slice(encodedKeys, cp, pred, readConsistency.level)
    }.map { result =>
      // decode result
      val rows: Map[K, Map[N, Column[N, V]]] = new HashMap(result.size)
      for (rowEntry <- asScalaIterable(result.entrySet)) {
        val cols: Map[N, Column[N, V]] = new HashMap(rowEntry.getValue.size)
        for (cosc <- asScalaIterable(rowEntry.getValue)) {
          val col = Column.convert(nameCodec, valueCodec, cosc)
          cols.put(col.name, col)
        }
        rows.put(keyCodec.decode(rowEntry.getKey), cols)
      }
      rows
    }
  }

  /**
   * Returns a map of keys to a map of column names to the columns for a given
   * set of keys and columns as the default types.
   */
  def multigetColumns(keys: Set[Key], columnNames: Set[Name]) = {
    multigetColumnsAs[Key, Name, Value](keys, columnNames)(defaultKeyCodec, defaultNameCodec, defaultValueCodec)
  }

  /**
   * Inserts a column.
   */
  @throws(classOf[thrift.TimedOutException])
  @throws(classOf[thrift.UnavailableException])
  @throws(classOf[thrift.InvalidRequestException])
  def insert(key: Key, column: Column[Name, Value]) = {
    insertAs(key, column)(defaultKeyCodec, defaultNameCodec, defaultValueCodec)
  }

  def insertAs[K, N, V](key: K, column: Column[N, V])
                  (implicit keyCodec: Codec[K], nameCodec: Codec[N], valueCodec: Codec[V]) = {
    val cp = new thrift.ColumnParent(name)
    val col = Column.convert(nameCodec, valueCodec, clock, column)
    log.debug("insert(%s, %s, %s, %s, %d, %s)", keyspace, key, cp, column.value,
      col.timestamp, writeConsistency.level)
    provider.map {
      _.insert(keyCodec.encode(key), cp, col, writeConsistency.level)
    }
  }

  /**
   * Removes a column from a key.
   */
   @throws(classOf[thrift.TimedOutException])
   @throws(classOf[thrift.UnavailableException])
   @throws(classOf[thrift.InvalidRequestException])
  def removeColumn(key: Key, columnName: Name) = {
    val cp = new thrift.ColumnPath(name)
    val timestamp = clock.timestamp
    cp.setColumn(defaultNameCodec.encode(columnName))
    log.debug("remove(%s, %s, %s, %d, %s)", keyspace, key, cp, timestamp, writeConsistency.level)
    provider.map { _.remove(defaultKeyCodec.encode(key), cp, timestamp, writeConsistency.level) }
  }

  /**
   * Removes a set of columns from a key.
   */
  def removeColumns(key: Key, columnNames: Set[Name]) = {
    batch()
      .removeColumns(key, columnNames)
      .execute()
  }

  /**
   * Removes a key.
   */
  def removeRow(key: Key) = {
    removeRowWithTimestamp(key, clock.timestamp)
  }

  /**
   * Removes a key with a specific timestamp.
   */
  @throws(classOf[thrift.TimedOutException])
  @throws(classOf[thrift.UnavailableException])
  @throws(classOf[thrift.InvalidRequestException])
  def removeRowWithTimestamp(key: Key, timestamp: Long) = {
    val cp = new thrift.ColumnPath(name)
    log.debug("remove(%s, %s, %s, %d, %s)", keyspace, key, cp, timestamp, writeConsistency.level)
    provider.map { _.remove(defaultKeyCodec.encode(key), cp, timestamp, writeConsistency.level) }
  }

  /**
   * @return A Builder that can be used to execute multiple actions in a single
   * request.
   */
  def batch() = new BatchMutationBuilder(this)

  @throws(classOf[thrift.TimedOutException])
  @throws(classOf[thrift.UnavailableException])
  @throws(classOf[thrift.InvalidRequestException])
  private[cassie] def batch(mutations: java.util.Map[ByteBuffer, java.util.Map[String, java.util.List[Mutation]]]) = {
    log.debug("batch_mutate(%s, %s, %s", keyspace, mutations, writeConsistency.level)
    provider.map { _.batch_mutate(mutations, writeConsistency.level) }
  }

  /**
   * Returns a column iterator which iterates over all columns of all rows in
   * the column family with the given batch size as the given types.
   */
  def rowIterateeAs[K, N, V](batchSize: Int)
                         (implicit keyCodec: Codec[K], nameCodec: Codec[N], valueCodec: Codec[V]): ColumnIteratee[K, N, V] = {
    val pred = new thrift.SlicePredicate
    pred.setSlice_range(new thrift.SliceRange(EMPTY, EMPTY, false, Int.MaxValue))
    new ColumnIteratee(this, EMPTY, EMPTY, batchSize, pred, keyCodec, nameCodec, valueCodec)
  }

  /**
   * Returns a column iterator which iterates over all columns of all rows in
   * the column family with the given batch size as the default types.
   */
  def rowIteratee(batchSize: Int): ColumnIteratee[Key, Name, Value] = {
    rowIterateeAs[Key, Name, Value](batchSize)(defaultKeyCodec, defaultNameCodec, defaultValueCodec)
  }


  /**
   * Returns a column iterator which iterates over the given column of all rows
   * in the column family with the given batch size as the given types.
   */
  def columnIterateeAs[K, N, V](batchSize: Int, columnName: N)
                               (implicit keyCodec: Codec[K], nameCodec: Codec[N], valueCodec: Codec[V]): ColumnIteratee[K, N, V] =
    columnsIterateeAs(batchSize, singletonSet(columnName))(keyCodec, nameCodec, valueCodec)

  /**
   * Returns a column iterator which iterates over the given column of all rows
   * in the column family with the given batch size as the default types.
   */
  def columnIteratee(batchSize: Int,
                     columnName: Name): ColumnIteratee[Key, Name, Value] =
    columnIterateeAs[Key, Name, Value](batchSize, columnName)(defaultKeyCodec, defaultNameCodec, defaultValueCodec)

  /**
   * Returns a column iterator which iterates over the given columns of all rows
   * in the column family with the given batch size as the given types.
   */
  def columnsIterateeAs[K, N, V](batchSize: Int,
                                 columnNames: Set[N])
                                (implicit keyCodec: Codec[K], nameCodec: Codec[N], valueCodec: Codec[V]): ColumnIteratee[K, N, V] = {
    val pred = new thrift.SlicePredicate
    pred.setColumn_names(encodeSet(columnNames))
    new ColumnIteratee(this, EMPTY, EMPTY, batchSize, pred, keyCodec, nameCodec, valueCodec)
  }

  /**
   * Returns a column iterator which iterates over the given columns of all rows
   * in the column family with the given batch size as the default types.
   */
  def columnsIteratee(batchSize: Int,
                      columnNames: Set[Name]): ColumnIteratee[Key, Name, Value] = {
    columnsIterateeAs[Key, Name, Value](batchSize, columnNames)(defaultKeyCodec, defaultNameCodec, defaultValueCodec)
  }

  @throws(classOf[thrift.TimedOutException])
  @throws(classOf[thrift.UnavailableException])
  @throws(classOf[thrift.InvalidRequestException])
  private def getSlice[K, N, V](key: K,
                                pred: thrift.SlicePredicate,
                                keyCodec: Codec[K], nameCodec: Codec[N], valueCodec: Codec[V]): Future[Map[N,Column[N,V]]] = {
    val cp = new thrift.ColumnParent(name)
    log.debug("get_slice(%s, %s, %s, %s, %s)", keyspace, key, cp, pred, readConsistency.level)
    provider.map { _.get_slice(keyCodec.encode(key), cp, pred, readConsistency.level) }
      .map { result =>
        val cols: Map[N,Column[N,V]] = new HashMap(result.size)
        for (cosc <- result.iterator) {
          val col = Column.convert(nameCodec, valueCodec, cosc)
          cols.put(col.name, col)
        }
        cols
      }
  }

  @throws(classOf[thrift.TimedOutException])
  @throws(classOf[thrift.UnavailableException])
  @throws(classOf[thrift.InvalidRequestException])
  private[cassie] def getRangeSlice(startKey: ByteBuffer,
                                    endKey: ByteBuffer,
                                    count: Int,
                                    predicate: thrift.SlicePredicate) = {
    val cp = new thrift.ColumnParent(name)
    val range = new thrift.KeyRange(count)
    range.setStart_key(startKey)
    range.setEnd_key(endKey)
    log.debug("get_range_slices(%s, %s, %s, %s, %s)", keyspace, cp, predicate, range, readConsistency.level)
    provider.map { _.get_range_slices(cp, predicate, range, readConsistency.level) }
  }

  def encodeSet[V](values: Set[V])(implicit codec: Codec[V]): List[ByteBuffer] = {
    val output = new ArrayList[ByteBuffer](values.size)
    for (value <- asScalaIterable(values))
      output.add(codec.encode(value))
    output
  }
}

private[cassie] object ColumnFamily
{
  val EMPTY = ByteBuffer.allocate(0)
}
