package com.twitter.cassie

import codecs.{Codec, Utf8Codec}
import connection.ClientProvider

import org.apache.cassandra.finagle.thrift
import com.twitter.logging.Logger
import java.nio.ByteBuffer
import java.util.Collections.{singleton => singletonSet}

import java.util.{ArrayList, HashMap, Iterator, List, Map, Set}
import thrift.CounterMutation
import scala.collection.JavaConversions._

import com.twitter.util.Future

/**
 * A readable, writable column family with batching capabilities. This is a
 * lightweight object: it inherits a connection pool from the Keyspace.
 *
 * TODO: remove (insert/get)As methods in favor of copying the CF to allow for alternate types.
 * TODO: figure out how to get rid of code duplication vs non counter columns
 */
case class CounterColumnFamily[Key, Name](
    keyspace: String,
    name: String,
    provider: ClientProvider,
    defaultKeyCodec: Codec[Key],
    defaultNameCodec: Codec[Name],
    readConsistency: ReadConsistency = ReadConsistency.Quorum) {

  val log = Logger.get

  import CounterColumnFamily._

  val writeConsistency = WriteConsistency.One

  def keysAs[K](codec: Codec[K]): CounterColumnFamily[K, Name] = copy(defaultKeyCodec = codec)
  def namesAs[N](codec: Codec[N]): CounterColumnFamily[Key, N] = copy(defaultNameCodec = codec)
  def consistency(rc: ReadConsistency) = copy(readConsistency = rc)

  /**
   * @Java
   * Creates a new Column.
   */
  def newColumn[N](n: N, v: Long) = CounterColumn(n, v)

  private[cassie] def getColumnAs[K, N](key: K,
                           columnName: N)
                          (implicit keyCodec: Codec[K], nameCodec: Codec[N]): Future[Option[CounterColumn[N]]] = {
    getColumnsAs(key, singletonSet(columnName))(keyCodec, nameCodec)
      .map { resmap => Option(resmap.get(columnName)) }
  }

  /**
    * Get an individual column from a single row.
    * @return a future that can contain [[org.apache.cassandra.finagle.thrift.TimedOutException]],
    *  [[org.apache.cassandra.finagle.thrift.UnavailableException]] or [[org.apache.cassandra.finagle.thrift.InvalidRequestException]]
    * @param key the row's key
    * @param the name of the column */
  def getColumn(key: Key,
                columnName: Name): Future[Option[CounterColumn[Name]]] = {
    getColumnAs[Key, Name](key, columnName)(defaultKeyCodec, defaultNameCodec)
  }

  private[cassie] def getRowAs[K, N](key: K)
                    (implicit keyCodec: Codec[K], nameCodec: Codec[N]): Future[Map[N, CounterColumn[N]]] = {
    getRowSliceAs[K, N](key, None, None, Int.MaxValue, Order.Normal)(keyCodec, nameCodec)
  }

  /**
    * Results in a map of all column names to the columns for a given key by slicing over a whole row.
    *   If your rows contain a huge number of columns, this will be slow and horrible and you will hate your ife.
    * @return a future that can contain [[org.apache.cassandra.finagle.thrift.TimedOutException]],
    *  [[org.apache.cassandra.finagle.thrift.UnavailableException]] or [[org.apache.cassandra.finagle.thrift.InvalidRequestException]]
    * @param key the row's key */
  def getRow(key: Key): Future[Map[Name, CounterColumn[Name]]] = {
    getRowAs[Key, Name](key)(defaultKeyCodec, defaultNameCodec)
  }

  /**
   * Returns a slice of all columns of a row as the given types.
   */
  private[cassie] def getRowSliceAs[K, N](key: K,
                          startColumnName: Option[N],
                          endColumnName: Option[N],
                          count: Int,
                          order: Order)
                          (implicit keyCodec: Codec[K], nameCodec: Codec[N]) = {
    val startBytes = startColumnName.map { c => nameCodec.encode(c) }.getOrElse(EMPTY)
    val endBytes = endColumnName.map { c => nameCodec.encode(c) }.getOrElse(EMPTY)
    val pred = new thrift.SlicePredicate()
    pred.setSlice_range(new thrift.SliceRange(startBytes, endBytes, order.reversed, count))
    getSlice(key, pred, keyCodec, nameCodec)
  }

  /**
    * Get a slice of a single row, starting at `startColumnName` (inclusive) and continuing to `endColumnName` (inclusive).
    *   ordering is determined by the server. 
    * @return a future that can contain [[org.apache.cassandra.finagle.thrift.TimedOutException]],
    *   [[org.apache.cassandra.finagle.thrift.UnavailableException]] or [[org.apache.cassandra.finagle.thrift.InvalidRequestException]]
    * @param key the row's key
    * @param startColumnName an optional start. if None it starts at the first column
    * @param endColumnName an optional end. if None it ends at the last column
    * @param count like LIMIT in SQL. note that all of start..end will be loaded into memory
    * @param order sort forward or reverse (by column name) */
  def getRowSlice(key: Key,
                  startColumnName: Option[Name],
                  endColumnName: Option[Name],
                  count: Int,
                  order: Order): Future[Map[Name, CounterColumn[Name]]] = {
    getRowSliceAs[Key, Name](key, startColumnName, endColumnName, count, order)(defaultKeyCodec, defaultNameCodec)
  }

  private[cassie] def getColumnsAs[K, N](key: K,
                         columnNames: Set[N])
                         (implicit keyCodec: Codec[K], nameCodec: Codec[N]): Future[Map[N, CounterColumn[N]]] = {
    val pred = new thrift.SlicePredicate()
    pred.setColumn_names(encodeSet(columnNames))
    getSlice(key, pred, keyCodec, nameCodec)
  }

  /**
    * Get a selection of columns from a single row.
    * @return a future that can contain [[org.apache.cassandra.finagle.thrift.TimedOutException]],
    *   [[org.apache.cassandra.finagle.thrift.UnavailableException]] or [[org.apache.cassandra.finagle.thrift.InvalidRequestException]]
    * @param key the row key
    * @param the column names you want */
  def getColumns(key: Key,
                 columnNames: Set[Name]): Future[Map[Name, CounterColumn[Name]]] = {
    getColumnsAs[Key, Name](key, columnNames)(defaultKeyCodec, defaultNameCodec)
  }

  private[cassie] def multigetColumnAs[K, N](keys: Set[K],
                             columnName: N)
                             (implicit keyCodec: Codec[K], nameCodec: Codec[N]): Future[Map[K, CounterColumn[N]]] = {
    multigetColumnsAs[K, N](keys, singletonSet(columnName))(keyCodec, nameCodec).map { rows =>
      val cols: Map[K, CounterColumn[N]] = new HashMap(rows.size)
      for (rowEntry <- asScalaIterable(rows.entrySet))
        if (!rowEntry.getValue.isEmpty) {
          cols.put(rowEntry.getKey, rowEntry.getValue.get(columnName))
        }
      cols
    }
  }

  /**
    * Get a single column from multiple rows.
    * @return a future that can contain [[org.apache.cassandra.finagle.thrift.TimedOutException]],
    *   [[org.apache.cassandra.finagle.thrift.UnavailableException]] or [[org.apache.cassandra.finagle.thrift.InvalidRequestException]].
    * @param keys the row keys
    * @param the column name */
  def multigetColumn(keys: Set[Key],
                     columnName: Name): Future[Map[Key, CounterColumn[Name]]] = {
    multigetColumnAs[Key, Name](keys, columnName)(defaultKeyCodec, defaultNameCodec)
  }

  private[cassie] def multigetColumnsAs[K, N](keys: Set[K],
                              columnNames: Set[N])
                             (implicit keyCodec: Codec[K], nameCodec: Codec[N]): Future[Map[K, Map[N, CounterColumn[N]]]] = {
    val cp = new thrift.ColumnParent(name)
    val pred = new thrift.SlicePredicate()
    pred.setColumn_names(encodeSet(columnNames))
    log.debug("multiget_counter_slice(%s, %s, %s, %s, %s)", keyspace, keys, cp, pred, readConsistency.level)
    val encodedKeys = encodeSet(keys)
    provider.map {
      _.multiget_counter_slice(encodedKeys, cp, pred, readConsistency.level)
    }.map { result =>
      // decode result
      val rows: Map[K, Map[N, CounterColumn[N]]] = new HashMap(result.size)
      for (rowEntry <- asScalaIterable(result.entrySet)) {
        val cols: Map[N, CounterColumn[N]] = new HashMap(rowEntry.getValue.size)
        for (counter <- asScalaIterable(rowEntry.getValue)) {
          val col = CounterColumn.convert(nameCodec, counter)
          cols.put(col.name, col)
        }
        rows.put(keyCodec.decode(rowEntry.getKey), cols)
      }
      rows
    }
  }

  /**
    * Get multiple columns from multiple rows.
    * @return a future that can contain [[org.apache.cassandra.finagle.thrift.TimedOutException]],
    *  [[org.apache.cassandra.finagle.thrift.UnavailableException]] or [[org.apache.cassandra.finagle.thrift.InvalidRequestException]]
    * @param keys the row keys
    * @param columnNames the column names */
  def multigetColumns(keys: Set[Key], columnNames: Set[Name]) = {
    multigetColumnsAs[Key, Name](keys, columnNames)(defaultKeyCodec, defaultNameCodec)
  }

  /**
   * Increments a column. */
  def add(key: Key, column: CounterColumn[Name]) = {
    addAs(key, column)(defaultKeyCodec, defaultNameCodec)
  }

  private[cassie] def addAs[K, N](key: K, column: CounterColumn[N])
                  (implicit keyCodec: Codec[K], nameCodec: Codec[N]) = {
    val cp = new thrift.ColumnParent(name)
    val col = CounterColumn.convert(nameCodec, column)
    log.debug("add(%s, %s, %s, %d, %s)", keyspace, key, cp, column.value,
      writeConsistency.level)
    provider.map {
      _.add(keyCodec.encode(key), cp, col, writeConsistency.level)
    }
  }

  /**
    * Remove a single column.
    * @return a future that can contain [[org.apache.cassandra.finagle.thrift.TimedOutException]],
    *  [[org.apache.cassandra.finagle.thrift.UnavailableException]] or [[org.apache.cassandra.finagle.thrift.InvalidRequestException]]
    * @param key the row key
    * @param columnName the column's name */
  def removeColumn(key: Key, columnName: Name) = {
    val cp = new thrift.ColumnPath(name)
    cp.setColumn(defaultNameCodec.encode(columnName))
    log.debug("remove_counter(%s, %s, %s, %s)", keyspace, key, cp, writeConsistency.level)
    provider.map { _.remove_counter(defaultKeyCodec.encode(key), cp, writeConsistency.level) }
  }

  /**
    * Remove a set of columns from a single row via a batch mutation.
    * @return a future that can contain [[org.apache.cassandra.finagle.thrift.TimedOutException]],
    *  [[org.apache.cassandra.finagle.thrift.UnavailableException]] or [[org.apache.cassandra.finagle.thrift.InvalidRequestException]]
    * @param key the row key
    * @param columnNames the names of the columns to be deleted */
  def removeColumns(key: Key, columnNames: Set[Name]) = {
    batch()
      .removeColumns(key, columnNames)
      .execute()
  }

  /**
   * @return A Builder that can be used to execute multiple actions in a single
   * request.
   */
  def batch() = new CounterBatchMutationBuilder(this)

  private[cassie] def batch(mutations: java.util.Map[ByteBuffer, java.util.Map[String, java.util.List[CounterMutation]]]) = {
    log.debug("batch_add(%s, %s, %s", keyspace, mutations, writeConsistency.level)
    provider.map { _.batch_add(mutations, writeConsistency.level) }
  }

  private def getSlice[K, N, V](key: K,
                                pred: thrift.SlicePredicate,
                                keyCodec: Codec[K], nameCodec: Codec[N]): Future[Map[N,CounterColumn[N]]] = {
    val cp = new thrift.ColumnParent(name)
    log.debug("get_counter_slice(%s, %s, %s, %s, %s)", keyspace, key, cp, pred, readConsistency.level)
    provider.map { _.get_counter_slice(keyCodec.encode(key), cp, pred, readConsistency.level) }
      .map { result =>
        val cols: Map[N,CounterColumn[N]] = new HashMap(result.size)
        for (counter <- result.iterator) {
          val col = CounterColumn.convert(nameCodec, counter)
          cols.put(col.name, col)
        }
        cols
      }
  }

  def encodeSet[V](values: Set[V])(implicit codec: Codec[V]): List[ByteBuffer] = {
    val output = new ArrayList[ByteBuffer](values.size)
    for (value <- asScalaIterable(values))
      output.add(codec.encode(value))
    output
  }
}

private[cassie] object CounterColumnFamily
{
  val EMPTY = ByteBuffer.allocate(0)
}
