package com.twitter.cassie

import com.twitter.cassie.codecs.Codec
import com.twitter.cassie.connection.ClientProvider

import org.apache.cassandra.finagle.thrift
import com.twitter.logging.Logger
import java.nio.ByteBuffer
import java.util.Collections.{singleton => singletonJSet}
import com.twitter.cassie.util.ByteBufferUtil.EMPTY

import java.util.{ArrayList => JArrayList, HashMap => JHashMap,
    Iterator => JIterator, List => JList, Map => JMap, Set => JSet}
import scala.collection.JavaConversions._

import com.twitter.util.Future
import com.twitter.finagle.stats.{StatsReceiver, NullStatsReceiver}


/**
 * A readable, writable column family with batching capabilities. This is a
 * lightweight object: it inherits a connection pool from the Keyspace.
 *
 * TODO: figure out how to get rid of code duplication vs non counter columns
 */
case class CounterColumnFamily[Key, Name](
    keyspace: String,
    name: String,
    provider: ClientProvider,
    keyCodec: Codec[Key],
    nameCodec: Codec[Name],
    stats: StatsReceiver = NullStatsReceiver,
    readConsistency: ReadConsistency = ReadConsistency.Quorum,
    writeConsistency: WriteConsistency = WriteConsistency.One) {

  val log = Logger.get

  def keysAs[K](codec: Codec[K]): CounterColumnFamily[K, Name] = copy(keyCodec = codec)
  def namesAs[N](codec: Codec[N]): CounterColumnFamily[Key, N] = copy(nameCodec = codec)
  def consistency(rc: ReadConsistency) = copy(readConsistency = rc)
  def consistency(wc: WriteConsistency) = copy(writeConsistency = wc)

  /**
   * @Java
   * Creates a new Column.
   */
  def newColumn[N](n: N, v: Long) = CounterColumn(n, v)

  /**
    * Get an individual column from a single row.
    * @return a future that can contain [[org.apache.cassandra.finagle.thrift.TimedOutException]],
    *  [[org.apache.cassandra.finagle.thrift.UnavailableException]] or [[org.apache.cassandra.finagle.thrift.InvalidRequestException]]
    * @param key the row's key
    * @param the name of the column */
  def getColumn(key: Key,
                columnName: Name): Future[Option[CounterColumn[Name]]] = {
    getColumns(key, singletonJSet(columnName)).map {
      result => Option(result.get(columnName))
    }
  }

  /**
    * Results in a map of all column names to the columns for a given key by slicing over a whole row.
    *   If your rows contain a huge number of columns, this will be slow and horrible and you will hate your ife.
    * @return a future that can contain [[org.apache.cassandra.finagle.thrift.TimedOutException]],
    *  [[org.apache.cassandra.finagle.thrift.UnavailableException]] or [[org.apache.cassandra.finagle.thrift.InvalidRequestException]]
    * @param key the row's key */
  def getRow(key: Key): Future[JMap[Name, CounterColumn[Name]]] = {
    getRowSlice(key, None, None, Int.MaxValue, Order.Normal)
  }

  /**
    * Get a slice of a single row, starting at `startColumnName` (inclusive) and continuing to `endColumnName` (inclusive).
    *   ordering is determined by the server.
    * @return a future that can contain [[org.apache.cassandra.finagle.thrift.TimedOutException]],
    *   [[org.apache.cassandra.finagle.thrift.UnavailableException]] or
    *   [[org.apache.cassandra.finagle.thrift.InvalidRequestException]].
    * @param key the row's key
    * @param startColumnName an optional start. if None it starts at the first column
    * @param endColumnName an optional end. if None it ends at the last column
    * @param count like LIMIT in SQL. note that all of start..end will be loaded into memory
    * @param order sort forward or reverse (by column name) */
  def getRowSlice(key: Key,
                  startColumnName: Option[Name],
                  endColumnName: Option[Name],
                  count: Int,
                  order: Order): Future[JMap[Name, CounterColumn[Name]]] = {
    try {
      val startBytes = startColumnName.map { c => nameCodec.encode(c) }.getOrElse(EMPTY)
      val endBytes = endColumnName.map { c => nameCodec.encode(c) }.getOrElse(EMPTY)
      val pred = new thrift.SlicePredicate().setSlice_range(
          new thrift.SliceRange(startBytes, endBytes, order.reversed, count))
      getSlice(key, pred)
    } catch {
      case e => Future.exception(e)
    }
  }

  /**
    * Get a selection of columns from a single row.
    * @return a future that can contain [[org.apache.cassandra.finagle.thrift.TimedOutException]],
    *   [[org.apache.cassandra.finagle.thrift.UnavailableException]] or [[org.apache.cassandra.finagle.thrift.InvalidRequestException]]
    * @param key the row key
    * @param the column names you want */
  def getColumns(key: Key,
                 columnNames: JSet[Name]): Future[JMap[Name, CounterColumn[Name]]] = {
    try {
      val pred = new thrift.SlicePredicate().setColumn_names(encodeNames(columnNames))
      getSlice(key, pred)
    } catch {
      case e => Future.exception(e)
    }
  }

  /**
    * Get a single column from multiple rows.
    * @return a future that can contain [[org.apache.cassandra.finagle.thrift.TimedOutException]],
    *   [[org.apache.cassandra.finagle.thrift.UnavailableException]] or [[org.apache.cassandra.finagle.thrift.InvalidRequestException]].
    * @param keys the row keys
    * @param the column name */
  def multigetColumn(keys: JSet[Key],
                     columnName: Name): Future[JMap[Key, CounterColumn[Name]]] = {
    multigetColumns(keys, singletonJSet(columnName)).map { rows =>
      val cols: JMap[Key, CounterColumn[Name]] = new JHashMap(rows.size)
      for (rowEntry <- asScalaIterable(rows.entrySet))
        if (!rowEntry.getValue.isEmpty) {
          cols.put(rowEntry.getKey, rowEntry.getValue.get(columnName))
        }
      cols
    }
  }

  /**
    * Get multiple columns from multiple rows.
    * @return a future that can contain [[org.apache.cassandra.finagle.thrift.TimedOutException]],
    *  [[org.apache.cassandra.finagle.thrift.UnavailableException]] or [[org.apache.cassandra.finagle.thrift.InvalidRequestException]]
    * @param keys the row keys
    * @param columnNames the column names */
  def multigetColumns(keys: JSet[Key], columnNames: JSet[Name]) = {
    try {
      val pred = new thrift.SlicePredicate().setColumn_names(encodeNames(columnNames))
      multigetSlice(keys, pred)
    } catch {
      case e => Future.exception(e)
    }
  }

  private def multigetSlice(keys: JSet[Key], pred: thrift.SlicePredicate): Future[JMap[Key, JMap[Name, CounterColumn[Name]]]] = {
    val cp = new thrift.ColumnParent(name)
    log.debug("multiget_counter_slice(%s, %s, %s, %s, %s)", keyspace, keys, cp, pred, readConsistency.level)
    val encodedKeys = encodeKeys(keys)
    stats.timeFuture("multiget_slice") {
      provider.map {
        _.multiget_slice(encodedKeys, cp, pred, readConsistency.level)
      }.map { result =>
        val rows: JMap[Key, JMap[Name, CounterColumn[Name]]] = new JHashMap(result.size)
        for (rowEntry <- asScalaIterable(result.entrySet)) {
          val cols: JMap[Name, CounterColumn[Name]] = new JHashMap(rowEntry.getValue.size)
          for (counter <- asScalaIterable(rowEntry.getValue)) {
            val col = CounterColumn.convert(nameCodec, counter.getCounter_column)
            cols.put(col.name, col)
          }
          rows.put(keyCodec.decode(rowEntry.getKey), cols)
        }
        rows
      }
    }
  }

  def multigetSlices(keys: JSet[Key], start: Name, end: Name): Future[JMap[Key, JMap[Name, CounterColumn[Name]]]] = {
    try {
      val pred = sliceRangePredicate(Some(start), Some(end), Order.Normal, Int.MaxValue)
      multigetSlice(keys, pred)
    } catch {
      case e => Future.exception(e)
    }
  }

  /**
    * Increments a column. */
  def add(key: Key, column: CounterColumn[Name]) = {
    try {
      val cp = new thrift.ColumnParent(name)
      val col = CounterColumn.convert(nameCodec, column)
      log.debug("add(%s, %s, %s, %d, %s)", keyspace, key, cp, column.value, writeConsistency.level)
      stats.timeFuture("add") {
        provider.map {
          _.add(keyCodec.encode(key), cp, col, writeConsistency.level)
        }
      }
    } catch {
      case e => Future.exception(e)
    }
  }

  /**
    * Remove a single column.
    * @return a future that can contain [[org.apache.cassandra.finagle.thrift.TimedOutException]],
    *  [[org.apache.cassandra.finagle.thrift.UnavailableException]] or [[org.apache.cassandra.finagle.thrift.InvalidRequestException]]
    * @param key the row key
    * @param columnName the column's name */
  def removeColumn(key: Key, columnName: Name) = {
    try {
      val cp = new thrift.ColumnPath(name)
      cp.setColumn(nameCodec.encode(columnName))
      log.debug("remove_counter(%s, %s, %s, %s)", keyspace, key, cp, writeConsistency.level)
      stats.timeFuture("remove_counter") {
        provider.map {
          _.remove_counter(keyCodec.encode(key), cp, writeConsistency.level)
        }
      }
    } catch {
      case e => Future.exception(e)
    }
  }

  /**
    * Remove a set of columns from a single row via a batch mutation.
    * @return a future that can contain [[org.apache.cassandra.finagle.thrift.TimedOutException]],
    *  [[org.apache.cassandra.finagle.thrift.UnavailableException]] or [[org.apache.cassandra.finagle.thrift.InvalidRequestException]]
    * @param key the row key
    * @param columnNames the names of the columns to be deleted */
  def removeColumns(key: Key, columnNames: JSet[Name]) = {
    batch()
      .removeColumns(key, columnNames)
      .execute()
  }

  /**
    * @return A Builder that can be used to execute multiple actions in a single
    * request. */
  def batch() = new CounterBatchMutationBuilder(this)

  private[cassie] def batch(mutations: JMap[ByteBuffer, JMap[String, JList[thrift.Mutation]]]) = {
    log.debug("batch_mutate(%s, %s, %s)", keyspace, mutations, writeConsistency.level)
    stats.timeFuture("batch_mutate") {
      provider.map {
        _.batch_mutate(mutations, writeConsistency.level)
      }
    }
  }

  private def getSlice(key: Key, pred: thrift.SlicePredicate) = {
    val cp = new thrift.ColumnParent(name)
    log.debug("get_counter_slice(%s, %s, %s, %s, %s)", keyspace, key, cp, pred, readConsistency.level)
    stats.timeFuture("get_slice") {
      provider.map {
        _.get_slice(keyCodec.encode(key), cp, pred, readConsistency.level
      )} map { result =>
        val cols: JMap[Name, CounterColumn[Name]] = new JHashMap(result.size)
        for (c <- result.iterator) {
          val col = CounterColumn.convert(nameCodec, c.getCounter_column)
          cols.put(col.name, col)
        }
        cols
      }
    }
  }

  private def sliceRangePredicate(startColumnName: Option[Name], endColumnName: Option[Name], order: Order, count: Int) = {
    val startBytes = startColumnName.map { c => nameCodec.encode(c) }.getOrElse(EMPTY)
    val endBytes = endColumnName.map { c => nameCodec.encode(c) }.getOrElse(EMPTY)
    val pred = new thrift.SlicePredicate()
    pred.setSlice_range(new thrift.SliceRange(startBytes, endBytes, order.reversed, count))
  }

  def encodeNames(values: JSet[Name]): JList[ByteBuffer] = {
    val output = new JArrayList[ByteBuffer](values.size)
    for (value <- asScalaIterable(values))
      output.add(nameCodec.encode(value))
    output
  }

  def encodeKeys(values: JSet[Key]): JList[ByteBuffer] = {
    val output = new JArrayList[ByteBuffer](values.size)
    for (value <- asScalaIterable(values))
      output.add(keyCodec.encode(value))
    output
  }
}