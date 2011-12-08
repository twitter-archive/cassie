package com.twitter.cassie

import com.twitter.cassie.clocks.{ MicrosecondEpochClock, Clock }
import com.twitter.cassie.codecs.{ ThriftCodec, Codec }
import com.twitter.cassie.connection.ClientProvider
import com.twitter.cassie.util.FutureUtil.timeFutureWithFailures

import org.apache.cassandra.finagle.thrift
import com.twitter.logging.Logger
import java.nio.ByteBuffer
import java.util.Collections.{ singleton => singletonJSet }
import com.twitter.cassie.util.ByteBufferUtil.EMPTY
import java.util.{
  ArrayList => JArrayList,
  HashMap => JHashMap,
  List => JList,
  Map => JMap,
  Set => JSet
}
import org.apache.cassandra.finagle.thrift
import scala.collection.JavaConversions._ // TODO get rid of this
import com.twitter.finagle.tracing.Trace

import com.twitter.util.Future
import com.twitter.finagle.stats.StatsReceiver

/**
 * A readable, writable column family with batching capabilities. This is a
 * lightweight object: it inherits a connection pool from the Keyspace.
 */
case class ColumnFamily[Key, Name, Value](
  keyspace: String,
  name: String,
  provider: ClientProvider,
  keyCodec: Codec[Key],
  nameCodec: Codec[Name],
  valueCodec: Codec[Value],
  stats: StatsReceiver,
  readConsistency: ReadConsistency = ReadConsistency.Quorum,
  writeConsistency: WriteConsistency = WriteConsistency.Quorum) {

  /**
   * This is necessary to create cglib proxies of column families.
   */
  protected def this() = this(null, null, null, null, null, null, null)

  private[cassie] var clock: Clock = MicrosecondEpochClock
  private[cassie] val annPredCodec =
    new ThriftCodec[thrift.SlicePredicate](classOf[thrift.SlicePredicate])
  val log: Logger = Logger.get

  /**
   * @return a copy of this [[ColumnFamily]] with a different key codec
   * @param codec new key codec
   */
  def keysAs[K](codec: Codec[K]): ColumnFamily[K, Name, Value] = copy(keyCodec = codec)

  /**
   * @return a copy of this [[ColumnFamily]] with a different name codec
   * @param codec new name codec
   */
  def namesAs[N](codec: Codec[N]): ColumnFamily[Key, N, Value] = copy(nameCodec = codec)

  /**
   * @return a copy of this [[ColumnFamilyLike]] with a different value codec
   * @param codec the new value codec
   */
  def valuesAs[V](codec: Codec[V]): ColumnFamily[Key, Name, V] = copy(valueCodec = codec)

  /**
   * @return a copy of this [[ColumnFamilyLike]] with a different read consistency
   * @param rc the new read consistency level
   */
  def consistency(rc: ReadConsistency) = copy(readConsistency = rc)

  /**
   * @return a copy of this [[ColumnFamilyLike]] with a different write consistency level
   * @param wc the new write consistency level
   */
  def consistency(wc: WriteConsistency) = copy(writeConsistency = wc)

  /**
   * Create a new column for this column family. Useful for java-based users.
   * @param n the column name
   * @param v the column value
   */
  def newColumn[N, V](n: N, v: V) = Column(n, v)

  /**
   * Create a new column for this column family. Useful for java-based users.
   * @param n the column name
   * @param v the column value
   * @param ts the column's timestamp
   */
  def newColumn[N, V](n: N, v: V, ts: Long) = new Column(n, v, Some(ts), None)

  /**
   * Get an individual column from a single row.
   * @return a Future that can contain [[org.apache.cassandra.finagle.thrift.TimedOutException]],
   *  [[org.apache.cassandra.finagle.thrift.UnavailableException]] or
   *  [[org.apache.cassandra.finagle.thrift.InvalidRequestException]]
   * @param key the row's key
   * @param the name of the column
   */
  def getColumn(key: Key,
    columnName: Name): Future[Option[Column[Name, Value]]] = {
    getColumns(key, singletonJSet(columnName)).map { result => Option(result.get(columnName)) }
  }

  /**
   * Results in a map of all column names to the columns for a given key by slicing over a whole
   *  row. If your rows contain a huge number of columns, this will be slow and horrible and you
   *  will hate your life.
   * @return a future that can contain [[org.apache.cassandra.finagle.thrift.TimedOutException]],
   *  [[org.apache.cassandra.finagle.thrift.UnavailableException]] or
   *  [[org.apache.cassandra.finagle.thrift.InvalidRequestException]]
   * @param key the row's key
   */
  def getRow(key: Key): Future[JMap[Name, Column[Name, Value]]] = {
    val pred = sliceRangePredicate(None, None, Order.Normal, Int.MaxValue)
    getMapSlice(key, pred)
  }

  /**
   * Get a slice of a single row, starting at `startColumnName` (inclusive) and continuing
   *  to `endColumnName` (inclusive). ordering is determined by the server.
   * @return a Future that can contain [[org.apache.cassandra.finagle.thrift.TimedOutException]],
   *  [[org.apache.cassandra.finagle.thrift.UnavailableException]] or
   *  [[org.apache.cassandra.finagle.thrift.InvalidRequestException]]
   * @param key the row's key
   * @param startColumnName An optional starting column. If None it starts at the first column.
   * @param endColumnName An optional ending column. If None it ends at the last column.
   * @param count Like LIMIT in SQL. Note that all of start..end will be loaded into memory serverside.
   * @param order sort forward or reverse (by column name)
   */
  def getRowSlice(key: Key,
    startColumnName: Option[Name],
    endColumnName: Option[Name],
    count: Int,
    order: Order = Order.Normal): Future[Seq[Column[Name, Value]]] = {
    try {
      val pred = sliceRangePredicate(startColumnName, endColumnName, order, count)
      getOrderedSlice(key, pred)
    } catch {
      case e => Future.exception(e)
    }
  }

  /**
   * Get a set of columns from a single row.
   *
   * @return a Future that can contain [[org.apache.cassandra.finagle.thrift.TimedOutException]],
   *   [[org.apache.cassandra.finagle.thrift.UnavailableException]] or
   *   [[org.apache.cassandra.finagle.thrift.InvalidRequestException]]
   * @param key the row key
   * @param the column names you want
   */
  def getColumns(key: Key, columnNames: JSet[Name]): Future[JMap[Name, Column[Name, Value]]] = {
    try {
      val pred = new thrift.SlicePredicate().setColumn_names(nameCodec.encodeSet(columnNames))
      getMapSlice(key, pred)
    } catch {
      case e => Future.exception(e)
    }
  }

  /**
   * Get a single column from multiple rows.
   *
   * @return a future that can contain [[org.apache.cassandra.finagle.thrift.TimedOutException]],
   *  [[org.apache.cassandra.finagle.thrift.UnavailableException]] or
   *  [[org.apache.cassandra.finagle.thrift.InvalidRequestException]].
   * @param keys the row keys
   * @param the column name
   */
  def multigetColumn(keys: JSet[Key], columnName: Name): Future[JMap[Key, Column[Name, Value]]] = {
    multigetColumns(keys, singletonJSet(columnName)).map { rows =>
      val cols: JMap[Key, Column[Name, Value]] = new JHashMap(rows.size)
      for (rowEntry <- asScalaIterable(rows.entrySet))
        if (!rowEntry.getValue.isEmpty)
          cols.put(rowEntry.getKey, rowEntry.getValue.get(columnName))
      cols
    }
  }

  /**
   * Get multiple columns from multiple rows.
   *
   * @return a future that can contain [[org.apache.cassandra.finagle.thrift.TimedOutException]],
   *  [[org.apache.cassandra.finagle.thrift.UnavailableException]] or [[org.apache.cassandra.finagle.thrift.InvalidRequestException]]
   * @param keys the row keys
   * @param columnNames the column names
   */
  def multigetColumns(keys: JSet[Key], columnNames: JSet[Name]) = {
    val pred = sliceRangePredicate(columnNames)
    multiget(keys, pred)
  }


  /**
   * Get multiple whole rows.
   *
   * @return a future that can contain [[org.apache.cassandra.finagle.thrift.TimedOutException]],
   *  [[org.apache.cassandra.finagle.thrift.UnavailableException]] or [[org.apache.cassandra.finagle.thrift.InvalidRequestException]]
   * @param keys the row keys
   * @param startColumnName An optional starting column. If None it starts at the first column.
   * @param endColumnName An optional ending column. If None it ends at the last column.
   * @param count Like LIMIT in SQL. Note that all of start..end will be loaded into memory serverside.
   * @param order sort forward or reverse (by column name)
   */
  def multigetRows(keys: JSet[Key], startColumnName: Option[Name], endColumnName: Option[Name], order: Order, count: Int) = {
    val pred = sliceRangePredicate(startColumnName, endColumnName, order, count)
    multiget(keys, pred)
  }

  private[cassie] def multiget(keys: JSet[Key], pred: thrift.SlicePredicate) = {
    try {
      val cp = new thrift.ColumnParent(name)
      log.debug("multiget_slice(%s, %s, %s, %s, %s)", keyspace, keys, cp, pred, readConsistency.level)
      timeFutureWithFailures(stats, "multiget_slice") {
        Trace.recordBinary("cassie.keyspace", keyspace)
        Trace.recordBinary("cassie.columnfamily", name)
        Trace.recordBinary("cassie.predicate", annPredCodec.encode(pred))
        Trace.recordBinary("cassie.readconsistency", readConsistency.level.toString)
        provider.map {
          _.multiget_slice(keyCodec.encodeSet(keys), cp, pred, readConsistency.level)
        }.map { result =>
          val rows: JMap[Key, JMap[Name, Column[Name, Value]]] = new JHashMap(result.size)
          for (rowEntry <- asScalaIterable(result.entrySet)) {
            val cols = new JHashMap[Name, Column[Name, Value]](rowEntry.getValue.size)
            for (cosc <- asScalaIterable(rowEntry.getValue)) {
              val col = Column.convert(nameCodec, valueCodec, cosc)
              cols.put(col.name, col)
            }
            rows.put(keyCodec.decode(rowEntry.getKey), cols)
          }
          rows
        }
      }
    } catch {
      case e => Future.exception(e)
    }
  }

  /**
   * Insert a single column.
   *
   * @return a Future that can contain [[org.apache.cassandra.finagle.thrift.TimedOutException]],
   *  [[org.apache.cassandra.finagle.thrift.UnavailableException]] or
   *  [[org.apache.cassandra.finagle.thrift.InvalidRequestException]]
   * @param key the row key
   * @param column the column
   */
  def insert(key: Key, column: Column[Name, Value]) = {
    try {
      val cp = new thrift.ColumnParent(name)
      val col = Column.convert(nameCodec, valueCodec, clock, column)
      log.debug("insert(%s, %s, %s, %s, %d, %s)", keyspace, key, cp, column.value,
        col.timestamp, writeConsistency.level)
      timeFutureWithFailures(stats, "insert") {
        provider.map {
          val keyEncoded = keyCodec.encode(key)
          Trace.recordBinary("cassie.keyspace", keyspace)
          Trace.recordBinary("cassie.columnfamily", name)
          Trace.recordBinary("cassie.key", keyEncoded)
          Trace.recordBinary("cassie.column", col.name)
          Trace.recordBinary("cassie.writeconsistency", writeConsistency.level.toString)
          _.insert(keyEncoded, cp, col, writeConsistency.level)
        }
      }
    } catch {
      case e => Future.exception(e)
    }
  }

  /**
   * Truncates this column family.
   *
   * @return a Future that can contain [[org.apache.cassandra.finagle.thrift.UnavailableException]]
   *   or [[org.apache.cassandra.finagle.thrift.InvalidRequestException]]
   */
  def truncate() = timeFutureWithFailures(stats, "truncate") { provider.map(_.truncate(name)) }

  /**
   * Remove a single column.
   *
   * @return a future that can contain [[org.apache.cassandra.finagle.thrift.TimedOutException]],
   *  [[org.apache.cassandra.finagle.thrift.UnavailableException]] or
   *  [[org.apache.cassandra.finagle.thrift.InvalidRequestException]]
   * @param key the row key
   * @param columnName the column's name
   */
  def removeColumn(key: Key, columnName: Name) = {
    try {
      val cp = new thrift.ColumnPath(name).setColumn(nameCodec.encode(columnName))
      val timestamp = clock.timestamp
      log.debug("remove(%s, %s, %s, %d, %s)", keyspace, key, cp, timestamp, writeConsistency.level)
      timeFutureWithFailures(stats, "remove") {
        provider.map {
          val keyEncoded = keyCodec.encode(key)
          Trace.recordBinary("cassie.keyspace", keyspace)
          Trace.recordBinary("cassie.columnfamily", name)
          Trace.recordBinary("cassie.key", keyEncoded)
          Trace.recordBinary("cassie.column", cp.column)
          Trace.recordBinary("cassie.writeconsistency", writeConsistency.level.toString)
          _.remove(keyEncoded, cp, timestamp, writeConsistency.level)
        }
      }
    } catch {
      case e => Future.exception(e)
    }
  }

  /**
   * Remove a set of columns from a single row via a batch mutation.
   *
   * @return a Future that can contain [[org.apache.cassandra.finagle.thrift.TimedOutException]],
   *  [[org.apache.cassandra.finagle.thrift.UnavailableException]] or
   *  [[org.apache.cassandra.finagle.thrift.InvalidRequestException]]
   * @param key the row key
   * @param columnNames the names of the columns to be deleted
   */
  def removeColumns(key: Key, columnNames: JSet[Name]): Future[Void] = {
    batch()
      .removeColumns(key, columnNames)
      .execute()
  }

  /**
   * Remove a set of columns from a single row at a specific timestamp. Useful for replaying data.
   *
   * @return a Future that can contain [[org.apache.cassandra.finagle.thrift.TimedOutException]],
   *  [[org.apache.cassandra.finagle.thrift.UnavailableException]] or
   *  [[org.apache.cassandra.finagle.thrift.InvalidRequestException]]
   * @param key the row key
   * @param columnNames the columns to be deleted
   * @param timestamp the timestamp at which the columns should be deleted
   */
  def removeColumns(key: Key, columnNames: JSet[Name], timestamp: Long): Future[Void] = {
    batch()
      .removeColumns(key, columnNames, timestamp)
      .execute()
  }

  /**
   * Remove an entire row.
   *
   * @return a Future that can contain [[org.apache.cassandra.finagle.thrift.TimedOutException]],
   *  [[org.apache.cassandra.finagle.thrift.UnavailableException]] or
   *  [[org.apache.cassandra.finagle.thrift.InvalidRequestException]]
   * @param key the row key to be deleted
   */
  def removeRow(key: Key) = {
    removeRowWithTimestamp(key, clock.timestamp)
  }

  /**
   * Remove an entire row at the given timestamp.
   * @return a future that can contain [[org.apache.cassandra.finagle.thrift.TimedOutException]],
   *  [[org.apache.cassandra.finagle.thrift.UnavailableException]] or
   *  [[org.apache.cassandra.finagle.thrift.InvalidRequestException]]
   * @param key the row key to be deleted
   * @param timestamp the time at which the row was deleted
   */
  def removeRowWithTimestamp(key: Key, timestamp: Long) = {
    val cp = new thrift.ColumnPath(name)
    log.debug("remove(%s, %s, %s, %d, %s)", keyspace, key, cp, timestamp, writeConsistency.level)
    timeFutureWithFailures(stats, "remove") {
      provider.map {
        val keyEncoded = keyCodec.encode(key)
        Trace.recordBinary("cassie.keyspace", keyspace)
        Trace.recordBinary("cassie.columnfamily", name)
        Trace.recordBinary("cassie.key", keyEncoded)
        Trace.recordBinary("cassie.timestamp", timestamp.toString)
        Trace.recordBinary("cassie.writeconsistency", writeConsistency.level.toString)
        _.remove(keyEncoded, cp, timestamp, writeConsistency.level)
      }
    }
  }

  /**
   * Start a batch operation by returning a new BatchMutationBuilder
   */
  def batch() = new BatchMutationBuilder(this)

  private[cassie] def batch(mutations: JMap[ByteBuffer, JMap[String, JList[thrift.Mutation]]]) = {
    log.debug("batch_mutate(%s, %s, %s", keyspace, mutations, writeConsistency.level)
    timeFutureWithFailures(stats, "batch_mutate") {
      Trace.recordBinary("cassie.keyspace", keyspace)
      Trace.recordBinary("cassie.columnfamily", name)
      Trace.recordBinary("cassie.writeconsistency", writeConsistency.level.toString)
      provider.map {
        _.batch_mutate(mutations, writeConsistency.level)
      }
    }
  }

  /**
   * Start asynchronous iteration over a range of rows.
   * Example:
   *  val future = cf.rowsIteratee("foo", "bar", 100, new JHashSet("asdf", "jkl")).foreach {case (key, columns)
   *    println(key, columns)
   *  }
   *  future.get(1.minute) //timeout
   *
   * @return a RowsIteratee which iterates over all columns of all rows in
   * the column family with the given batch size.
   * @param start the starting key (inclusive)
   * @param end the ending key (exclusive)
   * @param batchSize the number of rows to load at a time
   * @param columnNames the columns to load from each row (like a projection)
   */
  def rowsIteratee(start: Key, end: Key, batchSize: Int, columnNames: JSet[Name]) = {
    RowsIteratee(this, start, end, batchSize, sliceRangePredicate(columnNames))
  }

  /**
   * Start asynchronous iteration through a range of rows.
   *
   * @return RowsIteratee with iterates over all rows in the CF
   * @param batchSize the number of rows to load at a time
   */
  def rowsIteratee(batchSize: Int): RowsIteratee[Key, Name, Value] = {
    val pred = sliceRangePredicate(None, None, Order.Normal, Int.MaxValue)
    RowsIteratee(this, batchSize, pred)
  }

  /**
   * Start asynchronous iteration throw a range of rows, grabbing a single column from each.
   *
   * @return RowsIteratee
   * @param batchSize the number of rows to load at a time
   * @param columnName the name of the column to load
   */
  def rowsIteratee(batchSize: Int,
    columnName: Name): RowsIteratee[Key, Name, Value] =
    rowsIteratee(batchSize, singletonJSet(columnName))

  /**
   * Start asynchronous iteration throw a range of rows, grabbing a set of columns.
   *
   * @return RowsIteratee that walks the columns for all rows
   * @param batchSize the number of rows to load at once
   * @param columnNames the columns to lead from each row
   */
  def rowsIteratee(batchSize: Int, columnNames: JSet[Name]): RowsIteratee[Key, Name, Value] = {
    val pred = sliceRangePredicate(columnNames)
    RowsIteratee(this, batchSize, pred)
  }

  /**
   * Start asynchronous iteration over all the columns in a row.
   *
   * @return ColumnsIteratee
   * @param key the row key to walk
   */
  def columnsIteratee(key: Key): ColumnsIteratee[Key, Name, Value] = {
    columnsIteratee(100, key)
  }

  /**
   * Start asynchronous iteration over all the columns in a row.
   *
   * @return ColumnsIteratee
   * @param batchSize the number of columns to load at once
   * @param key the row key to walk
   */
  def columnsIteratee(batchSize: Int, key: Key): ColumnsIteratee[Key, Name, Value] = {
    columnsIteratee(batchSize, key, None, None)
  }

  /**
   * Start asynchronous iteration over a range of the columns in a row.
   *
   * @return ColumnsIteratee
   * @param batchSize the number of columns to load at once
   * @param key the row key to walk
   * @param start start walking at this column in the row
   * @param end end walk at this column in the row
   */
  def columnsIteratee(batchSize: Int, key: Key, start: Option[Name],
                      end: Option[Name]): ColumnsIteratee[Key, Name, Value] = {
    columnsIteratee(batchSize, key, start, end, Int.MaxValue)
  }

  /**
   * Start asynchronous iteration over a range of the columns in a row.
   *
   * @return ColumnsIteratee
   * @param batchSize the number of columns to load at once
   * @param key the row key to walk
   * @param start start walking at this column in the row
   * @param end end walk at this column in the row
   * @param limit only return this many columns
   * @param order get columns in this order
   */
  def columnsIteratee(batchSize: Int, key: Key, start: Option[Name],
                      end: Option[Name], limit: Int, order: Order = Order.Normal): ColumnsIteratee[Key, Name, Value] = {
    ColumnsIteratee(this, key, start, end, batchSize, limit, order)
  }

  private[cassie] def getMapSlice(key: Key,
    pred: thrift.SlicePredicate): Future[JMap[Name, Column[Name, Value]]] = {
    val cp = new thrift.ColumnParent(name)
    log.debug("get_slice(%s, %s, %s, %s, %s)", keyspace, key, cp, pred, readConsistency.level)
    timeFutureWithFailures(stats, "get_slice") {
      provider.map {
        val keyEncoded = keyCodec.encode(key)
        Trace.recordBinary("cassie.keyspace", keyspace)
        Trace.recordBinary("cassie.columnfamily", name)
        Trace.recordBinary("cassie.key", keyEncoded)
        Trace.recordBinary("cassie.predicate", annPredCodec.encode(pred))
        Trace.recordBinary("cassie.readconsistency", readConsistency.level.toString)
        _.get_slice(keyEncoded, cp, pred, readConsistency.level)
      } map { result =>
        val cols: JMap[Name, Column[Name, Value]] = new JHashMap(result.size)
        for (cosc <- result.iterator) {
          val col = Column.convert(nameCodec, valueCodec, cosc)
          cols.put(col.name, col)
        }
        cols
      }
    }
  }

  private[cassie] def getOrderedSlice(key: Key, pred: thrift.SlicePredicate): Future[Seq[Column[Name, Value]]] = {
    val cp = new thrift.ColumnParent(name)
    log.debug("get_slice(%s, %s, %s, %s, %s)", keyspace, key, cp, pred, readConsistency.level)
    timeFutureWithFailures(stats, "get_slice") {
      provider.map {
        val keyEncoded = keyCodec.encode(key)
        Trace.recordBinary("cassie.keyspace", keyspace)
        Trace.recordBinary("cassie.columnfamily", name)
        Trace.recordBinary("cassie.key", keyEncoded)
        Trace.recordBinary("cassie.predicate", annPredCodec.encode(pred))
        Trace.recordBinary("cassie.readconsistency", readConsistency.level.toString)
        _.get_slice(keyEncoded, cp, pred, readConsistency.level)
      } map { result =>
        result.map { cosc =>
          Column.convert(nameCodec, valueCodec, cosc)
        }
      }
    }
  }

  private[cassie] def getRangeSlice(startKey: Key,
    endKey: Key,
    count: Int,
    predicate: thrift.SlicePredicate) = {

    val cp = new thrift.ColumnParent(name)
    val startKeyEncoded = keyCodec.encode(startKey)
    val endKeyEncoded = keyCodec.encode(endKey)
    val range = new thrift.KeyRange(count).setStart_key(startKeyEncoded).setEnd_key(endKeyEncoded)
    log.debug("get_range_slices(%s, %s, %s, %s, %s)", keyspace, cp, predicate, range, readConsistency.level)
    timeFutureWithFailures(stats, "get_range_slices") {
      provider.map {
        Trace.recordBinary("cassie.keyspace", keyspace)
        Trace.recordBinary("cassie.columnfamily", name)
        Trace.recordBinary("cassie.startkey", startKeyEncoded)
        Trace.recordBinary("cassie.endkey", endKeyEncoded)
        Trace.recordBinary("cassie.count", count.toString)
        Trace.recordBinary("cassie.predicate", annPredCodec.encode(predicate))
        Trace.recordBinary("cassie.readconsistency", readConsistency.level.toString)
        _.get_range_slices(cp, predicate, range, readConsistency.level)
      } map { slices =>
        val buf: JList[(Key, JList[Column[Name, Value]])] = new JArrayList[(Key, JList[Column[Name, Value]])](slices.size)
        slices.foreach { ks =>
          val key = keyCodec.decode(ks.key)
          val cols = new JArrayList[Column[Name, Value]](ks.columns.size)
          ks.columns.foreach { col =>
            cols.add(Column.convert(nameCodec, valueCodec, col))
          }
          buf.add((key, cols))
        }
        buf
      }
    }
  }

  private def sliceRangePredicate(startColumnName: Option[Name], endColumnName: Option[Name], order: Order, count: Int) = {
    val startBytes = startColumnName.map { c => nameCodec.encode(c) }.getOrElse(EMPTY)
    val endBytes = endColumnName.map { c => nameCodec.encode(c) }.getOrElse(EMPTY)
    val pred = new thrift.SlicePredicate()
    pred.setSlice_range(new thrift.SliceRange(startBytes, endBytes, order.reversed, count))
  }

  private def sliceRangePredicate(columnNames: JSet[Name]) = {
    new thrift.SlicePredicate().setColumn_names(nameCodec.encodeSet(columnNames))
  }

}
