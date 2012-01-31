package com.twitter.cassie

import com.twitter.cassie.clocks.{ MicrosecondEpochClock, Clock }
import com.twitter.cassie.codecs.{ ThriftCodec, Codec }
import com.twitter.cassie.connection.ClientProvider
import com.twitter.cassie.util.ByteBufferUtil.EMPTY
import com.twitter.cassie.util.FutureUtil.timeFutureWithFailures
import com.twitter.finagle.stats.{ StatsReceiver, NullStatsReceiver }
import com.twitter.logging.Logger
import com.twitter.util.Future
import java.nio.ByteBuffer
import java.util.Collections.{ singleton => singletonJSet }
import java.util.{ ArrayList => JArrayList, HashMap => JHashMap, Iterator => JIterator, List => JList, Map => JMap, Set => JSet }
import org.apache.cassandra.finagle.thrift
import scala.collection.JavaConversions._

/**
 * A readable, writable column family with batching capabilities. This is a
 * lightweight object: it inherits a connection pool from the Keyspace.
 *
 * TODO: figure out how to get rid of code duplication vs non counter columns
 */
object CounterColumnFamily {
  private val log = Logger.get(this.getClass)
}
case class CounterColumnFamily[Key, Name](
  keyspace: String,
  name: String,
  provider: ClientProvider,
  keyCodec: Codec[Key],
  nameCodec: Codec[Name],
  stats: StatsReceiver = NullStatsReceiver,
  readConsistency: ReadConsistency = ReadConsistency.Quorum,
  writeConsistency: WriteConsistency = WriteConsistency.One
) extends BaseColumnFamily(keyspace, name, provider, stats) {

  import CounterColumnFamily._
  import BaseColumnFamily._

  private[cassie] var clock: Clock = MicrosecondEpochClock

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
   * @param the name of the column
   */
  def getColumn(key: Key,
    columnName: Name): Future[Option[CounterColumn[Name]]] = {
    getColumns(key, singletonJSet(columnName)).map { result =>
      Option(result.get(columnName))
    }
  }

  /**
   * Results in a map of all column names to the columns for a given key by slicing over a whole row.
   *   If your rows contain a huge number of columns, this will be slow and horrible and you will hate your ife.
   * @return a future that can contain [[org.apache.cassandra.finagle.thrift.TimedOutException]],
   *  [[org.apache.cassandra.finagle.thrift.UnavailableException]] or [[org.apache.cassandra.finagle.thrift.InvalidRequestException]]
   * @param key the row's key
   */
  def getRow(key: Key): Future[JMap[Name, CounterColumn[Name]]] = {
    val pred = sliceRangePredicate(None, None, Order.Normal, Int.MaxValue)
    getMapSlice(key, pred)
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
   * @param order sort forward or reverse (by column name)
   */
  def getRowSlice(
    key: Key,
    start: Option[Name],
    end: Option[Name],
    count: Int,
    order: Order = Order.Normal): Future[Seq[CounterColumn[Name]]] = {
    try {
      val pred = sliceRangePredicate(start, end, order, count)
      getOrderedSlice(key, pred)
    } catch {
      case e => Future.exception(e)
    }
  }

  private[cassie] def getOrderedSlice(key: Key, pred: thrift.SlicePredicate) = {
    val cp = new thrift.ColumnParent(name)
    val keyEncoded = keyCodec.encode(key)
    log.debug("get_slice(%s, %s, %s, %s, %s)", keyspace, keyEncoded, cp, pred, readConsistency.level)
    withConnection("get_slice", Map("key" -> keyEncoded, "predicate" -> annPredCodec.encode(pred),
      "readconsistency" -> readConsistency.level.toString)
    ) {
        _.get_slice(keyEncoded, cp, pred, readConsistency.level)
      } map { result =>
        result.map { cosc =>
          CounterColumn.convert(nameCodec, cosc)
        }
      }
  }

  private[cassie] def getMapSlice(key: Key, pred: thrift.SlicePredicate) = {
    val cp = new thrift.ColumnParent(name)
    log.debug("get_slice(%s, %s, %s, %s, %s)", keyspace, key, cp, pred, readConsistency.level)
    getOrderedSlice(key, pred) map { result =>
      val cols: JMap[Name, CounterColumn[Name]] = new JHashMap(result.size)
      for (col <- result) {
        cols.put(col.name, col)
      }
      cols
    }
  }

  /**
   * Get a selection of columns from a single row.
   * @return a future that can contain [[org.apache.cassandra.finagle.thrift.TimedOutException]],
   *   [[org.apache.cassandra.finagle.thrift.UnavailableException]] or [[org.apache.cassandra.finagle.thrift.InvalidRequestException]]
   * @param key the row key
   * @param the column names you want
   */
  def getColumns(key: Key,
    columnNames: JSet[Name]): Future[JMap[Name, CounterColumn[Name]]] = {
    try {
      val pred = new thrift.SlicePredicate().setColumn_names(nameCodec.encodeSet(columnNames))
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
   * @param the column name
   */
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
   * @param columnNames the column names
   */
  def multigetColumns(keys: JSet[Key], columnNames: JSet[Name]) = {
    try {
      val pred = new thrift.SlicePredicate().setColumn_names(nameCodec.encodeSet(columnNames))
      multigetSlice(keys, pred)
    } catch {
      case e => Future.exception(e)
    }
  }

  private def multigetSlice(keys: JSet[Key], pred: thrift.SlicePredicate): Future[JMap[Key, JMap[Name, CounterColumn[Name]]]] = {
    val cp = new thrift.ColumnParent(name)
    val encodedKeys = keyCodec.encodeSet(keys)
    log.debug("multiget_slice(%s, %s, %s, %s, %s)", keyspace, encodedKeys, cp, pred, readConsistency.level)
    withConnection("multiget_slice", Map("keys" -> encodedKeys, "predicate" -> annPredCodec.encode(pred),
      "readconsistency" -> readConsistency.level.toString)
    ) {
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

  /**
   * Get multiple whole rows.
   *
   * @return a future that can contain [[org.apache.cassandra.finagle.thrift.TimedOutException]],
   *  [[org.apache.cassandra.finagle.thrift.UnavailableException]] or [[org.apache.cassandra.finagle.thrift.InvalidRequestException]]
   * @param keys the row keys
   * @param startColumn An optional starting column. If None it starts at the first column.
   * @param endColumn An optional ending column. If None it ends at the last column.
   * @param count Like LIMIT in SQL. Note that all of start..end will be loaded into memory serverside.
   * @param order sort forward or reverse (by column name)
   */
  def multigetRows(keys: JSet[Key], startColumn: Option[Name], endColumn: Option[Name], order: Order,
    count: Int): Future[JMap[Key, JMap[Name, CounterColumn[Name]]]] = {
    val pred = sliceRangePredicate(startColumn, endColumn, order, count)
    multigetSlice(keys, pred)
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
   * Increments a column.
   */
  def add(key: Key, column: CounterColumn[Name]) = {
    try {
      val cp = new thrift.ColumnParent(name)
      val col = CounterColumn.convert(nameCodec, column)
      val keyEncoded = keyCodec.encode(key)
      log.debug("add(%s, %s, %s, %d, %s)", keyspace, keyEncoded, cp, column.value, writeConsistency.level)
      withConnection("add", Map("key" -> keyEncoded, "column" -> col.name, "readconsistency" -> writeConsistency.toString)) {
        _.add(keyEncoded, cp, col, writeConsistency.level)
      }
    } catch {
      case e => Future.exception(e)
    }
  }

  /**
   * Remove a single column. Note that deleting counter columns and then re-adding them is undefined
   * behavior in Cassandra.
   * @return a future that can contain [[org.apache.cassandra.finagle.thrift.TimedOutException]],
   *  [[org.apache.cassandra.finagle.thrift.UnavailableException]] or [[org.apache.cassandra.finagle.thrift.InvalidRequestException]]
   * @param key the row key
   * @param columnName the column's name
   */
  def removeColumn(key: Key, columnName: Name) = {
    try {
      val cp = new thrift.ColumnPath(name)
      cp.setColumn(nameCodec.encode(columnName))
      val keyEncoded = keyCodec.encode(key)
      log.debug("remove_counter(%s, %s, %s, %s)", keyspace, keyEncoded, cp, writeConsistency.level)
      withConnection("remove_counter", Map("key" -> keyEncoded, "readconsistency" -> readConsistency.toString)) {
        _.remove_counter(keyEncoded, cp, writeConsistency.level)
      }
    } catch {
      case e => Future.exception(e)
    }
  }

  /**
   * Remove a set of columns from a single row via a batch mutation. Note that deleting counter
   * columns and then re-adding them is undefined behavior in Cassandra.
   *
   * @return a future that can contain [[org.apache.cassandra.finagle.thrift.TimedOutException]],
   *  [[org.apache.cassandra.finagle.thrift.UnavailableException]] or [[org.apache.cassandra.finagle.thrift.InvalidRequestException]]
   * @param key the row key
   * @param columnNames the names of the columns to be deleted
   */
  def removeColumns(key: Key, columnNames: JSet[Name]) = {
    batch()
      .removeColumns(key, columnNames)
      .execute()
  }

  /**
   * Remove an entire row. Note that deleting counter columns and then re-adding them is undefined
   * behavior in Cassandra.
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
    val keyEncoded = keyCodec.encode(key)
    log.debug("remove(%s, %s, %s, %d, %s)", keyspace, keyEncoded, cp, timestamp, writeConsistency.level)
    withConnection("remove", Map("key" -> keyEncoded, "timestamp" -> timestamp.toString, "writeconsistency" -> writeConsistency.toString)) {
      _.remove(keyEncoded, cp, timestamp, writeConsistency.level)
    }
  }

  /**
   * Truncates this column family.
   *
   * @return a Future that can contain [[org.apache.cassandra.finagle.thrift.UnavailableException]]
   *   or [[org.apache.cassandra.finagle.thrift.InvalidRequestException]]
   */
  def truncate() = timeFutureWithFailures(stats, "truncate") {
    withConnection("trace") {
      _.truncate(name)
    }
  }

  /**
   * @return A Builder that can be used to execute multiple actions in a single
   * request.
   */
  def batch() = new CounterBatchMutationBuilder(this)

  private[cassie] def batch(mutations: JMap[ByteBuffer, JMap[String, JList[thrift.Mutation]]]) = {
    log.debug("batch_mutate(%s, %s, %s)", keyspace, mutations, writeConsistency.level)
    withConnection("batch_mutate") {
      _.batch_mutate(mutations, writeConsistency.level)
    }
  }

  /**
   * Prepare asynchronous iteration over a range of rows. Call foreach to start.
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
    CounterRowsIteratee(this, start, end, batchSize, sliceRangePredicate(columnNames))
  }

  /**
   * Start asynchronous iteration through a range of rows.
   *
   * @return RowsIteratee with iterates over all rows in the CF
   * @param batchSize the number of rows to load at a time
   */
  def rowsIteratee(batchSize: Int): CounterRowsIteratee[Key, Name] = {
    val pred = sliceRangePredicate(None, None, Order.Normal, Int.MaxValue)
    CounterRowsIteratee(this, batchSize, pred)
  }

  /**
   * Start asynchronous iteration throw a range of rows, grabbing a single column from each.
   *
   * @return RowsIteratee
   * @param batchSize the number of rows to load at a time
   * @param columnName the name of the column to load
   */
  def rowsIteratee(batchSize: Int, columnName: Name): CounterRowsIteratee[Key, Name] =
    rowsIteratee(batchSize, singletonJSet(columnName))

  /**
   * Start asynchronous iteration throw a range of rows, grabbing a set of columns.
   *
   * @return RowsIteratee that walks the columns for all rows
   * @param batchSize the number of rows to load at once
   * @param columnNames the columns to lead from each row
   */
  def rowsIteratee(batchSize: Int, columnNames: JSet[Name]): CounterRowsIteratee[Key, Name] = {
    CounterRowsIteratee(this, batchSize, sliceRangePredicate(columnNames))
  }

  /**
   * Start asynchronous iteration over all the columns in a row.
   *
   * @param key the row key to walk
   */
  def columnsIteratee(key: Key): CounterColumnsIteratee[Key, Name] = {
    columnsIteratee(100, key)
  }

  /**
   * Start asynchronous iteration over all the columns in a row.
   *
   * @param batchSize the number of columns to load at once
   * @param key the row key to walk
   */
  def columnsIteratee(batchSize: Int, key: Key): CounterColumnsIteratee[Key, Name] = {
    columnsIteratee(batchSize, key, None, None)
  }

  /**
   * Start asynchronous iteration over a range of the columns in a row.
   *
   * @param batchSize the number of columns to load at once
   * @param key the row key to walk
   * @param start start walking at this column in the row
   * @param end end walk at this column in the row
   */
  def columnsIteratee(batchSize: Int, key: Key, start: Option[Name],
    end: Option[Name]): CounterColumnsIteratee[Key, Name] = {
    columnsIteratee(batchSize, key, start, end, Int.MaxValue)
  }

  /**
   * Start asynchronous iteration over a range of the columns in a row.
   *
   * @param batchSize the number of columns to load at once
   * @param key the row key to walk
   * @param start start walking at this column in the row
   * @param end end walk at this column in the row
   * @param limit only return this many columns
   * @param order get columns in this order
   */
  def columnsIteratee(batchSize: Int, key: Key, start: Option[Name],
    end: Option[Name], limit: Int, order: Order = Order.Normal): CounterColumnsIteratee[Key, Name] = {
    CounterColumnsIteratee(this, key, start, end, batchSize, limit, order)
  }

  private def getSlice(key: Key, pred: thrift.SlicePredicate) = {
    val cp = new thrift.ColumnParent(name)
    val keyEncoded = keyCodec.encode(key)
    log.debug("get_counter_slice(%s, %s, %s, %s, %s)", keyspace, keyEncoded, cp, pred, readConsistency.level)
    withConnection("get_slice", Map("key" -> keyEncoded, "predicate" -> annPredCodec.encode(pred),
      "readconsistency" -> readConsistency.toString)) {
      _.get_slice(keyEncoded, cp, pred, readConsistency.level)
    } map { result =>
      val cols: JMap[Name, CounterColumn[Name]] = new JHashMap(result.size)
      for (c <- result.iterator) {
        val col = CounterColumn.convert(nameCodec, c.getCounter_column)
        cols.put(col.name, col)
      }
      cols
    }
  }

  private[cassie] def getRangeSlice(
    startKey: Key,
    endKey: Key,
    count: Int,
    predicate: thrift.SlicePredicate) = {
    val cp = new thrift.ColumnParent(name)
    val startKeyEncoded = keyCodec.encode(startKey)
    val endKeyEncoded = keyCodec.encode(endKey)
    val range = new thrift.KeyRange(count).setStart_key(startKeyEncoded).setEnd_key(endKeyEncoded)
    log.debug("get_range_slices(%s, %s, %s, %s, %s)", keyspace, cp, predicate, range, readConsistency.level)
    withConnection("get_range_slices", Map("startkey" -> startKeyEncoded, "endkey" -> endKeyEncoded,
      "count" -> count.toString, "predicate" -> annPredCodec.encode(predicate),
      "readconsistency" -> readConsistency.level.toString)) {

      _.get_range_slices(cp, predicate, range, readConsistency.level)
    } map { slices =>
      val buf: JList[(Key, JList[CounterColumn[Name]])] = new JArrayList[(Key, JList[CounterColumn[Name]])](slices.size)
      slices.foreach { ks =>
        val key = keyCodec.decode(ks.key)
        val cols = new JArrayList[CounterColumn[Name]](ks.columns.size)
        ks.columns.foreach { col =>
          cols.add(CounterColumn.convert(nameCodec, col))
        }
        buf.add((key, cols))
      }
      buf
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
