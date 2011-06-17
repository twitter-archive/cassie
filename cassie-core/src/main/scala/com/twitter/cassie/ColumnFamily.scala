package com.twitter.cassie

import clocks.{MicrosecondEpochClock, Clock}
import codecs.{Codec}
import connection.ClientProvider

import org.apache.cassandra.finagle.thrift
import com.twitter.logging.Logger
import java.nio.ByteBuffer
import java.util.Collections.{singleton => singletonJSet}
import com.twitter.cassie.util.ByteBufferUtil.EMPTY
import java.util.{ArrayList => JArrayList, HashMap => JHashMap, List => JList,
  Map => JMap, Set => JSet}
import org.apache.cassandra.finagle.thrift
import scala.collection.JavaConversions._ // TODO get rid of this

import com.twitter.util.Future

/**
 * A readable, writable column family with batching capabilities. This is a
 * lightweight object: it inherits a connection pool from the Keyspace. */
case class ColumnFamily[Key, Name, Value](
    keyspace: String,
    name: String,
    provider: ClientProvider,
    keyCodec: Codec[Key],
    nameCodec: Codec[Name],
    valueCodec: Codec[Value],
    readConsistency: ReadConsistency = ReadConsistency.Quorum,
    writeConsistency: WriteConsistency = WriteConsistency.Quorum
  ) extends ColumnFamilyLike[Key, Name, Value] {

  private[cassie] var clock: Clock = MicrosecondEpochClock
  val log: Logger = Logger.get

  def keysAs[K](codec: Codec[K]): ColumnFamily[K, Name, Value] = copy(keyCodec = codec)
  def namesAs[N](codec: Codec[N]): ColumnFamily[Key, N, Value] = copy(nameCodec = codec)
  def valuesAs[V](codec: Codec[V]): ColumnFamily[Key, Name, V] = copy(valueCodec = codec)
  def consistency(rc: ReadConsistency) = copy(readConsistency = rc)
  def consistency(wc: WriteConsistency) = copy(writeConsistency = wc)

  def newColumn[N, V](n: N, v: V) = Column(n, v)
  def newColumn[N, V](n: N, v: V, ts: Long) = new Column(n, v, Some(ts), None)

  def getColumn(key: Key,
                columnName: Name): Future[Option[Column[Name, Value]]] = {
    getColumns(key, singletonJSet(columnName)).map { result => Option(result.get(columnName))}
  }

  def getRow(key: Key): Future[JMap[Name, Column[Name, Value]]] = {
    getRowSlice(key, None, None, Int.MaxValue, Order.Normal)
  }

  def getRowSlice(key: Key,
                  startColumnName: Option[Name],
                  endColumnName: Option[Name],
                  count: Int,
                  order: Order): Future[JMap[Name, Column[Name, Value]]] = {
    val startBytes = startColumnName.map { c => nameCodec.encode(c) }.getOrElse(EMPTY)
    val endBytes = endColumnName.map { c => nameCodec.encode(c) }.getOrElse(EMPTY)
    val pred = new thrift.SlicePredicate()
    pred.setSlice_range(new thrift.SliceRange(startBytes, endBytes, order.reversed, count))
    getSlice(key, pred, keyCodec, nameCodec, valueCodec)
  }

  def getColumns(key: Key, columnNames: JSet[Name]): Future[JMap[Name, Column[Name, Value]]] = {
    val pred = new thrift.SlicePredicate()
    pred.setColumn_names(encodeNames(columnNames))
    getSlice(key, pred, keyCodec, nameCodec, valueCodec)
  }

  def multigetColumn(keys: JSet[Key], columnName: Name): Future[JMap[Key, Column[Name, Value]]] = {
    multigetColumns(keys, singletonJSet(columnName)).map { rows =>
      val cols: JMap[Key, Column[Name, Value]] = new JHashMap(rows.size)
      for (rowEntry <- asScalaIterable(rows.entrySet))
        if (!rowEntry.getValue.isEmpty)
          cols.put(rowEntry.getKey, rowEntry.getValue.get(columnName))
      cols
    }
  }

  def multigetColumns(keys: JSet[Key], columnNames: JSet[Name]) = {
    val cp = new thrift.ColumnParent(name)
    val pred = new thrift.SlicePredicate()
    pred.setColumn_names(encodeNames(columnNames))
    log.debug("multiget_slice(%s, %s, %s, %s, %s)", keyspace, keys, cp, pred, readConsistency.level)
    val encodedKeys = encodeKeys(keys)
    provider.map {
      _.multiget_slice(encodedKeys, cp, pred, readConsistency.level)
    }.map { result =>
      // decode result
      val rows: JMap[Key, JMap[Name, Column[Name, Value]]] = new JHashMap(result.size)
      for (rowEntry <- asScalaIterable(result.entrySet)) {
        val cols: JMap[Name, Column[Name, Value]] = new JHashMap(rowEntry.getValue.size)
        for (cosc <- asScalaIterable(rowEntry.getValue)) {
          val col = Column.convert(nameCodec, valueCodec, cosc)
          cols.put(col.name, col)
        }
        rows.put(keyCodec.decode(rowEntry.getKey), cols)
      }
      rows
    }
  }

  def insert(key: Key, column: Column[Name, Value]) = {
    val cp = new thrift.ColumnParent(name)
    val col = Column.convert(nameCodec, valueCodec, clock, column)
    log.debug("insert(%s, %s, %s, %s, %d, %s)", keyspace, key, cp, column.value,
      col.timestamp, writeConsistency.level)
    provider.map {
      _.insert(keyCodec.encode(key), cp, col, writeConsistency.level)
    }
  }

  def truncate() = provider.map(_.truncate(name))

  def removeColumn(key: Key, columnName: Name) = {
    val cp = new thrift.ColumnPath(name)
    val timestamp = clock.timestamp
    cp.setColumn(nameCodec.encode(columnName))
    log.debug("remove(%s, %s, %s, %d, %s)", keyspace, key, cp, timestamp, writeConsistency.level)
    provider.map { _.remove(keyCodec.encode(key), cp, timestamp, writeConsistency.level) }
  }

  def removeColumns(key: Key, columnNames: JSet[Name]): Future[Void] = {
    batch()
      .removeColumns(key, columnNames)
      .execute()
  }

  def removeColumns(key: Key, columnNames: JSet[Name], timestamp: Long): Future[Void] = {
    batch()
      .removeColumns(key, columnNames, timestamp)
      .execute()
  }

  def removeRow(key: Key) = {
    removeRowWithTimestamp(key, clock.timestamp)
  }

  def removeRowWithTimestamp(key: Key, timestamp: Long) = {
    val cp = new thrift.ColumnPath(name)
    log.debug("remove(%s, %s, %s, %d, %s)", keyspace, key, cp, timestamp, writeConsistency.level)
    provider.map { _.remove(keyCodec.encode(key), cp, timestamp, writeConsistency.level) }
  }

  def batch() = new BatchMutationBuilder(this)

  private[cassie] def batch(mutations: JMap[ByteBuffer, JMap[String, JList[thrift.Mutation]]]) = {
    log.debug("batch_mutate(%s, %s, %s", keyspace, mutations, writeConsistency.level)
    provider.map { _.batch_mutate(mutations, writeConsistency.level) }
  }

  def rowIteratee(batchSize: Int): ColumnIteratee[Key, Name, Value] = {
    val pred = new thrift.SlicePredicate
    pred.setSlice_range(new thrift.SliceRange(EMPTY, EMPTY, false, Int.MaxValue))
    new ColumnIteratee(this, EMPTY, EMPTY, batchSize, pred, keyCodec, nameCodec, valueCodec)
  }

  def columnIteratee(batchSize: Int,
                     columnName: Name): ColumnIteratee[Key, Name, Value] =
    columnsIteratee(batchSize, singletonJSet(columnName))

  def columnsIteratee(batchSize: Int, columnNames: JSet[Name]): ColumnIteratee[Key, Name, Value] = {
    val pred = new thrift.SlicePredicate
    pred.setColumn_names(encodeNames(columnNames))
    new ColumnIteratee(this, EMPTY, EMPTY, batchSize, pred, keyCodec, nameCodec, valueCodec)
  }

  private def getSlice[K, N, V](key: K,
                                pred: thrift.SlicePredicate,
                                keyCodec: Codec[K], nameCodec: Codec[N], valueCodec: Codec[V]): Future[JMap[N,Column[N,V]]] = {
    val cp = new thrift.ColumnParent(name)
    log.debug("get_slice(%s, %s, %s, %s, %s)", keyspace, key, cp, pred, readConsistency.level)
    provider.map { _.get_slice(keyCodec.encode(key), cp, pred, readConsistency.level) }
      .map { result =>
        val cols: JMap[N,Column[N,V]] = new JHashMap(result.size)
        for (cosc <- result.iterator) {
          val col = Column.convert(nameCodec, valueCodec, cosc)
          cols.put(col.name, col)
        }
        cols
      }
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
    provider.map { _.get_range_slices(cp, predicate, range, readConsistency.level) }
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