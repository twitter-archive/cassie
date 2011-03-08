package com.twitter.cassie

import clocks.Clock
import codecs.{Codec, Utf8Codec}
import connection.ClientProvider

import org.apache.cassandra.thrift
import com.twitter.logging.Logger
import java.nio.ByteBuffer
import java.util.Collections.{singleton => singletonSet}

import java.util.{ArrayList, HashMap, Iterator, List, Map, Set}
import org.apache.cassandra.thrift.Mutation
import scala.collection.JavaConversions._

import com.twitter.util.Future

/**
 * A readable, writable column family with batching capabilities. This is a
 * lightweight object: it inherits a connection pool from the Keyspace.
 *
 * TODO: remove (insert/get)As methods in favor of copying the CF to allow for alternate types.
 *
 * @author Ian Ownbey
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
   * Creates a new Column with.
   */
  def newColumn[N](n: N, v: Long) = CounterColumn(n, v)

  /**
   * Returns the optional value of a given column for a given key as the given
   * types.
   */
  def getColumnAs[K, N](key: K,
                           columnName: N)
                          (implicit keyCodec: Codec[K], nameCodec: Codec[N]): Future[Option[CounterColumn[N]]] = {
    getColumnsAs(key, singletonSet(columnName))(keyCodec, nameCodec)
      .map { resmap => Option(resmap.get(columnName)) }
  }

  /**
   * Returns the optional value of a given column for a given key as the default
   * types.
   */
  def getColumn(key: Key,
                columnName: Name): Future[Option[CounterColumn[Name]]] = {
    getColumnAs[Key, Name](key, columnName)(defaultKeyCodec, defaultNameCodec)
  }

  /**
   * Returns a map of all column names to the columns for a given key as the
   * given types. If your rows contain a huge number of columns, this will be
   * slow and horrible.
   */
  def getRowAs[K, N](key: K)
                    (implicit keyCodec: Codec[K], nameCodec: Codec[N]): Future[Map[N, CounterColumn[N]]] = {
    getRowSliceAs[K, N](key, None, None, Int.MaxValue, Order.Normal)(keyCodec, nameCodec)
  }

  /**
   * Returns a map of all column names to the columns for a given key as the
   * default types. If your rows contain a huge number of columns, this will be
   * slow and horrible.
   */
  def getRow(key: Key): Future[Map[Name, CounterColumn[Name]]] = {
    getRowAs[Key, Name](key)(defaultKeyCodec, defaultNameCodec)
  }

  /**
   * Returns a slice of all columns of a row as the given types.
   */
  def getRowSliceAs[K, N](key: K,
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
   * Returns a slice of all columns of a row as the default types.
   */
  def getRowSlice(key: Key,
                  startColumnName: Option[Name],
                  endColumnName: Option[Name],
                  count: Int,
                  order: Order): Future[Map[Name, CounterColumn[Name]]] = {
    getRowSliceAs[Key, Name](key, startColumnName, endColumnName, count, order)(defaultKeyCodec, defaultNameCodec)
  }

  /**
   * Returns a map of the given column names to the columns for a given key as
   * the given types.
   */
  def getColumnsAs[K, N](key: K,
                         columnNames: Set[N])
                         (implicit keyCodec: Codec[K], nameCodec: Codec[N]): Future[Map[N, CounterColumn[N]]] = {
    val pred = new thrift.SlicePredicate()
    pred.setColumn_names(encodeSet(columnNames))
    getSlice(key, pred, keyCodec, nameCodec)
  }

  /**
   * Returns a map of the given column names to the columns for a given key as
   * the default types.
   */
  def getColumns(key: Key,
                 columnNames: Set[Name]): Future[Map[Name, CounterColumn[Name]]] = {
    getColumnsAs[Key, Name](key, columnNames)(defaultKeyCodec, defaultNameCodec)
  }


  /**
   * Returns a map of keys to given column for a set of keys as the given types.
   */
  def multigetColumnAs[K, N](keys: Set[K],
                             columnName: N)
                             (implicit keyCodec: Codec[K], nameCodec: Codec[N]): Future[Map[K, CounterColumn[N]]] = {
    multigetColumnsAs[K, N](keys, singletonSet(columnName))(keyCodec, nameCodec).map { rows =>
      val cols: Map[K, CounterColumn[N]] = new HashMap(rows.size)
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
                     columnName: Name): Future[Map[Key, CounterColumn[Name]]] = {
    multigetColumnAs[Key, Name](keys, columnName)(defaultKeyCodec, defaultNameCodec)
  }

  /**
   * Returns a map of keys to a map of column names to the columns for a given
   * set of keys and columns as the given types.
   *   map<binary,list<Counter>> multiget_counter_slice(1:required list<binary> keys,
                                                   2:required ColumnParent column_parent,
                                                   3:required SlicePredicate predicate,
                                                   4:required ConsistencyLevel consistency_level=ConsistencyLevel.ONE)
      throws (1:InvalidRequestException ire, 2:UnavailableException ue, 3:TimedOutException te),
   */
  @throws(classOf[thrift.TimedOutException])
  @throws(classOf[thrift.UnavailableException])
  @throws(classOf[thrift.InvalidRequestException])
  def multigetColumnsAs[K, N](keys: Set[K],
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
   * Returns a map of keys to a map of column names to the columns for a given
   * set of keys and columns as the default types.
   */
  def multigetColumns(keys: Set[Key], columnNames: Set[Name]) = {
    multigetColumnsAs[Key, Name](keys, columnNames)(defaultKeyCodec, defaultNameCodec)
  }

  /**
   * Inserts a column.
   */
  @throws(classOf[thrift.TimedOutException])
  @throws(classOf[thrift.UnavailableException])
  @throws(classOf[thrift.InvalidRequestException])
  def add(key: Key, column: CounterColumn[Name]) = {
    addAs(key, column)(defaultKeyCodec, defaultNameCodec)
  }

  def addAs[K, N](key: K, column: CounterColumn[N])
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
   * Removes a column from a key.
   */
   @throws(classOf[thrift.TimedOutException])
   @throws(classOf[thrift.UnavailableException])
   @throws(classOf[thrift.InvalidRequestException])
  def removeColumn(key: Key, columnName: Name) = {
    val cp = new thrift.ColumnPath(name)
    cp.setColumn(defaultNameCodec.encode(columnName))
    log.debug("remove(%s, %s, %s, %s)", keyspace, key, cp, writeConsistency.level)
    provider.map { _.remove_counter(defaultKeyCodec.encode(key), cp, writeConsistency.level) }
  }

  /*TODO: IMPLETEMENT
   * Removes a set of columns from a key.
   *
  def removeColumns(key: Key, columnNames: Set[Name]) = {
    batch
      .removeColumns(key, columnNames)
      .execute()
  }   */



  /* TODO:Impletement this
   * @return A Builder that can be used to execute multiple actions in a single
   * request.

  def batch() = new BatchMutationBuilder(this)
  @throws(classOf[thrift.TimedOutException])
  @throws(classOf[thrift.UnavailableException])
  @throws(classOf[thrift.InvalidRequestException])
  private[cassie] def batch(mutations: java.util.Map[ByteBuffer, java.util.Map[String, java.util.List[Mutation]]]) = {
    log.debug("batch_mutate(%s, %s, %s", keyspace, mutations, writeConsistency.level)
    provider.map { _.batch_mutate(mutations, writeConsistency.level) }
  }*/


  @throws(classOf[thrift.TimedOutException])
  @throws(classOf[thrift.UnavailableException])
  @throws(classOf[thrift.InvalidRequestException])
  private def getSlice[K, N, V](key: K,
                                pred: thrift.SlicePredicate,
                                keyCodec: Codec[K], nameCodec: Codec[N]): Future[Map[N,CounterColumn[N]]] = {
    val cp = new thrift.ColumnParent(name)
    log.debug("get_slice(%s, %s, %s, %s, %s)", keyspace, key, cp, pred, readConsistency.level)
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
