package com.twitter.cassie

import clocks.Clock
import codecs.{Codec, Utf8Codec}
import connection.ClientProvider

import com.twitter.logging.Logger
import java.nio.ByteBuffer
import java.util.Collections.{singleton => singletonSet}

import java.util.{ArrayList, HashMap, Iterator, List, Map, Set}
import org.apache.cassandra.finagle.thrift.Mutation
import scala.collection.JavaConversions._

import com.twitter.util.Future

trait ColumnFamilyLike[Key, Name, Value] {

  /**
    * @return a copy of this [[ColumnFamilyLike]] with a different key codec
    * @param codec new key codec */
  def keysAs[K](codec: Codec[K]): ColumnFamilyLike[K, Name, Value]

  /**
    * @return a copy of this [[ColumnFamilyLike]] with a different name codec
    * @param codec new name codec */
  def namesAs[N](codec: Codec[N]): ColumnFamilyLike[Key, N, Value]

  /**
    * @return a copy of this [[ColumnFamilyLike]] with a different value codec
    * @param codec the new value codec */
  def valuesAs[V](codec: Codec[V]): ColumnFamilyLike[Key, Name, V]

  /**
    * @return a copy of this [[ColumnFamilyLike]] with a different read consistency
    * @param rc the new read consistency level */
  def consistency(rc: ReadConsistency): ColumnFamilyLike[Key, Name, Value]

  /**
    * @return a copy of this [[ColumnFamilyLike]] with a different write consistency level
    * @param wc the new write consistency level */
  def consistency(wc: WriteConsistency): ColumnFamilyLike[Key, Name, Value]

  /**
    * Create a new column for this column family. Useful for java-based users.
    * @param n the column name
    * @param v the column value */
  def newColumn[N, V](n: N, v: V): Column[N, V]

  /**
    * Get an individual column from a single row.
    * @return a future that can contain [[org.apache.cassandra.finagle.thrift.TimedOutException]],
    *  [[org.apache.cassandra.finagle.thrift.UnavailableException]] or [[org.apache.cassandra.finagle.thrift.InvalidRequestException]]
    * @param key the row's key
    * @param the name of the column */
  def getColumn(key: Key, columnName: Name): Future[Option[Column[Name, Value]]]

  /**
    * Results in a map of all column names to the columns for a given key by slicing over a whole row.
    *   If your rows contain a huge number of columns, this will be slow and horrible and you will hate your ife.
    * @return a future that can contain [[org.apache.cassandra.finagle.thrift.TimedOutException]],
    *  [[org.apache.cassandra.finagle.thrift.UnavailableException]] or [[org.apache.cassandra.finagle.thrift.InvalidRequestException]]
    * @param key the row's key */
  def getRow(key: Key): Future[Map[Name, Column[Name, Value]]]

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
                  order: Order): Future[Map[Name, Column[Name, Value]]]

  /**
    * Get a selection of columns from a single row.
    * @return a future that can contain [[org.apache.cassandra.finagle.thrift.TimedOutException]],
    *   [[org.apache.cassandra.finagle.thrift.UnavailableException]] or [[org.apache.cassandra.finagle.thrift.InvalidRequestException]]
    * @param key the row key
    * @param the column names you want */
  def getColumns(key: Key,
                 columnNames: Set[Name]): Future[Map[Name, Column[Name, Value]]]
  /**
    * Get a single column from multiple rows.
    * @return a future that can contain [[org.apache.cassandra.finagle.thrift.TimedOutException]],
    *   [[org.apache.cassandra.finagle.thrift.UnavailableException]] or [[org.apache.cassandra.finagle.thrift.InvalidRequestException]].
    * @param keys the row keys
    * @param the column name */
  def multigetColumn(keys: Set[Key],
                     columnName: Name): Future[Map[Key, Column[Name, Value]]]

  /**
    * Get multiple columns from multiple rows.
    * @return a future that can contain [[org.apache.cassandra.finagle.thrift.TimedOutException]],
    *  [[org.apache.cassandra.finagle.thrift.UnavailableException]] or [[org.apache.cassandra.finagle.thrift.InvalidRequestException]]
    * @param keys the row keys
    * @param columnNames the column names */
  def multigetColumns(keys: Set[Key], columnNames: Set[Name]): Future[Map[Key, Map[Name, Column[Name, Value]]]]

  /**
    * Insert a single column.
    * @return a future that can contain [[org.apache.cassandra.finagle.thrift.TimedOutException]],
    *  [[org.apache.cassandra.finagle.thrift.UnavailableException]] or [[org.apache.cassandra.finagle.thrift.InvalidRequestException]]
    * @param key the row key
    * @param column the column */
  def insert(key: Key, column: Column[Name, Value]): Future[Void]

  /**
    * Truncates this column family.
    * @return a future that can contain [[org.apache.cassandra.finagle.thrift.UnavailableException]]
    *   or [[org.apache.cassandra.finagle.thrift.InvalidRequestException]] */
  def truncate(): Future[Void]

  /**
    * Remove a single column.
    * @return a future that can contain [[org.apache.cassandra.finagle.thrift.TimedOutException]],
    *  [[org.apache.cassandra.finagle.thrift.UnavailableException]] or [[org.apache.cassandra.finagle.thrift.InvalidRequestException]]
    * @param key the row key
    * @param columnName the column's name */
  def removeColumn(key: Key, columnName: Name): Future[Void]

  /**
    * Remove a set of columns from a single row via a batch mutation.
    * @return a future that can contain [[org.apache.cassandra.finagle.thrift.TimedOutException]],
    *  [[org.apache.cassandra.finagle.thrift.UnavailableException]] or [[org.apache.cassandra.finagle.thrift.InvalidRequestException]]
    * @param key the row key
    * @param columnNames the names of the columns to be deleted */
  def removeColumns(key: Key, columnNames: Set[Name]): Future[Void]

  /**
    * Remove a set of columns from a single row at a specific timestamp. Useful for replaying data.
    * @return a future that can contain [[org.apache.cassandra.finagle.thrift.TimedOutException]],
    *  [[org.apache.cassandra.finagle.thrift.UnavailableException]] or [[org.apache.cassandra.finagle.thrift.InvalidRequestException]]
    * @param key the row key
    * @param columnNames the columns to be deleted
    * @param timestamp the timestamp at which the columns should be deleted */
  def removeColumns(key: Key, columnNames: Set[Name], timestamp: Long): Future[Void]

  /**
    * Remove an entire row.
    * @return a future that can contain [[org.apache.cassandra.finagle.thrift.TimedOutException]],
    *  [[org.apache.cassandra.finagle.thrift.UnavailableException]] or [[org.apache.cassandra.finagle.thrift.InvalidRequestException]]
    * @param key the row key to be deleted */
  def removeRow(key: Key): Future[Void]

  /**
    * Remove an entire row at the given timestamp. 
    * @return a future that can contain [[org.apache.cassandra.finagle.thrift.TimedOutException]],
    *  [[org.apache.cassandra.finagle.thrift.UnavailableException]] or [[org.apache.cassandra.finagle.thrift.InvalidRequestException]]
    * @param key the row key to be deleted
    * @param timestamp the time at which the row was deleted*/
  def removeRowWithTimestamp(key: Key, timestamp: Long): Future[Void]

  /**
    * Start a batch operation by returning a new BatchMutationBuilder */
  def batch(): BatchMutationBuilder[Key, Name, Value]

  /**
    * @return a column iterator which iterates over all columns of all rows in
    * the column family with the given batch size.
    * @param batchSize the number of rows to load at a time */
  def rowIteratee(batchSize: Int): ColumnIteratee[Key, Name, Value]

  /**
    * @return a column iterator which iterates over the given column of all rows
    * in the column family with the given batch size as the default types.
    * @param batchSize the number of columns/rows to load at a time (its only 1 column per row)
    * @param columnName the column to load */
  def columnIteratee(batchSize: Int, columnName: Name): ColumnIteratee[Key, Name, Value]

 /**
   * @return a column iterator which iterates over the given columns of all rows
   * in the column family with the given batch size as the default types.
   * @param batchSize the number of rows to load at a time.
   * @param columnNames the columns to load from each row */
  def columnsIteratee(batchSize: Int, columnNames: Set[Name]): ColumnIteratee[Key, Name, Value]
}