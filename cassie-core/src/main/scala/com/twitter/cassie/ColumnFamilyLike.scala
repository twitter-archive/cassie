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

trait ColumnFamilyLike[Key, Name, Value] {

  def keysAs[K](codec: Codec[K]): ColumnFamilyLike[K, Name, Value]
  def namesAs[N](codec: Codec[N]): ColumnFamilyLike[Key, N, Value]
  def valuesAs[V](codec: Codec[V]): ColumnFamilyLike[Key, Name, V]
  def consistency(rc: ReadConsistency): ColumnFamilyLike[Key, Name, Value]
  def consistency(wc: WriteConsistency): ColumnFamilyLike[Key, Name, Value]
  def newColumn[N, V](n: N, v: V): Column[N, V]
  def getColumnAs[K, N, V](key: K, columnName: N)(implicit keyCodec: Codec[K], nameCodec: Codec[N], valueCodec: Codec[V]): Future[Option[Column[N, V]]]
  def getColumn(key: Key, columnName: Name): Future[Option[Column[Name, Value]]]
  def getRowAs[K, N, V](key: K)
                    (implicit keyCodec: Codec[K], nameCodec: Codec[N], valueCodec: Codec[V]): Future[Map[N, Column[N, V]]]
  def getRow(key: Key): Future[Map[Name, Column[Name, Value]]]
  def getRowSliceAs[K, N, V](key: K,
                             startColumnName: Option[N],
                             endColumnName: Option[N],
                             count: Int,
                             order: Order)
                            (implicit keyCodec: Codec[K], nameCodec: Codec[N], valueCodec: Codec[V]): Future[Map[N,Column[N,V]]]

  def getRowSlice(key: Key,
                  startColumnName: Option[Name],
                  endColumnName: Option[Name],
                  count: Int,
                  order: Order): Future[Map[Name, Column[Name, Value]]]
  def getColumnsAs[K, N, V](key: K,
                            columnNames: Set[N])
                           (implicit keyCodec: Codec[K], nameCodec: Codec[N], valueCodec: Codec[V]): Future[Map[N, Column[N, V]]]
  def getColumns(key: Key,
                 columnNames: Set[Name]): Future[Map[Name, Column[Name, Value]]]
  def multigetColumnAs[K, N, V](keys: Set[K],
                                columnName: N)
                               (implicit keyCodec: Codec[K], nameCodec: Codec[N], valueCodec: Codec[V]): Future[Map[K, Column[N, V]]]
  def multigetColumn(keys: Set[Key],
                     columnName: Name): Future[Map[Key, Column[Name, Value]]]
  @throws(classOf[thrift.TimedOutException])
  @throws(classOf[thrift.UnavailableException])
  @throws(classOf[thrift.InvalidRequestException])
  def multigetColumnsAs[K, N, V](keys: Set[K],
                              columnNames: Set[N])
                             (implicit keyCodec: Codec[K], nameCodec: Codec[N], valueCodec: Codec[V]): Future[Map[K, Map[N, Column[N, V]]]]
  def multigetColumns(keys: Set[Key], columnNames: Set[Name]): Future[Map[Key, Map[Name, Column[Name, Value]]]]
  @throws(classOf[thrift.TimedOutException])
  @throws(classOf[thrift.UnavailableException])
  @throws(classOf[thrift.InvalidRequestException])
  def insert(key: Key, column: Column[Name, Value]): Future[Void]
  def insertAs[K, N, V](key: K, column: Column[N, V])
                  (implicit keyCodec: Codec[K], nameCodec: Codec[N], valueCodec: Codec[V]): Future[Void]
  def truncate(): Future[Void]
   @throws(classOf[thrift.TimedOutException])
   @throws(classOf[thrift.UnavailableException])
   @throws(classOf[thrift.InvalidRequestException])
  def removeColumn(key: Key, columnName: Name): Future[Void]
  def removeColumns(key: Key, columnNames: Set[Name]): Future[Void]
  def removeColumns(key: Key, columnNames: Set[Name], timestamp: Long): Future[Void]
  def removeRow(key: Key): Future[Void]
  @throws(classOf[thrift.TimedOutException])
  @throws(classOf[thrift.UnavailableException])
  @throws(classOf[thrift.InvalidRequestException])
  def removeRowWithTimestamp(key: Key, timestamp: Long): Future[Void]
  def batch(): BatchMutationBuilder[Key, Name, Value]
  def rowIterateeAs[K, N, V](batchSize: Int)
                         (implicit keyCodec: Codec[K], nameCodec: Codec[N], valueCodec: Codec[V]): ColumnIteratee[K, N, V]
  def rowIteratee(batchSize: Int): ColumnIteratee[Key, Name, Value]
  def columnIterateeAs[K, N, V](batchSize: Int, columnName: N)
                               (implicit keyCodec: Codec[K], nameCodec: Codec[N], valueCodec: Codec[V]): ColumnIteratee[K, N, V]
  def columnIteratee(batchSize: Int,
                     columnName: Name): ColumnIteratee[Key, Name, Value]
  def columnsIterateeAs[K, N, V](batchSize: Int,
                                 columnNames: Set[N])
                                (implicit keyCodec: Codec[K], nameCodec: Codec[N], valueCodec: Codec[V]): ColumnIteratee[K, N, V]
  def columnsIteratee(batchSize: Int,
                      columnNames: Set[Name]): ColumnIteratee[Key, Name, Value]
}