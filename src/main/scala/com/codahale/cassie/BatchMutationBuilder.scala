package com.codahale.cassie

import java.nio.ByteBuffer
import codecs.{Codec, Utf8Codec}
import clocks.Clock
import scalaj.collection.Imports._
import collection.mutable.{ArrayBuffer, HashMap}
import org.apache.cassandra.thrift.{SlicePredicate, Deletion, Mutation, Column => TColumn, ColumnOrSuperColumn}

/**
 * A ColumnFamily-alike which batches mutations into a single API call.
 *
 * @author coda
 */
class BatchMutationBuilder(cfName: String) {
  private val ops = new HashMap[ByteBuffer, HashMap[String, ArrayBuffer[Mutation]]]()

  // TODO: thread the key codec through as an implicit where necessary
  val keyCodec: Codec[String] = Utf8Codec

  /**
   * Inserts a column.
   */
  def insert[Name, Value](key: String, column: Column[Name, Value])
                         (implicit nameCodec: Codec[Name], valueCodec: Codec[Value]) {
    val cosc = new ColumnOrSuperColumn
    cosc.setColumn(
      new TColumn(
        nameCodec.encode(column.name),
        valueCodec.encode(column.value),
        column.timestamp
      )
    )
    val mutation = new Mutation
    mutation.setColumn_or_supercolumn(cosc)
    addMutation(key, mutation)
  }

  /**
   * Removes a column from a row.
   */
  def removeColumn[Name](key: String, columnName: Name)
                  (implicit clock: Clock, nameCodec: Codec[Name]) {
    removeColumnWithTimestamp(key, columnName, clock.timestamp)(nameCodec)
  }

  /**
   * Removes a column from a row with a specific timestamp.
   */
  def removeColumnWithTimestamp[Name](key: String, columnName: Name, timestamp: Long)
                  (implicit nameCodec: Codec[Name]) {
    removeColumnsWithTimestamp(key, Set(columnName), timestamp)(nameCodec)
  }

  /**
   * Removes a set of columns from a row.
   */
  def removeColumns[Name](key: String, columnNames: Set[Name])
                  (implicit clock: Clock, nameCodec: Codec[Name]) {
    removeColumnsWithTimestamp(key, columnNames, clock.timestamp)(nameCodec)
  }

  /**
   * Removes a set of columns from a row with a specific timestamp.
   */
  def removeColumnsWithTimestamp[Name](key: String, columnNames: Set[Name], timestamp: Long)
                  (implicit nameCodec: Codec[Name]) {
    val pred = new SlicePredicate
    pred.setColumn_names(columnNames.toList.map { nameCodec.encode(_) }.asJava)

    val deletion = new Deletion(timestamp)
    deletion.setPredicate(pred)

    val mutation = new Mutation
    mutation.setDeletion(deletion)

    addMutation(key, mutation)
  }

  private def addMutation(key: String, mutation: Mutation) {
    synchronized {
      ops.getOrElseUpdate(keyCodec.encode(key), new HashMap).
              getOrElseUpdate(cfName, new ArrayBuffer) += mutation
    }
  }

  private[cassie] def mutations: java.util.Map[ByteBuffer, java.util.Map[String, java.util.List[Mutation]]] = {
    synchronized {
      ops.map { case (key, cfMap) =>
        key -> cfMap.map { case (cf, m) =>
          cf -> m.asJava
        }.asJava
      }.asJava
    }
  }
}
