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
private[cassie] class BatchMutationBuilder[Key,Name,Value](cf: ColumnFamily[Key,Name,Value]) {
  private val ops = new HashMap[ByteBuffer, HashMap[String, ArrayBuffer[Mutation]]]()

  /**
   * Inserts a column.
   */
  def insert[Key, Name, Value](key: Key, column: Column[Name, Value])
                         (implicit keyCodec: Codec[Key], nameCodec: Codec[Name], valueCodec: Codec[Value]) = {
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
  def removeColumn[Key, Name](key: Key, columnName: Name)
                  (implicit clock: Clock, keyCodec: Codec[Key], nameCodec: Codec[Name]) = {
    removeColumnWithTimestamp(key, columnName, clock.timestamp)(keyCodec, nameCodec)
  }

  /**
   * Removes a column from a row with a specific timestamp.
   */
  def removeColumnWithTimestamp[Key, Name](key: Key, columnName: Name, timestamp: Long)
                  (implicit keyCodec: Codec[Key], nameCodec: Codec[Name]) = {
    removeColumnsWithTimestamp(key, Set(columnName), timestamp)(keyCodec, nameCodec)
  }

  /**
   * Removes a set of columns from a row.
   */
  def removeColumns[Key, Name](key: Key, columnNames: Set[Name])
                  (implicit clock: Clock, keyCodec: Codec[Key], nameCodec: Codec[Name]) = {
    removeColumnsWithTimestamp(key, columnNames, clock.timestamp)(keyCodec, nameCodec)
  }

  /**
   * Removes a set of columns from a row with a specific timestamp.
   */
  def removeColumnsWithTimestamp[Key, Name](key: Key, columnNames: Set[Name], timestamp: Long)
                  (implicit keyCodec: Codec[Key], nameCodec: Codec[Name]) = {
    val pred = new SlicePredicate
    pred.setColumn_names(columnNames.toList.map { nameCodec.encode(_) }.asJava)

    val deletion = new Deletion(timestamp)
    deletion.setPredicate(pred)

    val mutation = new Mutation
    mutation.setDeletion(deletion)

    addMutation(key, mutation)
  }

  /**
   * Submits the batch of operations.
   */
  def execute() = cf.batch(this)

  private def addMutation[Key](key: Key, mutation: Mutation)
                              (implicit keyCodec: Codec[Key]) = {
    synchronized {
      ops.getOrElseUpdate(keyCodec.encode(key), new HashMap).
              getOrElseUpdate(cf.name, new ArrayBuffer) += mutation
    }
    this
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
