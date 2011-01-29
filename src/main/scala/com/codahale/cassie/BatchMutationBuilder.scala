package com.codahale.cassie

import java.nio.ByteBuffer
import java.util.{List, Map, Set}
import java.util.Collections.{singleton => singletonSet}

import codecs.{Codec, Utf8Codec}
import clocks.Clock
import scalaj.collection.Imports._
import collection.mutable.{ArrayBuffer, HashMap}
import org.apache.cassandra.thrift.{SlicePredicate, Deletion, Mutation, Column => TColumn, ColumnOrSuperColumn}

/**
 * A ColumnFamily-alike which batches mutations into a single API call.
 *
 * TODO: Port to Java collections.
 *
 * @author coda
 */
private[cassie] class BatchMutationBuilder[Key,Name,Value](cf: ColumnFamily[Key,Name,Value]) {
  private val ops = new HashMap[ByteBuffer, HashMap[String, ArrayBuffer[Mutation]]]()

  /**
   * Inserts a column.
   */
  def insert(key: Key, column: Column[Name, Value]) = {
    val cosc = new ColumnOrSuperColumn
    cosc.setColumn(
      new TColumn(
        cf.defaultNameCodec.encode(column.name),
        cf.defaultValueCodec.encode(column.value),
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
  def removeColumn(key: Key, columnName: Name) =
    removeColumnWithTimestamp(key, columnName, cf.clock.timestamp)

  /**
   * Removes a column from a row with a specific timestamp.
   */
  def removeColumnWithTimestamp(key: Key, columnName: Name, timestamp: Long) =
    removeColumnsWithTimestamp(key, singletonSet(columnName), timestamp)

  /**
   * Removes a set of columns from a row.
   */
  def removeColumns(key: Key, columnNames: Set[Name]) = 
    removeColumnsWithTimestamp(key, columnNames, cf.clock.timestamp)

  /**
   * Removes a set of columns from a row with a specific timestamp.
   */
  def removeColumnsWithTimestamp(key: Key, columnNames: Set[Name], timestamp: Long) = {
    val pred = new SlicePredicate
    pred.setColumn_names(cf.encodeSet(columnNames)(cf.defaultNameCodec))

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

  private def addMutation(key: Key, mutation: Mutation) = {
    synchronized {
      ops.getOrElseUpdate(cf.defaultKeyCodec.encode(key), new HashMap).
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
