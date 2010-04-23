package com.codahale.cassie

import scalaj.collection.Imports._
import clocks.Clock
import collection.mutable.{ArrayBuffer, HashMap}
import org.apache.cassandra.thrift.{SlicePredicate, Deletion, Mutation, Column => TColumn, ColumnOrSuperColumn}

/**
 * A ColumnFamily-alike which batches mutations into a single API call.
 *
 * @author coda
 */
class BatchMutationBuilder[Name, Value](cf: ColumnFamily[Name, Value]) {
  private val ops = new HashMap[String, HashMap[String, ArrayBuffer[Mutation]]]()

  /**
   * Inserts a column.
   */
  def insert(key: String, column: Column[Name, Value]) {
    val cosc = new ColumnOrSuperColumn
    cosc.setColumn(
      new TColumn(
        cf.columnCodec.encode(column.name),
        cf.valueCodec.encode(column.value),
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
  def remove(key: String, columnName: Name)
            (implicit clock: Clock) {
    remove(key, columnName, clock.timestamp)
  }

  /**
   * Removes a column from a row with a specific timestamp.
   */
  def remove(key: String, columnName: Name, timestamp: Long) {
    remove(key, Set(columnName), timestamp)
  }

  /**
   * Removes a set of columns from a row.
   */
  def remove(key: String, columnNames: Set[Name])
            (implicit clock: Clock) {
    remove(key, columnNames, clock.timestamp)
  }

  /**
   * Removes a set of columns from a row with a specific timestamp.
   */
  def remove(key: String, columnNames: Set[Name], timestamp: Long) {
    val pred = new SlicePredicate
    pred.setColumn_names(columnNames.toList.map { cf.columnCodec.encode(_) }.asJava)

    val deletion = new Deletion(timestamp)
    deletion.setPredicate(pred)

    val mutation = new Mutation
    mutation.setDeletion(deletion)

    addMutation(key, mutation)
  }

  private def addMutation(key: String, mutation: Mutation) {
    synchronized {
      ops.getOrElseUpdate(key, new HashMap).
              getOrElseUpdate(cf.name, new ArrayBuffer) += mutation
    }
  }

  private[cassie] def mutations: java.util.Map[String, java.util.Map[String, java.util.List[Mutation]]] = {
    synchronized {
      ops.map { case (key, cfMap) =>
        key -> cfMap.map { case (cf, m) =>
          cf -> m.asJava
        }.asJava
      }.asJava
    }
  }
}
