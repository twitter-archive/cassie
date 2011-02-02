package com.twitter.cassie

import java.nio.ByteBuffer
import java.util.{List, Map, Set}
import java.util.Collections.{singleton => singletonSet}

import codecs.{Codec, Utf8Codec}
import clocks.Clock
import scalaj.collection.Imports._
import collection.mutable.{ArrayBuffer, HashMap}
import org.apache.cassandra.thrift.{SlicePredicate, Deletion, Mutation, Column => TColumn, ColumnOrSuperColumn}
import scala.collection.mutable.ListBuffer

/**
 * A ColumnFamily-alike which batches mutations into a single API call.
 *
 * TODO: Port to Java collections.
 */
private[cassie] class BatchMutationBuilder[Key,Name,Value](cf: ColumnFamily[Key,Name,Value]) {

  case class Op
  case class Insert(key: Key, column: Column[Name, Value]) extends Op
  case class ColumnDeletions(key: Key, columnNames: Set[Name]) extends Op
  case class TimestampedColumnDeletions(key: Key, columnNames: Set[Name], timestamp: Long) extends Op

  private val ops = new ListBuffer[Op]

  def insert(key: Key, column: Column[Name, Value]) = synchronized {
    ops.append(Insert(key, column))
    this
  }

  def removeColumn(key: Key, columnName: Name) =
    removeColumns(key, singletonSet(columnName))

  def removeColumns(key: Key, columnNames: Set[Name]) = synchronized {
    ops.append(ColumnDeletions(key, columnNames))
    this
  }

  def removeColumnWithTimestamp(key: Key, columnName: Name, timestamp: Long) = {
    removeColumnsWithTimestamp(key, singletonSet(columnName), timestamp)
  }

  def removeColumnsWithTimestamp(key: Key, columnNames: Set[Name], timestamp: Long) = synchronized {
    ops.append(TimestampedColumnDeletions(key, columnNames, timestamp))
    this
  }

  def execute() = {
    cf.batch(mutations)
  }

  private[cassie] def deleteMutation(key: Key, columnNames: Set[Name], timestamp: Long) = {
    val pred = new SlicePredicate
    pred.setColumn_names(cf.encodeSet(columnNames)(cf.defaultNameCodec))

    val deletion = new Deletion(timestamp)
    deletion.setPredicate(pred)

    val mutation = new Mutation
    mutation.setDeletion(deletion)
    mutation
  }

  private[cassie] def mutations: java.util.Map[ByteBuffer, java.util.Map[String, java.util.List[Mutation]]] = synchronized {
    val timestamp = cf.clock.timestamp
    val mutations = new HashMap[ByteBuffer, HashMap[String, ArrayBuffer[Mutation]]]()

    ops.map { op =>
      op match {
        case Insert(key, column) => {
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
          mutations.getOrElseUpdate(cf.defaultKeyCodec.encode(key), new HashMap).
                  getOrElseUpdate(cf.name, new ArrayBuffer) += mutation
        }
        case ColumnDeletions(key, columnNames) => {
          val mutation = deleteMutation(key, columnNames, timestamp)
          mutations.getOrElseUpdate(cf.defaultKeyCodec.encode(key), new HashMap).
                  getOrElseUpdate(cf.name, new ArrayBuffer) += mutation
        }
        case TimestampedColumnDeletions(key, columnNames, ts) => {
          val mutation = deleteMutation(key, columnNames, ts)
          mutations.getOrElseUpdate(cf.defaultKeyCodec.encode(key), new HashMap).
                  getOrElseUpdate(cf.name, new ArrayBuffer) += mutation
        }
      }
    }
    mutations.map { case (key, cfMap) =>
      key -> cfMap.map { case (cf, m) =>
        cf -> m.asJava
      }.asJava
    }.asJava
  }
}
