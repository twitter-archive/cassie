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

  def execute() = {
    cf.batch(mutations)
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
              column.timestamp.getOrElse(timestamp)
            )
          )
          val mutation = new Mutation
          mutation.setColumn_or_supercolumn(cosc)
          mutations.getOrElseUpdate(cf.defaultKeyCodec.encode(key), new HashMap).
                  getOrElseUpdate(cf.name, new ArrayBuffer) += mutation
        }
        case ColumnDeletions(key, columnNames) => {
          val pred = new SlicePredicate
          pred.setColumn_names(cf.encodeSet(columnNames)(cf.defaultNameCodec))

          val deletion = new Deletion(timestamp)
          deletion.setPredicate(pred)

          val mutation = new Mutation
          mutation.setDeletion(deletion)

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
