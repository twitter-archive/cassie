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

  case class Insert(key: Key, column: Column[Name, Value])
  case class Deletions(key: Key, columnNames: Set[Name])

  private val ops = new ListBuffer[Either[Insert, Deletions]]

  def insert(key: Key, column: Column[Name, Value]) = synchronized {
    ops.append(Left(Insert(key, column)))
    this
  }

  def removeColumn(key: Key, columnName: Name) =
    removeColumns(key, singletonSet(columnName))

  def removeColumns(key: Key, columnNames: Set[Name]) = synchronized {
    ops.append(Right(Deletions(key, columnNames)))
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
        case Left(insert) => {
          val cosc = new ColumnOrSuperColumn
          cosc.setColumn(
            new TColumn(
              cf.defaultNameCodec.encode(insert.column.name),
              cf.defaultValueCodec.encode(insert.column.value),
              insert.column.timestamp.getOrElse(timestamp)
            )
          )
          val mutation = new Mutation
          mutation.setColumn_or_supercolumn(cosc)
          mutations.getOrElseUpdate(cf.defaultKeyCodec.encode(insert.key), new HashMap).
                  getOrElseUpdate(cf.name, new ArrayBuffer) += mutation
        }
        case Right(deletions) => {
          val pred = new SlicePredicate
          pred.setColumn_names(cf.encodeSet(deletions.columnNames)(cf.defaultNameCodec))

          val deletion = new Deletion(timestamp)
          deletion.setPredicate(pred)

          val mutation = new Mutation
          mutation.setDeletion(deletion)

          mutations.getOrElseUpdate(cf.defaultKeyCodec.encode(deletions.key), new HashMap).
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
