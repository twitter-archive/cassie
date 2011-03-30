package com.twitter.cassie

import java.nio.ByteBuffer
import java.util.{List, Map, Set}
import java.util.Collections.{singleton => singletonSet}

import codecs.{Codec, Utf8Codec}
import java.util.{ArrayList, HashMap}
import org.apache.cassandra.finagle.thrift.{SlicePredicate, Deletion, Mutation, Column => TColumn, ColumnOrSuperColumn}
import scala.collection.mutable.ListBuffer
import scala.collection.JavaConversions._

/**
 * A ColumnFamily-alike which batches mutations into a single API call.
 *
 * TODO: Port to Java collections.
 */
private[cassie] class BatchMutationBuilder[Key,Name,Value](cf: ColumnFamily[Key,Name,Value]) {

  case class Insert(key: Key, column: Column[Name, Value])
  case class Deletions(key: Key, columnNames: Set[Name], timestamp: Long)

  private val ops = new ListBuffer[Either[Insert, Deletions]]

  def insert(key: Key, column: Column[Name, Value]) = synchronized {
    ops.append(Left(Insert(key, column)))
    this
  }

  def removeColumn(key: Key, columnName: Name) =
    removeColumns(key, singletonSet(columnName))

  def removeColumn(key: Key, columnName: Name, timestamp: Long) =
    removeColumns(key, singletonSet(columnName), timestamp)
    
  def removeColumns(key: Key, columns: Set[Name]): BatchMutationBuilder[Key,Name,Value] = 
    removeColumns(key, columns, cf.clock.timestamp)
  
  def removeColumns(key: Key, columns: Set[Name], timestamp: Long): BatchMutationBuilder[Key,Name,Value] = synchronized {
    ops.append(Right(Deletions(key, columns, timestamp)))
    this
  }
    
  /**
   * Submits the batch of operations, returning a future to allow blocking for success.
   */
  def execute() = {
    cf.batch(mutations)
  }

  private[cassie] def mutations: java.util.Map[ByteBuffer, java.util.Map[String, java.util.List[Mutation]]] = synchronized {
    val mutations = new HashMap[ByteBuffer, Map[String, List[Mutation]]]()

    ops.map { op =>
      op match {
        case Left(insert) => {
          val timestamp = cf.clock.timestamp
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

          val encodedKey = cf.defaultKeyCodec.encode(insert.key)

          val h = Option(mutations.get(encodedKey)).getOrElse{val x = new HashMap[String, List[Mutation]]; mutations.put(encodedKey, x); x}
          val l = Option(h.get(cf.name)).getOrElse{ val y = new ArrayList[Mutation]; h.put(cf.name, y); y}
          l.add(mutation)
        }
        case Right(deletions) => {
          val timestamp = deletions.timestamp
          val pred = new SlicePredicate
          pred.setColumn_names(cf.encodeSet(deletions.columnNames)(cf.defaultNameCodec))

          val deletion = new Deletion(timestamp)
          deletion.setPredicate(pred)

          val mutation = new Mutation
          mutation.setDeletion(deletion)

          val encodedKey = cf.defaultKeyCodec.encode(deletions.key)

          val h = Option(mutations.get(encodedKey)).getOrElse{val x = new HashMap[String, List[Mutation]]; mutations.put(encodedKey, x); x}
          val l = Option(h.get(cf.name)).getOrElse{ val y = new ArrayList[Mutation]; h.put(cf.name, y); y}
          l.add(mutation)
        }
      }
    }
    return mutations
  }
}
