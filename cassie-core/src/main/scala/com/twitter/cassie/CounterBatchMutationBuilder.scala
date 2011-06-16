package com.twitter.cassie

import java.nio.ByteBuffer
import java.util.{List => JList, Map, Set}
import java.util.Collections.{singleton => singletonSet}

import codecs.{Codec, Utf8Codec}
import java.util.{ArrayList, HashMap}
import scala.collection.mutable.ListBuffer
import scala.collection.JavaConversions._
import org.apache.cassandra.finagle.thrift.{SlicePredicate, CounterColumn => TCounterColumn, 
  Mutation => TMutation, ColumnOrSuperColumn => TColumnOrSuperColumn, Deletion => TDeletion}

/**
 * A ColumnFamily-alike which batches mutations into a single API call for counters.
 *
 * TODO: Port to Java collections.
 */
private[cassie] class CounterBatchMutationBuilder[Key,Name](cf: CounterColumnFamily[Key,Name]) {

  case class Insert(key: Key, column: CounterColumn[Name])
  case class Deletions(key: Key, columnNames: Set[Name])

  private val ops = new ListBuffer[Either[Insert, Deletions]]

  def insert(key: Key, column: CounterColumn[Name]) = synchronized {
    ops.append(Left(Insert(key, column)))
    this
  }

  def removeColumn(key: Key, columnName: Name) =
    removeColumns(key, singletonSet(columnName))

  def removeColumns(key: Key, columnNames: Set[Name]) = synchronized {
    ops.append(Right(Deletions(key, columnNames)))
    this
  }

  /**
   * Submits the batch of operations, returning a future to allow blocking for success.
   */
  def execute() = {
    cf.batch(mutations)
  }

  private[cassie] def mutations: java.util.Map[ByteBuffer, java.util.Map[String, JList[TMutation]]] = synchronized {
    val mutations = new HashMap[ByteBuffer, Map[String, JList[TMutation]]]()

    ops.map { op =>
      op match {
        case Left(insert) => {
          val cosc = new TColumnOrSuperColumn()
          val counterColumn = new TCounterColumn(cf.defaultNameCodec.encode(insert.column.name), insert.column.value)
          cosc.setCounter_column(counterColumn)
          val mutation = new TMutation
          mutation.setColumn_or_supercolumn(cosc)

          val encodedKey = cf.defaultKeyCodec.encode(insert.key)

          val h = Option(mutations.get(encodedKey)).getOrElse{val x = new HashMap[String, JList[TMutation]]; mutations.put(encodedKey, x); x}
          val l = Option(h.get(cf.name)).getOrElse{ val y = new ArrayList[TMutation]; h.put(cf.name, y); y}
          l.add(mutation)
        }
        case Right(deletions) => {
          val pred = new SlicePredicate
          pred.setColumn_names(cf.encodeSet(deletions.columnNames)(cf.defaultNameCodec))

          val deletion = new TDeletion
          deletion.setPredicate(pred)

          val mutation = new TMutation
          mutation.setDeletion(deletion)

          val encodedKey = cf.defaultKeyCodec.encode(deletions.key)

          val h = Option(mutations.get(encodedKey)).getOrElse{val x = new HashMap[String, JList[TMutation]]; mutations.put(encodedKey, x); x}
          val l = Option(h.get(cf.name)).getOrElse{ val y = new ArrayList[TMutation]; h.put(cf.name, y); y}
          l.add(mutation)
        }
      }
    }
    mutations
  }
}
