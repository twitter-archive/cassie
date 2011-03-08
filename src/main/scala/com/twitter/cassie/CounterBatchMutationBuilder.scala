package com.twitter.cassie

import java.nio.ByteBuffer
import java.util.{List, Map, Set}
import java.util.Collections.{singleton => singletonSet}

import codecs.{Codec, Utf8Codec}
import java.util.{ArrayList, HashMap}
import scala.collection.mutable.ListBuffer
import scala.collection.JavaConversions._
import org.apache.cassandra.thrift.{CounterDeletion, Counter, CounterMutation, SlicePredicate, CounterColumn => TCounterColumn}

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

  private[cassie] def mutations: java.util.Map[ByteBuffer, java.util.Map[String, java.util.List[CounterMutation]]] = synchronized {
    val mutations = new HashMap[ByteBuffer, Map[String, List[CounterMutation]]]()

    ops.map { op =>
      op match {
        case Left(insert) => {
          val counter = new Counter
          counter.setColumn(
            new TCounterColumn(
              cf.defaultNameCodec.encode(insert.column.name),
              insert.column.value
            )
          )
          val mutation = new CounterMutation
          mutation.setCounter(counter)

          val encodedKey = cf.defaultKeyCodec.encode(insert.key)

          val h = Option(mutations.get(encodedKey)).getOrElse{val x = new HashMap[String, List[CounterMutation]]; mutations.put(encodedKey, x); x}
          val l = Option(h.get(cf.name)).getOrElse{ val y = new ArrayList[CounterMutation]; h.put(cf.name, y); y}
          l.add(mutation)
        }
        case Right(deletions) => {
          val pred = new SlicePredicate
          pred.setColumn_names(cf.encodeSet(deletions.columnNames)(cf.defaultNameCodec))

          val deletion = new CounterDeletion
          deletion.setPredicate(pred)

          val mutation = new CounterMutation
          mutation.setDeletion(deletion)

          val encodedKey = cf.defaultKeyCodec.encode(deletions.key)

          val h = Option(mutations.get(encodedKey)).getOrElse{val x = new HashMap[String, List[CounterMutation]]; mutations.put(encodedKey, x); x}
          val l = Option(h.get(cf.name)).getOrElse{ val y = new ArrayList[CounterMutation]; h.put(cf.name, y); y}
          l.add(mutation)
        }
      }
    }
    mutations
  }
}
