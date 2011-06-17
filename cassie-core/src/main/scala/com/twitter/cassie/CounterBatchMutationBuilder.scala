package com.twitter.cassie

import java.nio.ByteBuffer
import java.util.{List => JList, Map => JMap, Set => JSet,
  ArrayList => JArrayList, HashMap => JHashMap}
import java.util.Collections.{singleton => singletonJSet}

import codecs.Codec
import scala.collection.mutable.ListBuffer
import scala.collection.JavaConversions._
import org.apache.cassandra.finagle.thrift
import com.twitter.util.Future

/**
 * A ColumnFamily-alike which batches mutations into a single API call for counters.
 *
 * TODO: Port to Java collections.
 */
private[cassie] class CounterBatchMutationBuilder[Key,Name](cf: CounterColumnFamily[Key,Name]) {

  case class Insert(key: Key, column: CounterColumn[Name])
  case class Deletions(key: Key, columnNames: JSet[Name])

  private val ops = new ListBuffer[Either[Insert, Deletions]]

  def insert(key: Key, column: CounterColumn[Name]) = synchronized {
    ops.append(Left(Insert(key, column)))
    this
  }

  def removeColumn(key: Key, columnName: Name) =
    removeColumns(key, singletonJSet(columnName))

  def removeColumns(key: Key, columnNames: JSet[Name]) = synchronized {
    ops.append(Right(Deletions(key, columnNames)))
    this
  }

  /**
    * Submits the batch of operations, returning a future to allow blocking for success. */
  def execute() = {
    try {
      cf.batch(mutations)
    } catch {
      case e => Future.exception(e)
    }
  }

  private[cassie] def mutations: JMap[ByteBuffer, JMap[String, JList[thrift.Mutation]]] = synchronized {
    val mutations = new JHashMap[ByteBuffer, JMap[String, JList[thrift.Mutation]]]()

    ops.map {
      case Left(insert) => {
        val cosc = new thrift.ColumnOrSuperColumn()
        val counterColumn = new thrift.CounterColumn(cf.nameCodec.encode(insert.column.name), insert.column.value)
        cosc.setCounter_column(counterColumn)
        val mutation = new thrift.Mutation
        mutation.setColumn_or_supercolumn(cosc)

        val encodedKey = cf.keyCodec.encode(insert.key)

        val h = Option(mutations.get(encodedKey)).getOrElse{val x = new JHashMap[String, JList[thrift.Mutation]]; mutations.put(encodedKey, x); x}
        val l = Option(h.get(cf.name)).getOrElse{ val y = new JArrayList[thrift.Mutation]; h.put(cf.name, y); y}
        l.add(mutation)
      }
      case Right(deletions) => {
        val pred = new thrift.SlicePredicate
        pred.setColumn_names(cf.encodeNames(deletions.columnNames))

        val deletion = new thrift.Deletion
        deletion.setPredicate(pred)

        val mutation = new thrift.Mutation
        mutation.setDeletion(deletion)

        val encodedKey = cf.keyCodec.encode(deletions.key)

        val h = Option(mutations.get(encodedKey)).getOrElse{val x = new JHashMap[String, JList[thrift.Mutation]]; mutations.put(encodedKey, x); x}
        val l = Option(h.get(cf.name)).getOrElse{ val y = new JArrayList[thrift.Mutation]; h.put(cf.name, y); y}
        l.add(mutation)
      }
    }
    mutations
  }
}
