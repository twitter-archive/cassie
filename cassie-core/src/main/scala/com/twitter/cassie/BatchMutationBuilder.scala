package com.twitter.cassie

import java.nio.ByteBuffer
import java.util.{List => JList, Map => JMap, Set => JSet, ArrayList => JArrayList,
  HashMap => JHashMap}
import java.util.Collections.{singleton => singletonJSet}

import org.apache.cassandra.finagle.thrift
import scala.collection.mutable.ListBuffer
import com.twitter.util.Future

trait BatchMutation {
  private[cassie] def mutations: JMap[ByteBuffer, JMap[String, JList[thrift.Mutation]]]
}

/**
 * A ColumnFamily-alike which batches mutations into a single API call.
 *
 * TODO: Port to Java collections.
 * TODO: make into a CFLike
 */
class BatchMutationBuilder[Key,Name,Value](private[cassie] val cf: ColumnFamily[Key,Name,Value])
    extends BatchMutation{

  private[cassie] case class Insert(key: Key, column: Column[Name, Value])
  private[cassie] case class Deletions(key: Key, columnNames: JSet[Name], timestamp: Long)

  private val ops = new ListBuffer[Either[Insert, Deletions]]

  def insert(key: Key, column: Column[Name, Value]) = synchronized {
    ops.append(Left(Insert(key, column)))
    this
  }

  def removeColumn(key: Key, columnName: Name) =
    removeColumns(key, singletonJSet(columnName))

  def removeColumn(key: Key, columnName: Name, timestamp: Long) =
    removeColumns(key, singletonJSet(columnName), timestamp)

  def removeColumns(key: Key, columns: JSet[Name]): BatchMutationBuilder[Key,Name,Value] =
    removeColumns(key, columns, cf.clock.timestamp)

  def removeColumns(key: Key, columns: JSet[Name], timestamp: Long): BatchMutationBuilder[Key,Name,Value] = synchronized {
    ops.append(Right(Deletions(key, columns, timestamp)))
    this
  }

  /**
    * Submits the batch of operations, returning a Future[Void] to allow blocking for success. */
  def execute() = {
    try {
      cf.batch(mutations)
    }  catch {
      case e => Future.exception(e)
    }
  }

  private[cassie] override def mutations: JMap[ByteBuffer, JMap[String, JList[thrift.Mutation]]] = synchronized {
    val mutations = new JHashMap[ByteBuffer, JMap[String, JList[thrift.Mutation]]]()

    ops.map {
      case Left(insert) => {
        val timestamp = cf.clock.timestamp
        val cosc = new thrift.ColumnOrSuperColumn
        cosc.setColumn(
          Column.convert(
            cf.nameCodec,
            cf.valueCodec,
            cf.clock,
            new Column(
              insert.column.name,
              insert.column.value,
              Some(insert.column.timestamp.getOrElse(timestamp)),
              None
            )
          )
        )
        val mutation = new thrift.Mutation
        mutation.setColumn_or_supercolumn(cosc)

        val encodedKey = cf.keyCodec.encode(insert.key)

        val h = Option(mutations.get(encodedKey)).getOrElse{val x = new JHashMap[String, JList[thrift.Mutation]]; mutations.put(encodedKey, x); x}
        val l = Option(h.get(cf.name)).getOrElse{ val y = new JArrayList[thrift.Mutation]; h.put(cf.name, y); y}
        l.add(mutation)
      }
      case Right(deletions) => {
        val timestamp = deletions.timestamp
        val pred = new thrift.SlicePredicate
        pred.setColumn_names(cf.encodeNames(deletions.columnNames))

        val deletion = new thrift.Deletion()
        deletion.setTimestamp(timestamp)
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
