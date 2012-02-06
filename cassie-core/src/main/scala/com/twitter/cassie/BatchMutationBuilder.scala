// Copyright 2012 Twitter, Inc.

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

// http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.twitter.cassie

import com.twitter.util.Future
import java.nio.ByteBuffer
import java.util.Collections.{ singleton => singletonJSet }
import java.util.{ List => JList, Map => JMap, Set => JSet, ArrayList => JArrayList,HashMap => JHashMap}
import org.apache.cassandra.finagle.thrift
import scala.collection.mutable.ListBuffer

trait BatchMutation {
  private[cassie] def mutations: JMap[ByteBuffer, JMap[String, JList[thrift.Mutation]]]
}

/**
 * A ColumnFamily-alike which batches mutations into a single API call.
 *
 */
class BatchMutationBuilder[Key, Name, Value](private[cassie] val cf: ColumnFamily[Key, Name, Value])
  extends BatchMutation {

  type This = BatchMutationBuilder[Key, Name, Value]

  private[cassie] case class Insert(key: Key, column: Column[Name, Value])
  private[cassie] case class Deletions(key: Key, columnNames: JSet[Name], timestamp: Long)

  private val ops = new ListBuffer[Either[Insert, Deletions]]

  def insert(key: Key, column: Column[Name, Value]): This = synchronized {
    ops.append(Left(Insert(key, column)))
    this
  }

  def removeColumn(key: Key, columnName: Name): This =
    removeColumns(key, singletonJSet(columnName))

  def removeColumn(key: Key, columnName: Name, timestamp: Long): This =
    removeColumns(key, singletonJSet(columnName), timestamp)

  def removeColumns(key: Key, columns: JSet[Name]): This =
    removeColumns(key, columns, cf.clock.timestamp)

  def removeColumns(key: Key, columns: JSet[Name], timestamp: Long): This = synchronized {
    ops.append(Right(Deletions(key, columns, timestamp)))
    this
  }

  /**
   * Submits the batch of operations, returning a Future[Void] to allow blocking for success.
   */
  def execute(): Future[Void] = {
    Future {
      cf.batch(mutations)
    }.flatten
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

        val h = Option(mutations.get(encodedKey)).getOrElse { val x = new JHashMap[String, JList[thrift.Mutation]]; mutations.put(encodedKey, x); x }
        val l = Option(h.get(cf.name)).getOrElse { val y = new JArrayList[thrift.Mutation]; h.put(cf.name, y); y }
        l.add(mutation)
      }
      case Right(deletions) => {
        val timestamp = deletions.timestamp
        val pred = new thrift.SlicePredicate
        pred.setColumn_names(cf.nameCodec.encodeSet(deletions.columnNames))

        val deletion = new thrift.Deletion()
        deletion.setTimestamp(timestamp)
        deletion.setPredicate(pred)

        val mutation = new thrift.Mutation
        mutation.setDeletion(deletion)

        val encodedKey = cf.keyCodec.encode(deletions.key)

        val h = Option(mutations.get(encodedKey)).getOrElse { val x = new JHashMap[String, JList[thrift.Mutation]]; mutations.put(encodedKey, x); x }
        val l = Option(h.get(cf.name)).getOrElse { val y = new JArrayList[thrift.Mutation]; h.put(cf.name, y); y }
        l.add(mutation)
      }
    }
    mutations
  }
}
