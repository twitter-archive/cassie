package com.twitter.cassie

import clocks.Clock
import codecs.Codec
import connection.ClientProvider
import com.twitter.util.Future
import scala.collection.JavaConversions._

/**
 * A Cassandra keyspace, which maintains a connection pool.
 *
 * @param provider a [[com.twitter.cassie.connection.ClientProvider]] instance
 */
class Keyspace(val name: String, val provider: ClientProvider) {
  /**
   * Returns a ColumnFamily with the given name and column/value codecs.
   */
  def columnFamily[Key, Name, Value](name: String)
    (implicit defaultKeyCodec: Codec[Key],
              defaultNameCodec: Codec[Name],
              defaultValueCodec: Codec[Value]) =
    new ColumnFamily(this.name, name, provider, defaultKeyCodec, defaultNameCodec, defaultValueCodec)

  def execute[Key, Name, Value](batches: Seq[BatchMutationBuilder[Key, Name, Value]]): Future[Void] = {
    if(batches.size == 0) return Future(null.asInstanceOf[Void])

    val mutations = batches.map(_.mutations).head

    if (batches.size > 1) {
      // java.util.Map[ByteBuffer, java.util.Map[String, java.util.List[Mutation]]]
      batches.map(_.mutations).tail.foreach { ms =>
        for ((row, inner) <- ms) {
          if (mutations.containsKey(row)) {
            val oldRowMap = mutations.get(row)
            for ((cf, mutationList) <- inner) {
              if (oldRowMap.containsKey(cf)) {
                val oldList = oldRowMap.get(cf)
                oldList.addAll(mutationList)
              } else {
                oldRowMap.put(cf, mutationList)
              }
            }
          } else {
            mutations.put(row, inner)
          }
        }
      }
    }

    val writeConsistency = batches.map(_.cf.writeConsistency).head
    provider.map { _.batch_mutate(mutations, writeConsistency.level) }
  }

  /**
   * Closes connections to the cluster for this keyspace.
   */
  def close() = provider.close()
}
