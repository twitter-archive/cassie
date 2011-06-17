package com.twitter.cassie

import codecs.Codec
import connection.ClientProvider
import com.twitter.util.Future
import scala.collection.JavaConversions._
import java.util.{HashMap => JHashMap, Map => JMap, List => JList, ArrayList => JArrayList}
import org.apache.cassandra.finagle.thrift
import java.nio.ByteBuffer

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

  /**
   * Returns a CounterColumnFamily with the given name and column codecs
   */
  def counterColumnFamily[Key, Name](name: String)
    (implicit defaultKeyCodec: Codec[Key],
              defaultNameCodec: Codec[Name]) =
    new CounterColumnFamily(this.name, name, provider, defaultKeyCodec, defaultNameCodec)

  /**
    * Execute batch mutations across column families. To use this, build a separate BatchMutationBuilder
    *   for each CF, then send them all to this method.
    * @return a future that can contain [[org.apache.cassandra.finagle.thrift.TimedOutException]],
    *  [[org.apache.cassandra.finagle.thrift.UnavailableException]] or [[org.apache.cassandra.finagle.thrift.InvalidRequestException]]
    * @param batches a Seq of BatchMutationBuilders, each for a different CF. Their mutations will be merged and
    *   sent as one operation */
  def execute(batches: Seq[BatchMutationBuilder[_, _, _]]): Future[Void] = {
    if(batches.size == 0) return Future.void

    val mutations = new JHashMap[ByteBuffer, JMap[String, JList[thrift.Mutation]]]

    batches.map(_.mutations).foreach { ms =>
      for ((row, inner) <- ms) {
        if (!mutations.containsKey(row)) {
          mutations.put(row, new JHashMap[String, JList[thrift.Mutation]])
        }
        val oldRowMap = mutations.get(row)
        for ((cf, mutationList) <- inner) {
          if (!oldRowMap.containsKey(cf)) {
            oldRowMap.put(cf, new JArrayList[thrift.Mutation])
          }
          val oldList = oldRowMap.get(cf)
          oldList.addAll(mutationList)
        }
      }
    }

    val writeConsistency = batches.head.cf.writeConsistency
    provider.map { _.batch_mutate(mutations, writeConsistency.level) }
  }

  /**
   * Closes connections to the cluster for this keyspace.
   */
  def close() = provider.close()
}
