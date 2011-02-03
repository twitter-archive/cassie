package com.twitter.cassie

import clocks.Clock
import codecs.Codec
import connection.ClientProvider

/**
 * A Cassandra keyspace, which maintains a connection pool.
 *
 * @param provider a [[com.twitter.cassie.connection.ClientProvider]] instance
 * @author coda
 */
class Keyspace(val name: String, val provider: ClientProvider) {
  /**
   * Returns a ColumnFamily with the given name and column/value codecs.
   */
  def columnFamily[Key, Name, Value](name: String, clock: Clock)
    (implicit defaultKeyCodec: Codec[Key],
              defaultNameCodec: Codec[Name],
              defaultValueCodec: Codec[Value]) =
    new ColumnFamily(this.name, name, provider, clock, defaultKeyCodec, defaultNameCodec, defaultValueCodec)
  
  /**
   * Closes connections to the cluster for this keyspace.
   */
  def close() = provider.close()
}
