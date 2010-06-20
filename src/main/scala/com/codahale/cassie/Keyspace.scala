package com.codahale.cassie

import codecs.Codec
import connection.ClientProvider

/**
 * A Cassandra keyspace.
 *
 * @author coda
 */
class Keyspace(val name: String, val provider: ClientProvider) {
  /**
   * Returns a ColumnFamily with the given name and column/value codecs.
   */
  def columnFamily[Name, Value](name: String,
                                defaultReadConsistency: ReadConsistency = ReadConsistency.Quorum,
                                defaultWriteConsistency: WriteConsistency = WriteConsistency.Quorum)
                               (implicit defaultNameCodec: Codec[Name],
                                         defaultValueCodec: Codec[Value]) =
    new ColumnFamily(this.name, name, provider, defaultNameCodec,
      defaultValueCodec, defaultReadConsistency, defaultWriteConsistency)
}
