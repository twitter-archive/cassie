package com.codahale.cassie

import codecs.Codec
import connection.ClientProvider

/**
 * A Cassandra keyspace.
 *
 * @param provider a [[com.codahale.cassie.connection.ClientProvider]] instance
 * @author coda
 */
class Keyspace(val name: String, val provider: ClientProvider) {
  /**
   * Returns a ColumnFamily with the given name and column/value codecs.
   */
  def columnFamily[Key, Name, Value](name: String,
                                     readConsistency: ReadConsistency = ReadConsistency.Quorum,
                                     writeConsistency: WriteConsistency = WriteConsistency.Quorum)
                                    (implicit defaultKeyCodec: Codec[Key],
                                              defaultNameCodec: Codec[Name],
                                              defaultValueCodec: Codec[Value]) =
    new ColumnFamily(this.name, name, provider, readConsistency, writeConsistency,
      defaultKeyCodec, defaultNameCodec, defaultValueCodec)
}
