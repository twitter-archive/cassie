package com.codahale.cassie

import client.ClientProvider
import codecs.Codec

/**
 * A Cassandra keyspace.
 *
 * @author coda
 */
class Keyspace(val name: String, val provider: ClientProvider) {
  /**
   * Returns a ColumnFamily with the given name and column/value codecs.
   */
  def columnFamily[A, B](name: String,
                         columnCodec: Codec[A],
                         valueCodec: Codec[B]): ColumnFamily[A, B] =
    new ColumnFamily(this.name, name, provider, columnCodec, valueCodec)
}
