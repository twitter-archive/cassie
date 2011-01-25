package com.codahale.cassie.tests

import org.scalatest.Spec
import org.scalatest.matchers.MustMatchers
import org.scalatest.mock.MockitoSugar
import com.codahale.cassie.codecs.Utf8Codec
import com.codahale.cassie.{WriteConsistency, ReadConsistency, Keyspace}
import com.codahale.cassie.connection.ClientProvider

class KeyspaceTest extends Spec with MustMatchers with MockitoSugar {
  describe("a keyspace") {
    val provider = mock[ClientProvider]
    val keyspace = new Keyspace("MyApp", provider)

    it("builds a column family with the same ClientProvider") {
      val cf = keyspace.columnFamily[String, String, String]("People")
      cf.keyspace must equal("MyApp")
      cf.name must equal("People")
      cf.defaultReadConsistency must equal(ReadConsistency.Quorum)
      cf.defaultWriteConsistency must equal(WriteConsistency.Quorum)
      cf.defaultNameCodec must equal(Utf8Codec)
      cf.defaultValueCodec must equal(Utf8Codec)
      cf.provider must equal(provider)
    }
  }
}
