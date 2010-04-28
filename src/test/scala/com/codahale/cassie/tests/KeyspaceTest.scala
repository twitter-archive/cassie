package com.codahale.cassie.tests

import org.scalatest.Spec
import org.scalatest.matchers.MustMatchers
import org.scalatest.mock.MockitoSugar
import com.codahale.cassie.client.ClientProvider
import com.codahale.cassie.{ColumnFamily, Keyspace}
import com.codahale.cassie.codecs.Utf8Codec

class KeyspaceTest extends Spec with MustMatchers with MockitoSugar {
  describe("a keyspace") {
    val provider = mock[ClientProvider]
    val keyspace = new Keyspace("MyApp", provider)

    it("builds a column family with the same ClientProvider") {
      val cf = keyspace.columnFamily[String, String]("People")
      cf.isInstanceOf[ColumnFamily[_, _]] must be(true)

      val cfi = cf.asInstanceOf[ColumnFamily[String, String]]
      cfi.keyspace must equal("MyApp")
      cfi.name must equal("People")
      cfi.defaultNameCodec must equal(Utf8Codec)
      cfi.defaultValueCodec must equal(Utf8Codec)
      cfi.provider must equal(provider)
    }
  }
}
