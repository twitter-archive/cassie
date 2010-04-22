package com.codahale.cassie.tests


import org.scalatest.Spec
import org.scalatest.matchers.MustMatchers
import org.scalatest.mock.MockitoSugar
import com.codahale.cassie.client.ClientProvider
import com.codahale.cassie.codecs.Codec
import com.codahale.cassie.{ColumnFamilyImpl, Keyspace}

class KeyspaceTest extends Spec with MustMatchers with MockitoSugar {
  describe("a keyspace") {
    val provider = mock[ClientProvider]
    val keyspace = new Keyspace("MyApp", provider)

    it("builds a column family with the same ClientProvider") {
      val (columnCodec, valueCodec) = (mock[Codec[String]], mock[Codec[Long]])

      val cf = keyspace.columnFamily("People", columnCodec, valueCodec)
      cf.isInstanceOf[ColumnFamilyImpl[_, _]] must be(true)

      val cfi = cf.asInstanceOf[ColumnFamilyImpl[String, Long]]
      cfi.keyspace must equal("MyApp")
      cfi.name must equal("People")
      cfi.columnCodec must equal(columnCodec)
      cfi.valueCodec must equal(valueCodec)
      cfi.provider must equal(provider)
    }
  }
}
