package com.twitter.cassie.codecs

import org.apache.thrift._
import org.apache.thrift.protocol._
import org.apache.thrift.transport._
import java.nio.ByteBuffer
import java.io._

class ThriftCodec[T <: TBase[_, _]](klass: Class[T]) extends Codec[T] {
  val thriftProtocolFactory = new TBinaryProtocol.Factory()

  def encode(t: T) = {
    val out = new ByteArrayOutputStream()
    val protocol = thriftProtocolFactory.getProtocol(new TIOStreamTransport(out))
    t.write(protocol)
    b2b(out.toByteArray)
  }

  def decode(ary: ByteBuffer) = {
    val protocol = thriftProtocolFactory.getProtocol(new TIOStreamTransport(new ByteArrayInputStream(b2b(ary))))
    val out = klass.newInstance
    out.read(protocol)
    out
  }
}
