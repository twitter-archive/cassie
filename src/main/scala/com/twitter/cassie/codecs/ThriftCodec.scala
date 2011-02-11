package com.twitter.cassie.codecs

import org.apache.thrift._
import org.apache.thrift.protocol._
import org.apache.thrift.transport._
import java.nio.ByteBuffer
import java.io._

class ThriftCodec[T <: TBase[_, _]](klass: Class[T]) extends Codec[T] {
  val thriftProtocolFactory = new TBinaryProtocol.Factory()
  val outputStream = new ByteArrayOutputStream()
  val outputProtocol = thriftProtocolFactory.getProtocol(new TIOStreamTransport(outputStream))
  val inputStream = new ByteArrayInputStream(Array.empty[Byte]) {
    def refill(ary: Array[Byte]) {
      buf = ary
      pos = 0
      mark = 0
      count = buf.length
    }
  }
  val inputProtocol = thriftProtocolFactory.getProtocol(new TIOStreamTransport(inputStream))
  
  def encode(t: T) = {
    outputStream.reset
    t.write(outputProtocol)
    b2b(outputStream.toByteArray)
  }

  def decode(ary: ByteBuffer) = {
    inputStream.refill(b2b(ary))
    val out = klass.newInstance
    out.read(inputProtocol)
    out
  }
}
