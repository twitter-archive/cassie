package com.twitter.cassie.codecs

import org.apache.thrift._
import org.apache.thrift.protocol._
import org.apache.thrift.transport._
import java.nio.ByteBuffer
import java.io._

class ThriftCodec[T <: TBase[_, _]](klass: Class[T]) extends Codec[T] {

  class ThreadLocal[T](init: => T) extends java.lang.ThreadLocal[T] {
    override def initialValue: T = init
  }
  implicit def getThreadLocal[S](tl: ThreadLocal[S]): S = tl.get

  val thriftProtocolFactory = new ThreadLocal(new TBinaryProtocol.Factory())
  val outputStream = new ThreadLocal(new ByteArrayOutputStream())
  val outputProtocol = new ThreadLocal(thriftProtocolFactory.getProtocol(new TIOStreamTransport(outputStream)))
  val inputStream = new ThreadLocal(new ByteArrayInputStream(Array.empty[Byte]) {
    def refill(ary: Array[Byte]) {
      buf = ary
      pos = 0
      mark = 0
      count = buf.length
    }
  })
  val inputProtocol = new ThreadLocal(thriftProtocolFactory.getProtocol(new TIOStreamTransport(inputStream)))

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
