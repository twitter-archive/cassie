package com.twitter.cassie.mr

import org.apache.hadoop.mapreduce._
import org.apache.hadoop.io._
import java.nio.ByteBuffer
import com.twitter.cassie.codecs._
import com.twitter.cassie.clocks._
import scala.collection.JavaConversions._

class ColumnWritable extends ArrayWritable(classOf[BytesWritable]) {
  def this(name: ByteBuffer, value: ByteBuffer) = {
    this()
    set(name, value, System.currentTimeMillis)
  }
  
  def set(name: ByteBuffer, value: ByteBuffer, timestamp: Long) {
    val a = writable(name)
    val b = writable(value)
    val c = writable(FixedLongCodec.encode(timestamp))
    set(Array(a, b, c))
  }
  
  def name = getBuf(0)
  def value = getBuf(1)
  def timestamp = FixedLongCodec.decode(getBuf(2)).value
  
  def getBuf(i: Int) = {
    val bw = get()(i).asInstanceOf[BytesWritable]
    ByteBuffer.wrap(bw.getBytes(), 0, bw.getLength())
  }
  
  private def writable(buf: ByteBuffer) = {
    val out = new BytesWritable
    out.set(buf.array, buf.position, buf.limit - buf.position)
    out
  }
}
