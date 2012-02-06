// Copyright 2012 Twitter, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
// http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.twitter.cassie.hadoop

import com.twitter.cassie.codecs._
import java.nio.ByteBuffer
import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce._
import scala.collection.JavaConversions._

class ColumnWritable extends ArrayWritable(classOf[BytesWritable]) {
  def this(name: ByteBuffer, value: ByteBuffer) = {
    this()
    set(name, value, System.currentTimeMillis)
  }

  def set(name: ByteBuffer, value: ByteBuffer, timestamp: Long) {
    val a = writable(name)
    val b = writable(value)
    val c = writable(LongCodec.encode(timestamp))
    set(Array(a, b, c))
  }

  def name = getBuf(0)
  def value = getBuf(1)
  def timestamp = LongCodec.decode(getBuf(2))

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
