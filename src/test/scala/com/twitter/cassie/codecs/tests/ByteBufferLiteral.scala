package com.twitter.cassie.codecs.tests

import java.nio.ByteBuffer

/** Sugar for ByteBuffer literals: obviously not intended to be performant. */
object ByteBufferLiteral {
  def bb(arr: Byte*): ByteBuffer = {
    val buf = ByteBuffer.allocate(arr.length)
    for (byte <- arr) buf.put(byte)
    buf.rewind
    buf
  }
}
