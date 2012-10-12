package com.twitter.cassie.codecs

import com.twitter.cassie.types.Composite
import java.nio.ByteBuffer

object CompositeCodec {
  def apply[A](codec: Codec[A]) : CompositeCodec[A] = new CompositeCodec(codec)
}

class CompositeCodec[A](codec: Codec[A]) extends Codec[Composite[A]] {
  def encode(composite: Composite[A]) = composite.encode
  def decode(buf: ByteBuffer) = Composite.decode(buf, codec)
}
