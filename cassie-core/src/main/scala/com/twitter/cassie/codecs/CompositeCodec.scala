package com.twitter.cassie.codecs

import com.twitter.cassie.types.Composite
import java.nio.ByteBuffer

object CompositeCodec extends Codec[Composite] {

  def encode(composite: Composite) = composite.encode
  def decode(buf: ByteBuffer) = Composite.decode(buf)

}
