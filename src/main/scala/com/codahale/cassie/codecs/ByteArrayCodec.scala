package com.codahale.cassie.codecs

/**
 * An identity encoding.
 *
 * @author coda
 */
object ByteArrayCodec extends Codec[Array[Byte]] {
  def encode(obj: Array[Byte]) = obj
  def decode(ary: Array[Byte]) = ary
}
