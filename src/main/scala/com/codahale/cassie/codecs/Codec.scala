package com.codahale.cassie.codecs

/**
 * A bidirection encoding for column names or values.
 */
trait Codec[A] {
  def encode(obj: A) : Array[Byte]
  def decode(ary : Array[Byte]) : A
}
