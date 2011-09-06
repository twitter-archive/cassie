package com.twitter.cassie.codecs

import java.nio.ByteBuffer
import scala.collection.JavaConversions.asScalaIterable
import java.util.{ArrayList => JArrayList, Set => JSet}

/**
  * A bidirection encoding for column names or values. */
trait Codec[A] {
  def encode(obj: A): ByteBuffer
  def decode(ary: ByteBuffer): A

  /** To conveniently get the singleton/Object from Java. */
  def get() = this

  /** Helpers for conversion from ByteBuffers to byte arrays. Keep explicit! */
  def b2b(buff: ByteBuffer): Array[Byte] = {
    val bytes = new Array[Byte](buff.remaining)
    buff.duplicate.get(bytes)
    bytes
  }
  def b2b(array: Array[Byte]): ByteBuffer = ByteBuffer.wrap(array)

  def encodeSet(values: JSet[A]) = {
    val output = new JArrayList[ByteBuffer](values.size)
    for (value <- asScalaIterable(values))
      output.add(encode(value))
    output
  }
}
