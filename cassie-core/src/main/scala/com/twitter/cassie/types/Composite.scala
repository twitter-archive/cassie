package com.twitter.cassie.types

import java.nio.ByteBuffer
import scala.collection.mutable
import com.twitter.cassie.codecs.Codec

class Composite[A](codec: Codec[A]) {

  private var _components = new mutable.ListBuffer[Component[A]]

  def apply(idx: Int) = _components(idx)

  def get(idx: Int) = apply(idx)

  def length = _components.length

  def components = _components.toSeq

  def addComponent(component: Component[A]) : Composite[A] = {
    _components += component
    this
  }

  def encode : ByteBuffer = {
    //each component has 2-byte length + value + terminator
    val totalLength = _components.foldLeft(0) { (acc, c) => acc + 2 + codec.encode(c.value).remaining + 1 }
    val encoded = ByteBuffer.allocate(totalLength)

    _components.foreach { component =>
      val encComp = codec.encode(component.value)
      val length = encComp.remaining
      // add length
      encoded.putShort(length.toShort)
      // add value
      encoded.put(encComp)
      // add terminator
      encoded.put(component.equality)
    }

    encoded.rewind
    encoded
  }
}

object Composite {

  def apply[A](codec: Codec[A], components: Component[A]*) : Composite[A] = {
    val composite = new Composite(codec)
    components.foreach(composite.addComponent(_))
    composite
  }

  def decode[A](encoded: ByteBuffer, codec: Codec[A]) : Composite[A] = apply(codec, getComponents(encoded, codec).reverse:_*)

  private def getComponents[A](encoded: ByteBuffer, codec: Codec[A], acc: Seq[Component[A]] = Seq()) : Seq[Component[A]] =
    if (encoded.remaining == 0) acc
    else getComponents(encoded, codec, componentFromByteBuffer[A](encoded, codec) +: acc)

  private def componentFromByteBuffer[A](buf: ByteBuffer, codec: Codec[A]) = {
    val length = buf.getShort
    val value = new Array[Byte](length)
    buf.get(value)
    val equality = ComponentEquality.fromByte(buf.get)
    Component[A](codec.decode(ByteBuffer.wrap(value)), equality)
  }

}

object ComponentEquality {

  type ComponentEquality = Byte
  val EQ:Byte = 0x0
  val LTE:Byte = -0x1
  val GTE:Byte = 0x1

  def fromByte(b: Byte) : ComponentEquality = {
    if (b > 0) GTE
    else if (b < 0) LTE
    else EQ
  }

}

case class Component[A](value: A, equality: ComponentEquality.ComponentEquality = ComponentEquality.EQ)
