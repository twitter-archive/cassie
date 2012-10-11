package com.twitter.cassie.types

import java.nio.ByteBuffer
import scala.collection.mutable

class Composite {

  private var _components = new mutable.ListBuffer[Component]

  def apply(idx: Int) = _components(idx)

  def get(idx: Int) = apply(idx)

  def length = _components.length

  def components = _components.toSeq

  def addComponent(component: Component) : Composite = {
    _components += component
    this
  }

  def encode : ByteBuffer = {
    val totalLength = _components.foldLeft(0)(_ + 2 + _.value.remaining + 1)  //each component has 2-byte length + value + terminator
    val encoded = ByteBuffer.allocate(totalLength)

    _components.foreach { component =>
      val length = component.value.remaining
      // add length
      encoded.putShort(length.toShort)
      // add value
      encoded.put(component.value)
      // add terminator
      encoded.put(component.equality)
    }

    encoded.rewind
    encoded
  }
}

object Composite {

  def apply(components: Component*) : Composite = {
    val composite = new Composite
    components.foreach(composite.addComponent(_))
    composite
  }

  def decode(encoded: ByteBuffer) : Composite = apply(getComponents(encoded).reverse:_*)

  private def getComponents(encoded: ByteBuffer, acc: Seq[Component] = Seq()) : Seq[Component] =
    if (encoded.remaining == 0) acc
    else getComponents(encoded, componentFromByteBuffer(encoded) +: acc)

  private def componentFromByteBuffer(buf: ByteBuffer) = {
    val length = buf.getShort
    val value = new Array[Byte](length)
    buf.get(value)
    val equality = ComponentEquality.fromByte(buf.get)
    Component(ByteBuffer.wrap(value), equality)
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

case class Component(value: ByteBuffer, equality: ComponentEquality.ComponentEquality = ComponentEquality.EQ)
