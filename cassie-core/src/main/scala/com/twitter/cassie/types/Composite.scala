package com.twitter.cassie.types

import java.nio.ByteBuffer
import scala.collection.mutable
import com.twitter.cassie.codecs.{ByteArrayCodec, Codec}

class Composite {

  private[this] var _components = new mutable.ListBuffer[Component[_]]

  def apply(idx: Int) = _components(idx)

  def get(idx: Int) = apply(idx)

  def length = _components.length

  def components = _components.toSeq

  def addComponent(component: Component[_]): Composite = {
    _components += component
    this
  }

  def encode: ByteBuffer = {
    val encodedComponents = _components.map(c => (c.encoded, c.equality)).toSeq
    //each component has 2-byte length + value + terminator
    val totalLength = encodedComponents.foldLeft(0) { (acc, c) => acc + 2 + c._1.remaining + 1 }
    val encoded = ByteBuffer.allocate(totalLength)

    encodedComponents.foreach { case (component, equality) =>
      val length = component.remaining
      // add length
      encoded.putShort(length.toShort)
      // add value
      encoded.put(component)
      // add terminator
      encoded.put(equality)
    }

    encoded.rewind
    encoded
  }
}

object Composite {

  def apply(components: Component[_]*): Composite = {
    val composite = new Composite
    components.foreach(composite.addComponent(_))
    composite
  }

  def decode(encoded: ByteBuffer): Composite = apply(getComponents(encoded).reverse:_*)

  private def getComponents(encoded: ByteBuffer, acc: Seq[Component[ByteBuffer]] = Seq()): Seq[Component[ByteBuffer]] =
    if (encoded.remaining == 0) acc
    else getComponents(encoded, componentFromByteBuffer(encoded) +: acc)

  private def componentFromByteBuffer(buf: ByteBuffer) = {
    val length = buf.getShort
    val value = new Array[Byte](length)
    buf.get(value)
    val equality = ComponentEquality.fromByte(buf.get)
    Component(ByteBuffer.wrap(value), ByteArrayCodec, equality)
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

case class Component[A](value: A, codec: Codec[A], equality: ComponentEquality.ComponentEquality = ComponentEquality.EQ) {
  val encoded: ByteBuffer = codec.encode(value)
}

case class DecodedComposite2[A, B](_1: Component[A], _2: Component[B])
case class DecodedComposite3[A, B, C](_1: Component[A], _2: Component[B], _3: Component[C])
case class DecodedComposite4[A, B, C, D](_1: Component[A], _2: Component[B], _3: Component[C], _4: Component[D])
case class DecodedComposite5[A, B, C, D, E](_1: Component[A], _2: Component[B], _3: Component[C], _4: Component[D], _5: Component[E])

object Decoder {

  def convert[T](c: Component[ByteBuffer], codec: Codec[T]) : Component[T] = Component(codec.decode(c.value), codec, c.equality)

  def apply[A, B](codecA: Codec[A], codecB: Codec[B]) = new {
    def decode(composite: Composite) : DecodedComposite2[A, B] =
      DecodedComposite2(convert(composite(0).asInstanceOf[Component[ByteBuffer]], codecA),
                        convert(composite(1).asInstanceOf[Component[ByteBuffer]], codecB))
  }

  def apply[A, B, C](codecA: Codec[A], codecB: Codec[B], codecC: Codec[C]) = new {
    def decode(composite: Composite) : DecodedComposite3[A, B, C] =
      DecodedComposite3(convert(composite(0).asInstanceOf[Component[ByteBuffer]], codecA),
                        convert(composite(1).asInstanceOf[Component[ByteBuffer]], codecB),
                        convert(composite(2).asInstanceOf[Component[ByteBuffer]], codecC))
  }

  def apply[A, B, C, D](codecA: Codec[A], codecB: Codec[B], codecC: Codec[C], codecD: Codec[D]) = new {
    def decode(composite: Composite) : DecodedComposite4[A, B, C, D] =
      DecodedComposite4(convert(composite(0).asInstanceOf[Component[ByteBuffer]], codecA),
                        convert(composite(1).asInstanceOf[Component[ByteBuffer]], codecB),
                        convert(composite(2).asInstanceOf[Component[ByteBuffer]], codecC),
                        convert(composite(3).asInstanceOf[Component[ByteBuffer]], codecD))
  }

  def apply[A, B, C, D, E](codecA: Codec[A], codecB: Codec[B], codecC: Codec[C], codecD: Codec[D], codecE: Codec[E]) = new {
    def decode(composite: Composite) : DecodedComposite5[A, B, C, D, E] =
      DecodedComposite5(convert(composite(0).asInstanceOf[Component[ByteBuffer]], codecA),
                        convert(composite(1).asInstanceOf[Component[ByteBuffer]], codecB),
                        convert(composite(2).asInstanceOf[Component[ByteBuffer]], codecC),
                        convert(composite(3).asInstanceOf[Component[ByteBuffer]], codecD),
                        convert(composite(4).asInstanceOf[Component[ByteBuffer]], codecE))
  }
}
