package com.twitter.cassie.codecs.tests

import com.twitter.cassie.codecs.tests.ByteBufferLiteral._
import java.nio.charset.Charset
import java.nio.{ ByteBuffer, CharBuffer }
import org.scalacheck._
import org.scalatest.matchers.MustMatchers
import org.scalatest.prop.Checkers
import org.scalatest.Spec
import org.scalacheck.util.Buildable.buildableArray

// lifted from http://blog.zilverline.com/2011/04/07/serializing-strings-unicode-and-randomized-testing-using-scalacheck/
class CodecTest extends Spec with MustMatchers with Checkers {
  val UnicodeLeadingSurrogate = '\uD800' to '\uDBFF'
  val UnicodeTrailingSurrogate = '\uDC00' to '\uDFFF'
  val UnicodeBasicMultilingualPlane = ('\u0000' to '\uFFFF').diff(UnicodeLeadingSurrogate).diff(UnicodeTrailingSurrogate)

  val unicodeCharacterBasicMultilingualPlane: Gen[String] = Gen.oneOf(UnicodeBasicMultilingualPlane).map(_.toString)
  val unicodeCharacterSupplementaryPlane: Gen[String] = for {
    c1 <- Gen.oneOf(UnicodeLeadingSurrogate)
    c2 <- Gen.oneOf(UnicodeTrailingSurrogate)
  } yield {
    c1.toString + c2.toString
  }

  val unicodeCharacter = Gen.frequency(
    9 -> unicodeCharacterBasicMultilingualPlane,
    1 -> unicodeCharacterSupplementaryPlane)

  val unicodeString = Gen.listOf(unicodeCharacter).map(_.mkString)

  val bytesGen: Gen[Byte] = for {
    b <- Gen.choose(0, 255)
  } yield b.toByte

  val randomBuffer = Gen.containerOf[Array, Byte](bytesGen).map(ByteBuffer.wrap(_))

  implicit override val generatorDrivenConfig =
    PropertyCheckConfig(minSuccessful = 10000)
}