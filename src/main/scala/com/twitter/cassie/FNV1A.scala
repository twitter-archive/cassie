package com.twitter.cassie

/**
 * The FNV1-A 64-bit hashing algorithm.
 *
 * @author coda
 */
object FNV1A extends (Array[Byte] => Long) {
  private val offsetBasis = 0xcbf29ce484222325L
  private val prime = 0x100000001b3L

  def apply(ary: Array[Byte]): Long = {
    var n = offsetBasis
    var i = 0
    while (i < ary.length) {
      n = (n ^ ary(i)) * prime
      i += 1
    }
    n
  }
}
