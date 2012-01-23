package com.twitter.cassie.clocks

/**
 * A clock which returns a 64-bit timestamp.
 */
trait Clock {
  def timestamp: Long

  /** To conveniently get the singleton/Object from Java. */
  def get() = this
}
