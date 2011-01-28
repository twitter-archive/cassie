package com.codahale.cassie.clocks


/**
 * A clock which returns a 64-bit timestamp.
 *
 * @author coda
 */
trait Clock {
  def timestamp: Long

  /** To conveniently get the singleton/Object from Java. */
  def get() = this
}
