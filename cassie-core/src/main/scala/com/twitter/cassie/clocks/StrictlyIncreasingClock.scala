package com.twitter.cassie.clocks

import java.util.concurrent.atomic.AtomicLong

/**
 * A concurrent, strictly-increasing clock.
 */
abstract class StrictlyIncreasingClock extends Clock {
  private val counter = new AtomicLong(tick)

  def timestamp: Long = {
    var newTime: Long = 0
    while (newTime == 0) {
      val last = counter.get
      val current = tick
      val next = if (current > last) current else last + 1
      if (counter.compareAndSet(last, next)) {
        newTime = next
      }
    }
    return newTime
  }

  protected def tick: Long
}
