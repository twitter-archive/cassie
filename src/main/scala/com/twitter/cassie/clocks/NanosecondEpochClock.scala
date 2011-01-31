package com.twitter.cassie.clocks

/**
 * A clock which returns the time since Jan 1, 1970 UTC in nanoseconds.
 *
 * N.B.: This doesn't actually return nanoseconds, since few platforms actually
 * have reliable access to microsecond-accuracy clocks. What it does return is
 * the time in nanoseconds, multiplied by 1000000. That said, it *is* strictly
 * increasing, so that even if your calls to NanosecondEpochClock#timestamp
 * occur within a single millisecond, the timestamps will be ordered
 * appropriately.
 *
 * @author coda
 */
object NanosecondEpochClock extends StrictlyIncreasingClock {
  protected def tick = System.currentTimeMillis * 1000000
}
