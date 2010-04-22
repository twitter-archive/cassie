package com.codahale.cassie.clocks

/**
 * A clock which returns the time since Jan 1, 1970 UTC in microseconds.
 *
 * N.B.: This doesn't actually return microseconds, since few platforms actually
 * have reliable access to microsecond-accuracy clocks. What it does return is
 * the time in milliseconds, multiplied by 1000. That said, it *is* strictly
 * increasing, so that even if your calls to MicrosecondEpochClock#timestamp
 * occur within a single millisecond, the timestamps will be ordered
 * appropriately.
 *
 * @author coda
 */
object MicrosecondEpochClock extends StrictlyIncreasingClock {
  protected def tick = System.currentTimeMillis * 1000
}
