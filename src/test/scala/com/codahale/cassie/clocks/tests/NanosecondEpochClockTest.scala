package com.codahale.cassie.clocks.tests

import org.scalatest.Spec
import org.scalatest.matchers.MustMatchers
import com.codahale.cassie.clocks.NanosecondEpochClock

class NanosecondEpochClockTest extends Spec with MustMatchers {
  describe("the nanoseconds clock") {
    it("uses the Java epoch milliseconds clock") {
      NanosecondEpochClock.timestamp must be((System.currentTimeMillis * 1000000) plusOrMinus(1000000))
    }

    it("is strictly increasing, even beyond the precision of the clock") {
      val timestamps = 1.to(40).map { c => NanosecondEpochClock.timestamp }

      timestamps.sortWith { _ < _ } must equal(timestamps)
      timestamps.toSet.size must equal(timestamps.size)
    }
  }
}
