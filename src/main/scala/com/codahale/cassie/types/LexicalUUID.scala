package com.codahale.cassie.types

import com.codahale.cassie.clocks.Clock

object LexicalUUID {
  /**
   * Given a worker ID and a clock, generates a new LexicalUUID. If each node
   * has unique worker ID and a clock which is guaranteed to never go backwards,
   * then each generated UUID will be unique. 
   */
  def apply(workerID: Long)(implicit clock: Clock): LexicalUUID =
    LexicalUUID(clock.timestamp, workerID)
}

/**
 * A 128-bit UUID, composed of a 64-bit timestamp and a 64-bit worker ID.
 *
 * @author coda
 */
case class LexicalUUID(timestamp: Long, workerID: Long) {
  override def toString = {
    val hex = "%016x%016x".format(timestamp, workerID)
    "LexicalUUID(%s-%s-%s-%s)".format(hex.substring(0, 8), hex.substring(8, 12),
                                      hex.substring(12, 16), hex.substring(16, 32))
  }
}
