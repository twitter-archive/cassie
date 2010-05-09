package com.codahale.cassie.types

import com.codahale.cassie.clocks.Clock
import java.net.InetAddress.{getLocalHost => localHost}
import org.apache.commons.codec.binary.Hex.{decodeHex => decode}
import java.nio.ByteBuffer
import com.codahale.cassie.FNV1A

object LexicalUUID {
  private val defaultWorkerID = FNV1A(localHost.getHostName.getBytes)

  /**
   * Given a worker ID and a clock, generates a new LexicalUUID. If each node
   * has unique worker ID and a clock which is guaranteed to never go backwards,
   * then each generated UUID will be unique. 
   */
  def apply(workerID: Long)(implicit clock: Clock): LexicalUUID =
    LexicalUUID(clock.timestamp, workerID)

  /**
   * Given a clock, generates a new LexicalUUID, using a hash of the machine's
   * hostname as a worker ID.
   */
  def apply()(implicit clock: Clock): LexicalUUID = {
    LexicalUUID(clock.timestamp, defaultWorkerID)
  }

  /**
   * Given a UUID formatted as a string, returns it as a LexicalUUID.
   */
  def apply(uuid: String): LexicalUUID = {
    val buf = ByteBuffer.wrap(decode(uuid.toCharArray.filterNot { _ == '-' }))
    LexicalUUID(buf.getLong(), buf.getLong())
  }
}

/**
 * A 128-bit UUID, composed of a 64-bit timestamp and a 64-bit worker ID.
 *
 * @author coda
 */
case class LexicalUUID(timestamp: Long, workerID: Long) {
  override def toString = {
    val hex = "%016x%016x".format(timestamp, workerID)
    "%s-%s-%s-%s".format(hex.substring( 0,  8), hex.substring( 8, 12),
                         hex.substring(12, 16), hex.substring(16, 32))
  }
}
