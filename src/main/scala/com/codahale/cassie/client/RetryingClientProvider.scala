package com.codahale.cassie.client

import org.apache.commons.pool.ObjectPool
import com.codahale.logula.Logging
import java.io.IOException
import org.apache.thrift.transport.TTransportException
import org.apache.cassandra.thrift.Cassandra.{Client, Iface}

/**
 * A client provider which retries commands, potentially recovering from dropped
 * network connections, etc.
 *
 * @author coda
 */
abstract class RetryingClientProvider extends ClientProvider with Logging {
  protected val pool: ObjectPool
  protected val maxRetry: Int

  def map[A](f: (Iface) => A): A = {
    var attempts = 0
    var lastError: Throwable = null
    while (attempts < maxRetry) {
      if (attempts > 0) {
        log.warning("Re-attempting command, try %d", attempts)
      } else {
        log.finer("Acquiring connection")
      }
      val client = pool.borrowObject().asInstanceOf[Client]
      try {
        val result = f(client)
        log.finer("Returning connection")
        pool.returnObject(client)
        return result
      } catch {
        case e: TTransportException =>
          lastError = e
          log.warning(e, "Attempt %d failed", attempts)
          pool.invalidateObject(client)
      }
      attempts += 1
    }
    throw new IOException("Aborting query after %d attempts".format(attempts), lastError)
  }
}
