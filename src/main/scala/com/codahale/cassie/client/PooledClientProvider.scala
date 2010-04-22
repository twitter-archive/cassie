package com.codahale.cassie.client

import org.apache.commons.pool.impl.GenericObjectPool
import org.apache.cassandra.thrift.Cassandra.Client
import org.apache.thrift.transport.TTransportException
import java.io.IOException
import com.codahale.logula.Logging

/**
 * A client provider which provides access to a pool of connections to nodes
 * throughout the cluster. 
 *
 * @author coda
 */
class PooledClientProvider(selector: HostSelector,
                           minIdle: Int,
                           maxActive: Int,
                           maxRetry: Int)
        extends ClientProvider with Logging {

  private val factory = new PooledClientFactory(selector)
  private val config = {
    val c = new GenericObjectPool.Config
    c.maxActive = maxActive
    c.maxIdle = maxActive
    c.minIdle = minIdle
    c.testWhileIdle = true
    c
  }
  private val pool = new GenericObjectPool(factory, config)

  def map[A](f: (Client) => A): A = {
    var attempts = 0
    while (attempts < maxRetry) {
      if (attempts > 0) {
        log.warning("Re-attempting command, try %d", attempts)
      } else {
        log.finer("Acquiring connection")
      }
      val client = pool.borrowObject().asInstanceOf[Client]
      try {
        try {
          return f(client)
        } finally {
          log.finer("Returning connection")
          pool.returnObject(client)
        }
      } catch {
        case e: TTransportException =>
          log.warning(e, "Attempt %d failed", attempts)
          pool.invalidateObject(client)
        case e: IOException =>
          log.warning(e, "Attempt %d failed", attempts)
          pool.invalidateObject(client)
      }
      attempts += 1
    }
    throw new IOException("Aborting query after %d attempts".format(attempts))
  }
}
