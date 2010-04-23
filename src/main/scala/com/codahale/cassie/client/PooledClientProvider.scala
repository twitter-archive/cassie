package com.codahale.cassie.client

import org.apache.commons.pool.impl.GenericObjectPool

/**
 * A client provider which provides access to a pool of connections to nodes
 * throughout the cluster. 
 *
 * @author coda
 */
class PooledClientProvider(selector: HostSelector,
                           minIdle: Int,
                           maxActive: Int,
                           val maxRetry: Int)
        extends RetryingClientProvider {

  private val factory = new PooledClientFactory(selector)
  private val config = {
    val c = new GenericObjectPool.Config
    c.maxActive = maxActive
    c.maxIdle = maxActive
    c.minIdle = minIdle
    c.testWhileIdle = true
    c
  }
  protected val pool = new GenericObjectPool(factory, config)
}
