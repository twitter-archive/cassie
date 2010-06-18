package com.codahale.cassie.connection

import org.apache.commons.pool.impl.GenericObjectPool
import org.apache.cassandra.thrift.Cassandra.Client

/**
 * A dynamically-sized pool of connections.
 *
 * @param factory the [[com.codahale.cassie.connection.ConnectionFactory]] to
 *                be used for creating new [[com.codahale.connection.Connection]]s
 * @param min the minimum number of connections to maintain to the node
 * @param max the maximum number of connections to maintain to the ndoe
 * @param removeAfterIdleForMS the amount of time, in milliseconds, after which
 *                             idle connections should be closed and removed
 *                             from the pool
 * @author coda
 */
class BasicConnectionPool(val factory: ConnectionFactory,
                     val min: Int,
                     val max: Int,
                     val removeAfterIdleForMS: Int) extends ConnectionPool {
  protected val pool = {
    val p = new GenericObjectPool(factory)
    p.setMaxActive(max)
    p.setMaxIdle(max)
    p.setMinIdle(min)
    p.setMinEvictableIdleTimeMillis(removeAfterIdleForMS)
    p.setTimeBetweenEvictionRunsMillis(removeAfterIdleForMS)
    p.setWhenExhaustedAction(GenericObjectPool.WHEN_EXHAUSTED_FAIL)
    p.setTestWhileIdle(true)
    // initialize the pool
    (1 to min).map { _ => p.borrowObject }.foreach { o => p.returnObject(o) }
    p
  }

  def isEmpty = pool.getNumIdle == 0


  def size = pool.getNumActive + pool.getNumIdle

  def clear() {
    pool.clear()
  }

  /**
   * Borrows a connection from the pool and passes it to a callback function.
   *
   * If the query succeeds, the result is returned wrapped in a `Some` instance,
   * otherwise `None` is returned. If the pool is exhausted, returns `None`.
   *
   * @param f a function which given a Cassandra `Client`, returns a value
   * @tparam A the query result type
   * @return if `f` was called successfully, `Some(f(iface))`, otherwise `None`
   */
  def map[A](f: Client => A): Option[A] = {
    try {
      val connection = pool.borrowObject().asInstanceOf[Connection]
      try {
        return connection.map(f)
      } finally {
        pool.returnObject(connection)
      }
    } catch {
      case e: NoSuchElementException => None
    }
  }
}
