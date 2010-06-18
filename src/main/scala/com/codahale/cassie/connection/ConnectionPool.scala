package com.codahale.cassie.connection

import org.apache.cassandra.thrift.Cassandra.Client

/**
 * A pool of connections providing access to Cassandra `Client` instances.
 *
 * @author coda
 */
trait ConnectionPool {

  /**
   * Returns `true` if the pool has no idle connections to lend.
   */
  def isEmpty: Boolean

  /**
   * Returns the total number of connections in the pool.
   */
  def size: Int

  /**
   * Removes all connections from the pool.
   */
  def clear()

  /**
   * Borrows a connection from the pool and passes it to a callback function.
   *
   * @param f a function which given a Cassandra `Client`, returns a value
   * @tparam A the query result type
   * @return if `f` was called successfully, `Some(f(client))`, otherwise `None`
   */
  def map[A](f: Client => A): Option[A]
}
