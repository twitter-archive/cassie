package com.codahale.cassie.connection

import org.apache.cassandra.thrift.Cassandra.Client

/**
 * A utility interface for classes which pass a Cassandra `Client` instance to
 * a function and return the result.
 */
trait ClientProvider {
  /**
   * Passes a Cassandra `Client` instance to the given function and returns the
   * function's result.
   *
   * @tparam A the result type
   * @param f the function to which the `Client` is passed
   * @return `f(client)`
   */
  def map[A](f: Client => A): A
}
