package com.twitter.cassie.connection

import com.twitter.util.Future
import org.apache.cassandra.finagle.thrift.Cassandra.ServiceToClient

/**
  * A utility interface for classes which pass a Cassandra `Client` instance to
  * a function and return the result. */
trait ClientProvider {
  /**
    * Passes a Cassandra `ServiceToClient` instance to the given function and returns a
    * future which will be the client's response.
    *
    * @tparam A the result type
    * @param f the function to which the `ServiceToClient` is passed
    * @return `f(client)` */
  def map[A](f: ServiceToClient => Future[A]): Future[A]

  /**
    * Releases any resources held by the provider. */
  def close() = {}
}
