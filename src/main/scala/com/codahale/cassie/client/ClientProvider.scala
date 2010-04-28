package com.codahale.cassie.client

import org.apache.cassandra.thrift

trait ClientProvider {
  def map[A](f: thrift.Cassandra.Iface => A): A
}
