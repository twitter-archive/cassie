package com.twitter.cassie

import org.apache.cassandra.finagle.thrift

/**
 * The level of consistency required for a write operation.
 */
sealed case class WriteConsistency(level: thrift.ConsistencyLevel) {
  override def toString = "WriteConsistency." +
    level.toString.toLowerCase.capitalize
}

object WriteConsistency {
  /**
   * Ensure that the write has been written to at least 1 node, including hinted
   * recipients.
   */
  val Any = WriteConsistency(thrift.ConsistencyLevel.ANY)

  /**
   * Ensure that the write has been written to at least 1 node's commit log and
   * memory table before responding to the client.
   */
  val One = WriteConsistency(thrift.ConsistencyLevel.ONE)

  /**
   * Ensure that the write has been written to ReplicationFactor / 2 + 1 nodes
   * before responding to the client.
   */
  val Quorum = WriteConsistency(thrift.ConsistencyLevel.QUORUM)

  /**
   * Returns the record with the most recent timestamp once a majority of replicas within
   * the local datacenter have replied. Requres NetworkTopologyStrategy on the server side.
   */
  val LocalQuorum = WriteConsistency(thrift.ConsistencyLevel.LOCAL_QUORUM)

  /**
   * Returns the record with the most recent timestamp once a majority of replicas within
   * each datacenter have replied.
   */
  val EachQuorum = WriteConsistency(thrift.ConsistencyLevel.EACH_QUORUM)

  /**
   * Ensure that the write is written to all ReplicationFactor nodes before
   * responding to the client. Any unresponsive nodes will fail the operation.
   */
  val All = WriteConsistency(thrift.ConsistencyLevel.ALL)
}
