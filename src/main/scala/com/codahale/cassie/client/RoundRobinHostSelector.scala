package com.codahale.cassie.client

/**
 * A host selector which connects to all nodes in a cluster in round-robin
 * fashion.
 *
 * @author coda
 */
class RoundRobinHostSelector(clusterMap: ClusterMap) extends HostSelector {
  // TODO:  at what point should this re-query the cluster map?
  private val hosts = Iterator.continually(clusterMap.hosts().toArray.iterator).flatten

  def next = synchronized { hosts.next() }
}
