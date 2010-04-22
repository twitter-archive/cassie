package com.codahale.cassie.client

import java.util.Random

/**
 * A host selector which chooses a node from a cluster randomly.
 *
 */
class RandomHostSelector(clusterMap: ClusterMap) extends HostSelector {
  // TODO:  at what point should this re-query the cluster map?
  private val hosts = clusterMap.hosts().toArray
  private val prng = new Random


  def next = hosts(prng.nextInt(hosts.length))
}


