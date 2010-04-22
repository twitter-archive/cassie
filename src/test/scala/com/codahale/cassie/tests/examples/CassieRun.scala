package com.codahale.cassie.tests.examples


import com.codahale.cassie._
import client.{RoundRobinHostSelector, SingleClientProvider, PooledClientProvider, ClusterMap}
import clocks.MicrosecondEpochClock
import codecs.Utf8Codec
import com.codahale.logula.Logging
import java.util.logging.Level
import java.net.InetSocketAddress

object CassieRun extends Logging {
  def main(args: Array[String]) {
    Logging.configure(Level.INFO)

    implicit val clock = MicrosecondEpochClock

    // pull down data about all the nodes in the cluster
    val seedProvider = new SingleClientProvider(new InetSocketAddress("localhost", 9160))
    val map = new ClusterMap(seedProvider, 9160)

    // create a round-robin pool of 1-5 connections
    val selector = new RoundRobinHostSelector(map)
    val pool = new PooledClientProvider(selector, 1, 5, 10)

    // create a cluster
    val cluster = new Cluster(pool)

    // create a keyspace
    val keyspace = cluster.keyspace("Keyspace1")

    // create a column family
    val cass = keyspace.columnFamily("Standard1", Utf8Codec, Utf8Codec)

    log.info("inserting some columns")
    cass.insert("yay for me", Column("name", "Coda"), WriteConsistency.Quorum)
    cass.insert("yay for me", Column("motto", "Moar lean."), WriteConsistency.Quorum)

    cass.insert("yay for you", Column("name", "Niki"), WriteConsistency.Quorum)
    cass.insert("yay for you", Column("motto", "Told ya."), WriteConsistency.Quorum)

    cass.insert("yay for us", Column("name", "Biscuit"), WriteConsistency.Quorum)
    cass.insert("yay for us", Column("motto", "Mlalm."), WriteConsistency.Quorum)

    cass.insert("yay for everyone", Column("name", "Louie"), WriteConsistency.Quorum)
    cass.insert("yay for everyone", Column("motto", "Swish!"), WriteConsistency.Quorum)

    log.info("getting a column: %s", cass.get("yay for me", "name", ReadConsistency.Quorum))
    // Some(Column(name,Coda,1271789761374109))

    log.info("getting a column that doesn't exist: %s", cass.get("yay for no one", "name", ReadConsistency.Quorum))
    // None

    log.info("getting a column that doesn't exist #2: %s", cass.get("yay for no one", "oink", ReadConsistency.Quorum))
    // None

    log.info("getting a set of columns: %s", cass.get("yay for me", Set("name", "motto"), ReadConsistency.Quorum))
    // Map(motto -> Column(motto,Moar lean.,1271789761389735), name -> Column(name,Coda,1271789761374109))

    log.info("getting a whole row: %s", cass.get("yay for me", ReadConsistency.Quorum))
    // Map(motto -> Column(motto,Moar lean.,1271789761389735), name -> Column(name,Coda,1271789761374109))

    log.info("getting a column from a set of keys: %s", cass.multiget(Set("yay for me", "yay for you"), "name", ReadConsistency.Quorum))
    // Map(yay for you -> Column(name,Niki,1271789761390785), yay for me -> Column(name,Coda,1271789761374109))

    log.info("getting a set of columns from a set of keys: %s", cass.multiget(Set("yay for me", "yay for you"), Set("name", "motto"), ReadConsistency.Quorum))
    // Map(yay for you -> Map(motto -> Column(motto,Told ya.,1271789761391366), name -> Column(name,Niki,1271789761390785)), yay for me -> Map(motto -> Column(motto,Moar lean.,1271789761389735), name -> Column(name,Coda,1271789761374109)))

    log.info("removing a column")
    cass.remove("yay for me", "motto", WriteConsistency.Quorum)

    log.info("removing a row")
    cass.remove("yay for me", WriteConsistency.Quorum)
  }
}
