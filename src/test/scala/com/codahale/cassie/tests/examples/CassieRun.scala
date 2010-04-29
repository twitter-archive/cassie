package com.codahale.cassie.tests.examples

import com.codahale.cassie._
import client.{RoundRobinHostSelector, PooledClientProvider, ClusterMap}
import clocks.MicrosecondEpochClock
import codecs.{VarInt, AsciiString, FixedLong}
import com.codahale.logula.Logging
import java.util.logging.Level

object CassieRun extends Logging {
  def main(args: Array[String]) {
    Logging.configure(Level.ALL)

    implicit val clock = MicrosecondEpochClock

    // pull down data about all the nodes in the cluster
    val map = new ClusterMap("localhost", 9160)

    // create a round-robin pool of 1-5 connections
    val selector = new RoundRobinHostSelector(map)
    val pool = new PooledClientProvider(selector, 1, 5, 10)

    // create a cluster
    val cluster = new Cluster(pool)

    // create a keyspace
    val keyspace = cluster.keyspace("Keyspace1")

    // create a column family
    val cass = keyspace.columnFamily[String, String]("Standard1")

    log.info("inserting some columns")
    cass.insert("yay for me", Column("name", "Coda"))
    cass.insert("yay for me", Column("motto", "Moar lean."))

    cass.insert("yay for you", Column("name", "Niki"))
    cass.insert("yay for you", Column("motto", "Told ya."))

    cass.insert("yay for us", Column("name", "Biscuit"))
    cass.insert("yay for us", Column("motto", "Mlalm."))

    cass.insert("yay for everyone", Column("name", "Louie"))
    cass.insert("yay for everyone", Column("motto", "Swish!"))

    log.info("getting a column: %s", cass.getColumn("yay for me", "name"))
    // Some(Column(name,Coda,1271789761374109))

    log.info("getting a column that doesn't exist: %s", cass.getColumn("yay for no one", "name"))
    // None

    log.info("getting a column that doesn't exist #2: %s", cass.getColumn("yay for no one", "oink"))
    // None

    log.info("getting a set of columns: %s", cass.getColumns("yay for me", Set("name", "motto")))
    // Map(motto -> Column(motto,Moar lean.,1271789761389735), name -> Column(name,Coda,1271789761374109))

    log.info("getting a whole row: %s", cass.getRow("yay for me"))
    // Map(motto -> Column(motto,Moar lean.,1271789761389735), name -> Column(name,Coda,1271789761374109))

    log.info("getting a column from a set of keys: %s", cass.multigetColumn(Set("yay for me", "yay for you"), "name"))
    // Map(yay for you -> Column(name,Niki,1271789761390785), yay for me -> Column(name,Coda,1271789761374109))

    log.info("getting a set of columns from a set of keys: %s", cass.multigetColumns(Set("yay for me", "yay for you"), Set("name", "motto")))
    // Map(yay for you -> Map(motto -> Column(motto,Told ya.,1271789761391366), name -> Column(name,Niki,1271789761390785)), yay for me -> Map(motto -> Column(motto,Moar lean.,1271789761389735), name -> Column(name,Coda,1271789761374109)))

    cass.getColumnAs[FixedLong, AsciiString]("key", 2)
    cass.insert("digits", Column[VarInt, VarInt](1, 300))

    log.info("Iterating!")
    for ((key, col) <- cass.rowIterator(2)) {
      log.info("Found: %s", col)
    }

    log.info("removing a column")
    cass.removeColumn("yay for me", "motto")

    log.info("removing a row")
    cass.removeRow("yay for me")

    log.info("Batching up some stuff")
    cass.batch() { cf =>
      cf.removeColumn("yay for you", "name")
      cf.removeColumns("yay for us", Set("name", "motto"))
      cf.insert("yay for nobody", Column("name", "Burt"))
      cf.insert("yay for nobody", Column("motto", "'S funny."))
      cf.insert("bits!", Column("motto", FixedLong(20019L)))
    }
  }
}
