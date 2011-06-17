package com.twitter.cassie.tests.examples

import com.twitter.cassie._
import com.twitter.cassie.codecs.Utf8Codec
import com.twitter.logging.Logger
import types.{LexicalUUID, VarInt, AsciiString, FixedLong}
// TODO: unfortunate
import scala.collection.JavaConversions._

object CassieRun {
  val log = Logger.get

  def main(args: Array[String]) {
    // create a cluster with a single seed from which to map keyspaces
    val cluster = new Cluster("localhost")

    // create a keyspace
    val keyspace = cluster.keyspace("Keyspace1")
      .retryAttempts(5)
      .requestTimeoutInMS(5000)
      .minConnectionsPerHost(1)
      .maxConnectionsPerHost(10)
      .removeAfterIdleForMS(60000)
      .connect()

    // create a column family
    val cass = keyspace.columnFamily[String, String, String]("Standard1", Utf8Codec, Utf8Codec, Utf8Codec)

    log.info("inserting some columns")
    cass.insert("yay for me", Column("name", "Coda")).apply()
    cass.insert("yay for me", Column("motto", "Moar lean.")).apply()

    cass.insert("yay for you", Column("name", "Niki")).apply()
    cass.insert("yay for you", Column("motto", "Told ya.")).apply()

    cass.insert("yay for us", Column("name", "Biscuit")).apply()
    cass.insert("yay for us", Column("motto", "Mlalm.")).apply()

    cass.insert("yay for everyone", Column("name", "Louie")).apply()
    cass.insert("yay for everyone", Column("motto", "Swish!")).apply()

    log.info("getting a column: %s", cass.getColumn("yay for me", "name").apply())
    // Some(Column(name,Coda,1271789761374109))

    log.info("getting a column that doesn't exist: %s", cass.getColumn("yay for no one", "name").apply())
    // None

    log.info("getting a column that doesn't exist #2: %s", cass.getColumn("yay for no one", "oink").apply())
    // None

    log.info("getting a set of columns: %s", cass.getColumns("yay for me", Set("name", "motto")).apply())
    // Map(motto -> Column(motto,Moar lean.,1271789761389735), name -> Column(name,Coda,1271789761374109))

    log.info("getting a whole row: %s", cass.getRow("yay for me").apply())
    // Map(motto -> Column(motto,Moar lean.,1271789761389735), name -> Column(name,Coda,1271789761374109))

    log.info("getting a column from a set of keys: %s", cass.multigetColumn(Set("yay for me", "yay for you"), "name").apply())
    // Map(yay for you -> Column(name,Niki,1271789761390785), yay for me -> Column(name,Coda,1271789761374109))

    log.info("getting a set of columns from a set of keys: %s", cass.multigetColumns(Set("yay for me", "yay for you"), Set("name", "motto")).apply())
    // Map(yay for you -> Map(motto -> Column(motto,Told ya.,1271789761391366), name -> Column(name,Niki,1271789761390785)), yay for me -> Map(motto -> Column(motto,Moar lean.,1271789761389735), name -> Column(name,Coda,1271789761374109)))

    // drop some UUID sauce on things
    cass.insert(LexicalUUID(cass.clock), Column("yay", "boo")).apply()

    log.info("Iterating!")
    for ((key, col) <- cass.rowIteratee(2)) {
      log.info("Found: %s", col)
    }

    log.info("removing a column")
    cass.removeColumn("yay for me", "motto").apply()

    log.info("removing a row")
    cass.removeRow("yay for me").apply()

    log.info("Batching up some stuff")
    cass.batch()
      .removeColumn("yay for you", "name")
      .removeColumns("yay for us", Set("name", "motto"))
      .insert("yay for nobody", Column("name", "Burt"))
      .insert("yay for nobody", Column("motto", "'S funny."))
      .execute().apply()

    log.info("Wrappin' up");
    keyspace.close();
  }
}
