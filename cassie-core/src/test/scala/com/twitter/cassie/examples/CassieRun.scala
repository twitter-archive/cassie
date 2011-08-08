package com.twitter.cassie.tests.examples

import com.twitter.cassie._
import com.twitter.cassie.codecs.Utf8Codec
import com.twitter.logging.Logger
import types.LexicalUUID
// TODO: unfortunate
import scala.collection.JavaConversions._

object CassieRun {
  val log = Logger.get

  def main(args: Array[String]) {
    // create a cluster with a single seed from which to map keyspaces
    val cluster = new Cluster("localhost")

    // create a keyspace object (does nothing on the server)
    val keyspace = cluster.keyspace("Keyspace1").connect()

    // create a column family object (does nothing on the server)
    val cass = keyspace.columnFamily("Standard1", Utf8Codec, Utf8Codec, Utf8Codec)

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
    log.info("getting a column that doesn't exist: %s", cass.getColumn("yay for no one", "name").apply())
    log.info("getting a column that doesn't exist #2: %s", cass.getColumn("yay for no one", "oink").apply())
    log.info("getting a set of columns: %s", cass.getColumns("yay for me", Set("name", "motto")).apply())
    log.info("getting a whole row: %s", cass.getRow("yay for me").apply())
    log.info("getting a column from a set of keys: %s", cass.multigetColumn(Set("yay for me", "yay for you"), "name").apply())
    log.info("getting a set of columns from a set of keys: %s", cass.multigetColumns(Set("yay for me", "yay for you"), Set("name", "motto")).apply())

    log.info("Iterating!")
    val f = cass.rowsIteratee(2).foreach { case(key, cols) =>
      log.info("Found: %s %s", key, cols)
    }
    f()

    val f2 = cass.columnsIteratee(2, "yay for me").foreach { col =>
      log.info("Found Columns Iteratee: %s", col)
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
