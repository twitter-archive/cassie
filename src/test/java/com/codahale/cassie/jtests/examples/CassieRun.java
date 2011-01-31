package com.codahale.cassie.jtests.examples;

import java.util.Arrays;
import java.util.HashSet;

import scala.Tuple2;

import com.codahale.cassie.*;
import com.codahale.cassie.clocks.MicrosecondEpochClock;
import com.codahale.cassie.types.*;
import com.codahale.cassie.codecs.*;

  // FIXME: Logula is not Java friendly
import com.codahale.logula.Logging;
import com.codahale.logula.Log;
import org.apache.log4j.Level;

public final class CassieRun {
  private final static Log log = Log.forClass(CassieRun.class);
 
  public static <V> HashSet<V> Set(V... values) {
    return new HashSet<V>(Arrays.asList(values));
  }

  public static void info(String str) {
    log.info(str, new scala.collection.mutable.ArraySeq(1));
  }

  public static void main(String[] args) throws Exception {
    // create a cluster with a single seed from which to map keyspaces
    Cluster cluster = new Cluster("localhost");

    // create a keyspace
    Keyspace keyspace = cluster.keyspace("Keyspace1")
      .retryAttempts(5)
      .readTimeoutInMS(5000)
      .minConnectionsPerHost(1)
      .maxConnectionsPerHost(10)
      .removeAfterIdleForMS(60000)
      .connect();

    // create a column family
    ColumnFamily<String, String, String> cass = keyspace.columnFamily("Standard1", MicrosecondEpochClock.get(), Utf8Codec.get(), Utf8Codec.get(), Utf8Codec.get());

    info("inserting some columns");
    cass.insert("yay for me", cass.newColumn("name", "Coda"));
    cass.insert("yay for me", cass.newColumn("motto", "Moar lean."));

    cass.insert("yay for you", cass.newColumn("name", "Niki"));
    cass.insert("yay for you", cass.newColumn("motto", "Told ya."));

    cass.insert("yay for us", cass.newColumn("name", "Biscuit"));
    cass.insert("yay for us", cass.newColumn("motto", "Mlalm."));

    cass.insert("yay for everyone", cass.newColumn("name", "Louie"));
    cass.insert("yay for everyone", cass.newColumn("motto", "Swish!"));

    info("getting a column: " + cass.getColumn("yay for me", "name"));
    // Some(Column(name,Coda,1271789761374109))

    info("getting a column that doesn't exist: " + cass.getColumn("yay for no one", "name"));
    // None

    info("getting a column that doesn't exist #2: " + cass.getColumn("yay for no one", "oink"));
    // None

    info("getting a set of columns: " + cass.getColumns("yay for me", Set("name", "motto")));
    // Map(motto -> Column(motto,Moar lean.,1271789761389735), name -> Column(name,Coda,1271789761374109))

    info("getting a whole row: " + cass.getRow("yay for me"));
    // Map(motto -> Column(motto,Moar lean.,1271789761389735), name -> Column(name,Coda,1271789761374109))

    info("getting a column from a set of keys: " + cass.multigetColumn(Set("yay for me", "yay for you"), "name"));
    // Map(yay for you -> Column(name,Niki,1271789761390785), yay for me -> Column(name,Coda,1271789761374109))

    info("getting a set of columns from a set of keys: " + cass.multigetColumns(Set("yay for me", "yay for you"), Set("name", "motto")));
    // Map(yay for you -> Map(motto -> Column(motto,Told ya.,1271789761391366), name -> Column(name,Niki,1271789761390785)), yay for me -> Map(motto -> Column(motto,Moar lean.,1271789761389735), name -> Column(name,Coda,1271789761374109)))

    // drop some UUID sauce on things
    cass.keysAs(LexicalUUIDCodec.get()).insert(new LexicalUUID(cass.clock()), cass.newColumn("yay", "boo"));

    cass.namesAs(FixedLongCodec.get()).valuesAs(AsciiStringCodec.get()).getColumn("key", 2);
    cass.namesAs(VarIntCodec.get()).valuesAs(VarIntCodec.get())
        .insert("digits", cass.newColumn(new VarInt(1), new VarInt(300)));

    info("Iterating!");
    for (Tuple2<String, Column<String,String>> row : cass.rowIterator(2)) {
      info("Found: " + row._2());
    }

    info("removing a column");
    cass.removeColumn("yay for me", "motto");

    info("removing a row");
    cass.removeRow("yay for me");

    info("Batching up some stuff");
    cass.batch()
      .removeColumn("yay for you", "name")
      .removeColumns("yay for us", Set("name", "motto"))
      .insert("yay for nobody", cass.newColumn("name", "Burt"))
      .insert("yay for nobody", cass.newColumn("motto", "'S funny."))
      .execute();
  }
}
