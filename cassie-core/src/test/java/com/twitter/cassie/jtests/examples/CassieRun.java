package com.twitter.cassie.jtests.examples;

import java.util.Arrays;
import java.util.HashSet;

import scala.Tuple2;

import com.twitter.cassie.*;
import com.twitter.cassie.clocks.MicrosecondEpochClock;
import com.twitter.cassie.types.*;
import com.twitter.cassie.codecs.*;

public final class CassieRun {
  public static <V> HashSet<V> Set(V... values) {
    return new HashSet<V>(Arrays.asList(values));
  }

  public static void info(String str) {
    System.out.println(str);
  }

  public static void main(String[] args) throws Exception {
    // create a cluster with a single seed from which to map keyspaces
    Cluster cluster = new Cluster("localhost");

    // create a keyspace
    Keyspace keyspace = cluster.keyspace("Keyspace1")
      .retryAttempts(5)
      .requestTimeoutInMS(5000)
      .minConnectionsPerHost(1)
      .maxConnectionsPerHost(10)
      .removeAfterIdleForMS(60000)
      .connect();

    // create a column family
    ColumnFamily<String, String, String> cass = keyspace.columnFamily("Standard1", Utf8Codec.get(), Utf8Codec.get(), Utf8Codec.get());

    info("inserting some columns asynchronously");
    cass.insert("yay for me", cass.newColumn("name", "Coda")).apply();
    cass.insert("yay for me", cass.newColumn("motto", "Moar lean.")).apply();

    cass.insert("yay for you", cass.newColumn("name", "Niki")).apply();
    cass.insert("yay for you", cass.newColumn("motto", "Told ya.")).apply();

    cass.insert("yay for us", cass.newColumn("name", "Biscuit")).apply();
    cass.insert("yay for us", cass.newColumn("motto", "Mlalm.")).apply();

    cass.insert("yay for everyone", cass.newColumn("name", "Louie")).apply();
    cass.insert("yay for everyone", cass.newColumn("motto", "Swish!")).apply();

    info("getting a column: " + cass.getColumn("yay for me", "name").apply());
    // Some(Column(name,Coda,1271789761374109))

    info("getting a column that doesn't exist: " + cass.getColumn("yay for no one", "name").apply());
    // None

    info("getting a column that doesn't exist #2: " + cass.getColumn("yay for no one", "oink").apply());
    // None

    info("getting a set of columns: " + cass.getColumns("yay for me", Set("name", "motto")).apply());
    // Map(motto -> Column(motto,Moar lean.,1271789761389735), name -> Column(name,Coda,1271789761374109))

    info("getting a whole row: " + cass.getRow("yay for me").apply());
    // Map(motto -> Column(motto,Moar lean.,1271789761389735), name -> Column(name,Coda,1271789761374109))

    info("getting a column from a set of keys: " + cass.multigetColumn(Set("yay for me", "yay for you"), "name").apply());
    // Map(yay for you -> Column(name,Niki,1271789761390785), yay for me -> Column(name,Coda,1271789761374109))

    info("getting a set of columns from a set of keys: " + cass.multigetColumns(Set("yay for me", "yay for you"), Set("name", "motto")).apply());
    // Map(yay for you -> Map(motto -> Column(motto,Told ya.,1271789761391366), name -> Column(name,Niki,1271789761390785)), yay for me -> Map(motto -> Column(motto,Moar lean.,1271789761389735), name -> Column(name,Coda,1271789761374109)))

    // drop some UUID sauce on things
    cass.keysAs(LexicalUUIDCodec.get()).insert(new LexicalUUID(cass.clock()), cass.newColumn("yay", "boo")).apply();

    cass.namesAs(FixedLongCodec.get()).valuesAs(AsciiStringCodec.get()).getColumn("key", new FixedLong(2)).apply();
    cass.namesAs(VarIntCodec.get()).valuesAs(VarIntCodec.get())
        .insert("digits", cass.newColumn(new VarInt(1), new VarInt(300))).apply();

    info("Iterating!");
    for (Tuple2<String, Column<String,String>> row : cass.rowIteratee(2)) {
      info("Found: " + row._2());
    }

    info("removing a column");
    cass.removeColumn("yay for me", "motto").apply();

    info("removing a row");
    cass.removeRow("yay for me").apply();

    info("Batching up some stuff");
    cass.batch()
      .removeColumn("yay for you", "name")
      .removeColumns("yay for us", Set("name", "motto"))
      .insert("yay for nobody", cass.newColumn("name", "Burt"))
      .insert("yay for nobody", cass.newColumn("motto", "'S funny."))
      .execute().apply();

    info("Wrappin' up");
    keyspace.close();
  }
}
