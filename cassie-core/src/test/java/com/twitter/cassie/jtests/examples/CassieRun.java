package com.twitter.cassie.jtests.examples;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import com.twitter.cassie.*;
import com.twitter.cassie.clocks.MicrosecondEpochClock;
import com.twitter.cassie.types.*;
import com.twitter.cassie.codecs.*;
import com.twitter.util.Function2;
import com.twitter.util.Function;
import com.twitter.util.Future;

public final class CassieRun {
  public static <V> HashSet<V> Set(V... values) {
    return new HashSet<V>(Arrays.asList(values));
  }

  public static void info(Object o) {
    System.out.println(o);
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

    info("inserting some columns");
    //note that these calls are async, the apply() is where the waiting happens
    cass.insert("yay for me", cass.newColumn("name", "Coda")).apply();
    cass.insert("yay for me", cass.newColumn("motto", "Moar lean.")).apply();

    cass.insert("yay for you", cass.newColumn("name", "Niki")).apply();
    cass.insert("yay for you", cass.newColumn("motto", "Told ya.")).apply();

    cass.insert("yay for us", cass.newColumn("name", "Biscuit")).apply();
    cass.insert("yay for us", cass.newColumn("motto", "Mlalm.")).apply();

    cass.insert("yay for everyone", cass.newColumn("name", "Louie")).apply();
    cass.insert("yay for everyone", cass.newColumn("motto", "Swish!")).apply();

    info("getting a column: " + cass.getColumn("yay for me", "name").apply());
    info("getting a column that doesn't exist: " + cass.getColumn("yay for no one", "name").apply());
    info("getting a column that doesn't exist #2: " + cass.getColumn("yay for no one", "oink").apply());
    info("getting a set of columns: " + cass.getColumns("yay for me", Set("name", "motto")).apply());
    info("getting a whole row: " + cass.getRow("yay for me").apply());
    info("getting a column from a set of keys: " + cass.multigetColumn(Set("yay for me", "yay for you"), "name").apply());
    info("getting a set of columns from a set of keys: " + cass.multigetColumns(Set("yay for me", "yay for you"), Set("name", "motto")).apply());

    // drop some UUID sauce on things
    cass.keysAs(LexicalUUIDCodec.get()).insert(new LexicalUUID(cass.clock()), cass.newColumn("yay", "boo")).apply();
    cass.namesAs(LongCodec.get()).valuesAs(Utf8Codec.get()).getColumn("key", 2L).apply();
    cass.namesAs(IntCodec.get()).valuesAs(IntCodec.get())
        .insert("digits", cass.newColumn(1, 300)).apply();

    info("Iterating!");
    Future f = cass.rowsIteratee(2).foreach(new Function2<String, List<Column<String, String>>, Object>() {
      public Object apply(String key, List<Column<String,String>> columns) {
        info("Found: " + key);
        return null;
      }
    });

    f.apply();

    Future f2 = cass.columnsIteratee(2, "yay for me").foreach(new Function<Column<String, String>, Object>() {
      public Object apply(Column<String,String> column){
        info("Found Columns Iteratee: " + column);
        return null;
      }
    });

    f2.apply();

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
