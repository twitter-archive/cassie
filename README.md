Cassie
======

*Because life's too short to deal with Thrift.*

Cassie is a small, lightweight Cassandra client with connection pooling, cluster
discovery, and column name/value encoding and decoding. It tries to do what the
Thrift API *actually* does, not what it *says* it does.


Requirements
------------

* Java SE 6
* Scala 2.8 Beta1
* Cassandra 0.6.x


How To Use
----------

**First**, specify Cassie as a dependency:

    val codaRepo = "Coda Hale's Repository" at "http://repo.codahale.com/"
    val cassie = "com.yammer" %% "cassie" % "0.1" withSources()

**Second**, set up a connection:
    
    import com.codahale.cassie._
    import com.codahale.cassie.client._
    import com.codahale.cassie.clocks._
    import com.codahale.cassie.codecs._
    
    // pull down data about all the nodes in the cluster
    val map = new ClusterMap("localhost", 9160)
    
    // create a round-robin pool of 1-5 connections, and retry each query up to
    // 10 times
    val selector = new RoundRobinHostSelector(map)
    val provider = new PooledClientProvider(selector, 1, 5, 10)

**Third**, let Cassie know how it should be handling column names and values:
    
    val cluster = new Cluster(provider)
    val keyspace = cluster.keyspace("MyCassieApp")

    // access the "People" column family with column names and values as UTF-8
    // strings
    val people = keyspace.columnFamily[String, String]("People")
    
**Fourth**, interact with Cassandra:

Pick a clock to use for timestamps. Microseconds are fashionable:
    
    implicit val clock = MicrosecondEpochClock

Insert some columns:
    
    people.insert("codahale", Column("name", "Coda"), WriteConsistency.Quorum)
    people.insert("codahale", Column("motto", "Moar lean."), WriteConsistency.Quorum)

    people.insert("darlingnikles", Column("name", "Niki"), WriteConsistency.Quorum)
    people.insert("darlingnikles", Column("motto", "Told ya."), WriteConsistency.Quorum)

    people.insert("biscuitfoof", Column("name", "Biscuit"), WriteConsistency.Quorum)
    people.insert("biscuitfoof", Column("motto", "Mlalm."), WriteConsistency.Quorum)

    people.insert("louiefoof", Column("name", "Louie"), WriteConsistency.Quorum)
    people.insert("louiefoof", Column("motto", "Swish!"), WriteConsistency.Quorum)

Insert a column of a different type:
    
    people.insert("digits", Column[VarInt, VarInt](1, 300), WriteConsistency.Quorum)
    
Select a single column:
    
    people.get("codahale", "name", ReadConsistency.Quorum)
    // Some(Column(name,Coda,1271789761374109))

Select a single column of a different type:
    
    people.getAs[VarInt, VarInt]("digits", 1, ReadConsistency.One)

Select a column what don't exist:
    
    people.get("unicorncow", "name", ReadConsistency.Quorum)
    // None

Select a set of columns:
    
    people.get("codahale", Set("name", "motto"), ReadConsistency.One)    
    // Map(motto -> Column(motto,Moar lean.,1271789761389735), name -> Column(name,Coda,1271789761374109))

Select an entire row:
    
    people.get("codahale", ReadConsistency.Quorum)
    // Map(motto -> Column(motto,Moar lean.,1271789761389735), name -> Column(name,Coda,1271789761374109))

Select a column from a set of rows:
    
    people.multiget(Set("codahale", "darlingnikles"), "name", ReadConsistency.Quorum)
    // Map(darlingnikles -> Column(name,Niki,1271789761390785), codahale -> Column(name,Coda,1271789761374109))

Select a set of columns from a set of rows:
    
    people.multiget(Set("codahale", "yay for you"), Set("name", "motto"), ReadConsistency.Quorum)
    // Map(darlingnikles -> Map(motto -> Column(motto,Told ya.,1271789761391366), name -> Column(name,Niki,1271789761390785)),
    //     codahale -> Map(motto -> Column(motto,Moar lean.,1271789761389735), name -> Column(name,Coda,1271789761374109)))

Remove a column:

    people.remove("codahale", "motto", WriteConsistency.Quorum)

Or a set of columns:
    
    people.remove("codahale", Set("name", "motto"), WriteConsistency.Quorum)

Remove a row:
    
    people.remove("codahale", WriteConsistency.Quorum)

Or batch up a whole bunch of mutations and send 'em down the pipe at once:
    
    people.batch(WriteConsistency.Quorum) { batch =>
      batch.insert("ursusbourbonia", Column("name", "Drinky Bear"))
      batch.insert("ursusbourbonia", Column("motto", "Arghalhafflg."))
      batch.remove("tinkles", Set("name", "motto", "carpetstain"))
    }

And then iterate over the whole mess:
    
    for ((key, col) <- people.iterator(100, Set("name", "motto")) {
      println(" Found column %s in row %s", col, key)
    }


Things What Ain't Done Yet
==========================

* Anything relating to super columns
* Range queries
* Authentication
* Counting
* Meta data (e.g., `describe_*`)

Why? I don't need it yet.


License
-------

Copyright (c) 2010 Coda Hale
Published under The MIT License, see LICENSE