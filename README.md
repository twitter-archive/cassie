Cassie
======

Cassie is a small, lightweight Cassandra client built on [Finagle](http://github.com/twitter/finagle) with with all that provides plus column name/value encoding and decoding.

It is heavily used in production at Twitter so such be considered stable, yet it is incomplete in that it doesn't support the full feature set of Cassandra and will continue to evolve.

Requirements
------------

* Java SE 6
* Scala 2.8
* Cassandra 0.8 or later
* sbt 0.7

Note that Cassie *is* usable from Java. Its not super easy, but we're working
to make it easier.

Let's Get This Party Started
----------------------------

In your [simple-build-tool](https://github.com/harrah/xsbt/) project
file, add Cassie as a dependency:

    val twttr = "Twitter's Repository" at "http://maven.twttr.com/"
    val cassie = "com.twitter" % "cassie" % "0.19.0"
    
Or, for your _build.sbt:_

    resolvers += "Twitter's Repository" at "http://maven.twttr.com/"

    libraryDependencies += "com.twitter" % "cassie" % "0.19.0"
      excludeAll(
            ExclusionRule(organization = "com.sun.jdmk"),
            ExclusionRule(organization = "com.sun.jmx"),
            ExclusionRule(organization = "javax.jms")
        )

Finagle
-------

Before going further, you should probably learn about Finagle and its paradigm for asynchronous computing--[https://github.com/twitter/finagle](https://github.com/twitter/finagle.)

Connecting To Your Cassandra Cluster
------------------------------------

First create a cluster object, passing in a list of seed hosts. By default, when
creating a connection to a Keyspace, the given hosts will be queried for a full
list of nodes in the cluster. If you don't want to report stats use NullStatsReceiver.

    val cluster = new Cluster("host1,host2", OstrichStatsReceiver)

Then create a `Keyspace` instance which will use Finagle to maintain per-node
connection pools and do retries:

    val keyspace = cluster.keyspace("MyCassieApp").connect()
    // see KeyspaceBuilder for more options here. Try the defaults first.

(If you have some nodes with dramatically different latency—e.g., in another
data center–or if you have a huge cluster, you can disable keyspace mapping
via `mapHostsEvery(0.minutes)""" in which case clients will connect directly to
the seed hosts passed to "new Cluster".)
"
A Quick Note On Timestamps
--------------------------

Cassandra uses client-generated timestamps to determine the order in which
writes and deletes should be processed. Cassie previously came with several
different clock implementations. Now all Cassie users use the
MicrosecondEpochClock and timestamps should be mostly hidden from users.


A Longer Note, This Time On Column Names And Values
---------------------------------------------------

Cassandra stores the name and value of a column as an array of bytes. To
convert these bytes to and from useful Scala types, Cassie uses `Codec`
parameters for the given type.

For example, take adding a column to a column family of UTF-8 strings:

    val strings = keyspace.columnFamily[Utf8Codec, Utf8Codec, Utf8Codec]
    strings.insert("newstring", Column("colname", "colvalue"))

The `insert` method here requires a String and Column[String, String] because the type parameters of the columnFamily call were all `Codec[String]`.  The conversion between Strings and ByteArrays will be seamless. Cassie has codecs for a number of data types already:

* `Utf8Codec`: character sequence encoded with `UTF-8`
* `IntCodec`: 32-bit integer stored as a 4-byte sequence
* `LongCodec`: 64-bit integer stored as an 8-byte sequence
* `LexicalUUIDCodec` a UUID stored as a 16-byte sequence
* `ThriftCodec` a Thrift struct stored as variable-length sequence of bytes

Accessing Column Families
-------------------------

Once you've got a `Keyspace` instance, you can load your column families:

    val people  = keyspace.columnFamily[Utf8Codec, Utf8Codec, Utf8Codec]("People")
    val numbers = keyspace.columnFamily[Utf8Codec, Utf8Codec, IntCodec]("People",
                    defaultReadConsistency = ReadConsistency.One,
                    defaultWriteConsistency = WriteConsistency.Any)

By default, `ColumnFamily` instances have a default `ReadConsistency` and
`WriteConsistency` of `Quorum`, meaning reads and writes will only be considered
successful if a quorum of the replicas for that key respond successfully. You
can change this default or simply pass a different consistency level to specific
read and write operations.


Reading Data From Cassandra
---------------------------

Now that you've got your `ColumnFamily`, you can read some data from Cassandra:

    people.getColumn("codahale", "name")

`getColumn` returns an `Future[Option[Column[Name, Value]]]` where `Name` and `Value` are the type parameters of the `ColumnFamily`. If the row or column doesn't exist, `None` is returned. 

Explaining Futures is out of scope for this README, go the Finagle docs to learn more. But in essence you can do this:

    people.getColumn("codahale", "name") map {
      _ match {
        case col: Some(Column[String, String]) => # we have data
        case None => # there was no column
      }
    } handle {
      case e => {
        # there was an exception, do something about it
      }
    }

This whole block returns a Future which will be satisfied when the thrift rpc is done and the
callbacks have run.

Anyway, continuing--you can also get a set of columns:

    people.getColumns("codahale", Set("name", "motto"))

This returns a `Future[java.util.Map[Name, Column[Name, Value]]]`, where each column is mapped by its name.

If you want to get all columns of a row, that's cool too:

    people.getRow("codahale")

Cassie also supports multiget for columns and sets of columns:

    people.multigetColumn(Set("codahale", "darlingnikles"), "name")
    people.multigetColumns(Set("codahale", "darlingnikles"), Set("name", "motto"))

`multigetColumn` returns a `Future[Map[Key, Map[Name, Column[Name, Value]]]]` which maps row keys to column names to columns.


Asynchronous Iteration Through Rows and Columns
-----------------------------------------------

NOTE: This is new/experimental and likely to change in the future.

Cassie provides functionality for iterating through the rows of a column family and columns in a row. This works with both the random partitioner and the order-preserving partitioner, though iterating through rows in the random partitioner had undefined order.

You can iterate over every column of every row:

    val finished = cf.rowsIteratee(100).foreach { case(key, columns) =>
      println(key) //this function is executed async for each row
      println(cols)
    }
    finished() //this is a Future[Unit]. wait on it to know when the iteration is done

This gets 100 rows at a time and calls the above partial function on each one.


Writing Data To Cassandra
-------------------------

Inserting columns is pretty easy:

    people.insert("codahale", Column("name", "Coda"))
    people.insert("codahale", Column("motto", "Moar lean."))

You can insert a value with a specific timestamp:

    people.insert("darlingnikles", Column("name", "Niki").timestamp(200L))
    people.insert("darlingnikles", Column("motto", "Told ya.").timestamp(201L))

Batch operations are also possible:

    people.batch() { cf =>
      cf.insert("puddle", Column("name", "Puddle"))
      cf.insert("puddle", Column("motto", "Food!"))
    }.execute()

(See `BatchMutationBuilder` for a better idea of which operations are available.)


Deleting Data From Cassandra
----------------------------

First, it's important to understand
[exactly how deletes work](http://wiki.apache.org/cassandra/DistributedDeletes)
in a distributed system like Cassandra.

Once you've read that, then feel free to remove a column:

    people.removeColumn("puddle", "name")

Or a set of columns:

    people.removeColumns("puddle", Set("name", "motto"))

Or even a row:

    people.removeRow("puddle")

Generating Unique IDs
---------------------

If you're going to be storing data in Cassandra and don't have a naturally unique piece of data to use as a key, you've probably looked into UUIDs. The only problem with UUIDs is that they're mental, requiring access to MAC addresses or Gregorian calendars or POSIX ids. In general, people want UUIDs which are:

* Unique across a large set of workers without requiring coordination.
* Partially ordered by time.

Cassie's `LexicalUUID`s meet these criteria. They're 128 bits long. The most significant 64 bits are a timestamp value (from Cassie's strictly-increasing `Clock` implementation). The least significant 64 bits are a worker ID, with the default value being a hash of the machine's hostname.

When sorted using Cassandra's `LexicalUUIDType`, `LexicalUUID`s will be partially ordered by time--that is, UUIDs generated in order on a single process will be totally ordered by time; UUIDs generated simultaneously (i.e., within the same clock tick, given clock skew) will not have a deterministic order; UUIDs generated in order between single processes (i.e., in different clock ticks, given clock skew) will be totally ordered by time.

See *Lamport. Time, clocks, and the ordering of events in a distributed system. Communications of the ACM (1978) vol. 21 (7) pp. 565* and *Mattern. Virtual time and global states of distributed systems. Parallel and Distributed Algorithms (1989) pp. 215–226* for a more thorough discussion.

Things What Ain't Done Yet
==========================

* Authentication
* Meta data (e.g., `describe_*`)

Thanks
======

Many thanks to (pre twitter fork):

* Cliff Moon
* James Golick
* Robert J. Macomber

License
-------

* Copyright (c) 2010 Coda Hale
* Copyright (c) 2011-2012 Twitter, Inc.

Published under The Apache 2.0 License, see LICENSE.
