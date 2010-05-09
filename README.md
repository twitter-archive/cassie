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


Let's Get This Party Started
----------------------------

In your [simple-build-tool](http://code.google.com/p/simple-build-tool/) project
file, add Cassie as a dependency:
    
    val codaRepo = "Coda Hale's Repository" at "http://repo.codahale.com/"
    val cassie = "com.codahale" %% "cassie" % "0.1" withSources()


Connecting To Your Cassandra Cluster
------------------------------------

First, use a `ClusterMap` to pull down a list of nodes in your cluster from a
seed host:

    val map = new ClusterMap("localhost", 9160)

Then set up a pool of 1-5 connections with up to 10 retries (in case a request
times out or a connection is lost, etc.):

    val selector = new RoundRobinHostSelector(map)
    val provider = new PooledClientProvider(selector, 1, 5, 10)

This uses a round-robin approach when a new connection is required. There's also
`RandomHostSelector`, which simply chooses a random node from the cluster to
connect to.


A Quick Note On Timestamps
--------------------------

Cassandra uses client-generated timestamps to determine the order in which
writes and deletes should be processed. Cassie comes with a few different clock
implementations, but you'll probably want to use `MicrosecondEpochClock`, which
is a strictly-increasing clock of microseconds since January 1st, 1970.

Set it up as an implicit variable:
    
    implicit val clock = MicrosecondEpochClock

(For each operation which requires a timestamp, you always have the option of
passing in a specific timestamp. In general, though, you should stick with this
default.)


A Longer Note, This Time On Column Names And Values
---------------------------------------------------

Cassandra stores the name and value of a column as a simple array of bytes. To
convert these bytes to and from useful Scala types, Cassie uses implicit `Codec`
parameters for the given type.

For example, take adding a column to a column family of UTF-8 strings:
    
    strings.insert("newstring", Column("colname", "colvalue"))

The `insert` method looks for an implicit parameter of type `Codec[String]` to
convert both the name and the value to byte arrays. In this case, the `codecs`
package already provides `Utf8Codec` as an implicit parameter, so the conversion
is seamless. Cassie handles `String` and `Array[Byte]` instances out of the box,
and also provides some useful non-standard types:

* `AsciiString`: character sequence encoded with `US-ASCII`
* `FixedInt`: 32-bit integer stored as a 4-byte sequence
* `FixedLong`: 64-bit integer stored as an 8-byte sequence
* `VarInt`: 32-bit integer stored using [Avro](http://hadoop.apache.org/avro/)'s
  variable-length integer encoding
* `VarLong`: 64-bit integer stored using
  [Avro](http://hadoop.apache.org/avro/)'s variable-length integer encoding

These types also have implicit conversions defined, so if you have an instance
of `ColumnFamily[String, VarLong]` you can use regular `Long`s.


Accessing Column Families
-------------------------

Once you've got a `ClientProvider` instance (in the form of the
`PooledClientProvider` from before), you can load your cluster, keyspace, and
column families:

    val cluster = new Cluster(provider)
    val keyspace = cluster.keyspace("MyCassieApp")
    
    val people = keyspace.columnFamily[String, String]("People")
    val numbers = keyspace.columnFamily[String, VarInt]("People",
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
    
`getColumn` returns an `Option[Column[Name, Value]]` where `Name` and `Value`
are the type parameters of the `ColumnFamily`. If the row or column doesn't
exist, `None` is returned.

If you need a column with a name or value of a different type than the
`ColumnFamily`, you can use `getColumnAs`:
    
    people.getColumnAs[String, VarLong]("codahale", "name")

This will return an `Option[Column[String, VarLong]]`.

You can also get a set of columns:

    people.getColumns("codahale", Set("name", "motto"))

This returns a `Map[Name, Column[Name, Value]]`, where each column is mapped by
its name. Again, `getColumnsAs` provides a way for reading column names and
values of different types:
    
    people.getColumnsAs[FixedLong, Array[Byte]]("things", Set(1, 2, 3))

If you want to get all columns of a row, that's cool too:
    
    people.getRow("codahale")

(And hey, `getRowAs` works, too.)

Cassie also supports multiget for columns and sets of columns:
    
    people.multigetColumn(Set("codahale", "darlingnikles"), "name")
    people.multigetColumns(Set("codahale", "darlingnikles"), Set("name", "motto"))

`multigetColumn` returns a `Map[String, Map[Name, Column[Name, Value]]]` which
maps row keys to column names to columns. `multigetColumnAs` and
`multigetColumnsAs` do the usual for specifying column name and value types.


Iterating Through Rows
----------------------

Cassie provides functionality for iterating through the rows of a column family.
This works with both the random partitioner and the order-preserving
partitioner, at the expense of a wee bit of performance (really, though,
high-performance iteration is not going to be Cassandra's selling point).

It does this by requesting a certain number of rows, starting with the first
possible row (`""`) and ending with the last row possible row (`""`). The last
key of the returned rows is then used as the start key for the next request,
until either no rows are returned or the last row is returned twice.

(The performance hit in this is that the last row of one request will be the
first row of the next.)

You can iterate over every column of every row:
    
    for ((key, col) <- people.rowIterator(100) {
      println(" Found column %s in row %s", col, key)
    }

(This gets 100 rows at a time.)

Or just one column from every row:

    for ((key, col) <- people.columnIterator(100, "name") {
      println(" Found column %s in row %s", col, key)
    }

Or a set of columns from every row:

    for ((key, col) <- people.columnsIterator(100, Set("name", "motto")) {
      println(" Found column %s in row %s", col, key)
    }


Writing Data To Cassandra
-------------------------

Inserting columns is pretty easy:

    people.insert("codahale", Column("name", "Coda"))
    people.insert("codahale", Column("motto", "Moar lean."))

You can insert a value with a specific timestamp:
    
    people.insert("darlingnikles", Column("name", "Niki", 200L))
    people.insert("darlingnikles", Column("motto", "Told ya.", 201L))

Or even insert column names and values of a different type than those of the
`ColumnFamily`:

    people.insert("biscuitfoof", Column[AsciiString, AsciiString]("name", "Biscuit"))
    people.insert("biscuitfoof", Column[AsciiString, AsciiString]("motto", "Mlalm."))

Or insert values with a specific write consistency level:

    people.insert("louiefoof", Column("name", "Louie"), WriteConsistency.All)
    people.insert("louiefoof", Column("motto", "Swish!"), WriteConsistency.Any)

Batch operations are also possible:
    
    people.batch() { cf =>
      cf.insert("puddle", Column("name", "Puddle"))
      cf.insert("puddle", Column("motto", "Food!"))
    }

(See `BatchMutationBuilder` for a better idea of which operations are 
available.)


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

If you need to ensure your delete action has a specific timestamp, you can:

    people.removeColumnWithTimestamp("puddle", "name", 40010L)
    people.removeColumnsWithTimestamp("puddle", Set("name", "motto"), 818181L)
    people.removeRowWithTimestamp("puddle", 901289282L)


Generating Unique IDs
---------------------

If you're going to be storing data in Cassandra and don't have a naturally
unique piece of data to use as a key, you've probably looked into UUIDs. The
only problem with UUIDs is that they're mental, requiring access to MAC
addresses or Gregorian calendars or POSIX ids. In general, people want UUIDs
which are:

* Unique across a large set of workers without requiring coordination.
* Partially ordered by time.

Cassie's `LexicalUUID`s meet these criteria. They're 128 bits long. The most
significant 64 bits are a timestamp value (from one of Cassie's
strictly-increasing `Clock` implementations -- `NanosecondEpochClock` is
recommended). The least significant 64 bits are a worker ID, with the default
value being a hash of the machine's hostname.

When sorted using Cassandra's `LexicalUUIDType`, `LexicalUUID`s will be
partially ordered by time -- that is, UUIDs generated in order on a single
process will be totally ordered by time; UUIDs generated simultaneously (i.e.,
within the same clock tick, given clock skew) will not have a deterministic
order; UUIDs generated in order between single processes (i.e., in different
clock ticks, given clock skew) will be totally ordered by time.

See *Lamport. Time, clocks, and the ordering of events in a distributed system.
Communications of the ACM (1978) vol. 21 (7) pp. 565* and *Mattern. Virtual time
and global states of distributed systems. Parallel and Distributed Algorithms
(1989) pp. 215–226* for a more thorough discussion.

`LexicalUUID`s can be used as column names, in which case they're stored as
16-byte values and are sortable by `LexicalUUIDType`, or as keys, in which case
they're stored as traditional, hex-encoded strings. Cassie provides implicit
conversions between `LexicalUUID` and `String`:

    val uuid = LexicalUUID() // uses the implicit `Clock` and the machine's hostname
    
    people.insert(uuid, Column("one", "two")) // converted to hex automatically
    
    people.insert("key", Column(uuid, "what")) // converted to a byte array


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