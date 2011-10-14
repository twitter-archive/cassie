Cassie
======

Cassie is a small, lightweight Cassandra client built on
[Finagle](http://github.com/twitter/finagle) with with all that provides plus
column name/value encoding and decoding.

Requirements
------------

* Java SE 6
* Scala 2.8
* Cassandra 0.8 or later

Note that Cassie *is* usable from Java. Its not super easy, but we're working
to make it easier.

Let's Get This Party Started
----------------------------

In your [simple-build-tool](http://code.google.com/p/simple-build-tool/) project
file, add Cassie as a dependency:

    val twttr = "Twitter's Repository" at "http://maven.twttr.com/"
    val cassie = "com.twitter" % "cassie" % "0.16.0"


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
via "mapHostsEvery(0.minutes)" in which case clients will connect directly to
the seed hosts passed to "new Cluster".)

A Quick Note On Timestamps
--------------------------

Cassandra uses client-generated timestamps to determine the order in which
writes and deletes should be processed. Cassie previously came with several
different clock implementations. Now all Cassie users use the
MicrosecondEpochClock and timestamps should be mostly hidden from users.


A Longer Note, This Time On Column Names And Values
---------------------------------------------------

Cassandra stores the name and value of a column as an array of bytes. To
convert these bytes to and from useful Scala types, Cassie uses implicit `Codec`
parameters for the given type.

For example, take adding a column to a column family of UTF-8 strings:

    strings.insert("newstring", Column("colname", "colvalue"))

The `insert` method looks for implicit parameters of type `Codec[String]` to
convert the key, name and value to byte arrays. In this case, the `codecs`
package already provides `Utf8Codec` as an implicit parameter, so the conversion
is seamless. Cassie handles `String` and `Array[Byte]` instances out of the box,
and also provides some useful non-standard types:

* `AsciiString`: character sequence encoded with `US-ASCII`
* `Int`: 32-bit integer stored as a 4-byte sequence
* `Long`: 64-bit integer stored as an 8-byte sequence

These types also have implicit conversions defined, so if you have an instance
of `ColumnFamily[String, String, VarLong]` you can use regular `Long`s.


Accessing Column Families
-------------------------

Once you've got a `Keyspace` instance, you can load your column families:

    val people  = keyspace.columnFamily[String, String, String]("People", MicrosecondEpochClock)
    val numbers = keyspace.columnFamily[String, String, VarInt]("People", MicrosecondEpochClock,
                    defaultReadConsistency = ReadConsistency.One,
                    defaultWriteConsistency = WriteConsistency.Any)

By default, `ColumnFamily` instances have a default `ReadConsistency` and
`WriteConsistency` of `Quorum`, meaning reads and writes will only be considered
successful if a quorum of the replicas for that key respond successfully. You
can change this default or simply pass a different consistency level to specific
read and write operations.


TODO: write or link to docs on Futures

Reading Data From Cassandra
---------------------------

Now that you've got your `ColumnFamily`, you can read some data from Cassandra:

    people.getColumn("codahale", "name")

`getColumn` returns an `Future[Option[Column[Name, Value]]]` where `Name` and `Value`
are the type parameters of the `ColumnFamily`. If the row or column doesn't
exist, `None` is returned.

You can also get a set of columns:

    people.getColumns("codahale", Set("name", "motto"))

This returns a `Future[Map[Name, Column[Name, Value]]]`, where each column is mapped by
its name.

If you want to get all columns of a row, that's cool too:

    people.getRow("codahale")

Cassie also supports multiget for columns and sets of columns:

    people.multigetColumn(Set("codahale", "darlingnikles"), "name")
    people.multigetColumns(Set("codahale", "darlingnikles"), Set("name", "motto"))

`multigetColumn` returns a `Future[Map[Key, Map[Name, Column[Name, Value]]]]` which
maps row keys to column names to columns.


Iterating Through Rows
----------------------

Cassie provides functionality for iterating through the rows of a column family.
This works with both the random partitioner and the order-preserving
partitioner.

It does this by requesting a certain number of rows, starting with the first
possible row (`""`) and ending with the last row possible row (`""`). The last
key of the returned rows is then used as the start key for the next request,
until either no rows are returned or the last row is returned twice.

(The performance hit in this is that the last row of one request will be the
first row of the next.)

You can iterate over every column of every row:

    for ((key, col) <- people.rowIteratee(100) {
      println(" Found column %s in row %s", col, key)
    }

(This gets 100 rows at a time.)

Or just one column from every row:

    for ((key, col) <- people.columnIteratee(100, "name") {
      println(" Found column %s in row %s", col, key)
    }

Or a set of columns from every row:

    for ((key, col) <- people.columnsIteratee(100, Set("name", "motto")) {
      println(" Found column %s in row %s", col, key)
    }

The 'ColumnIteratee' object returned by these methods implements Iterable for
use in loops like those shown, but it also allows for async iteration. An
Iteratee contains a batch of values, and has a hasNext() method indicating
whether more batches are available. If more batches are available, continue()
will request the next batch and return a Future[Iteratee].

Writing Data To Cassandra
-------------------------

Inserting columns is pretty easy:

    people.insert("codahale", Column("name", "Coda"))
    people.insert("codahale", Column("motto", "Moar lean."))

You can insert a value with a specific timestamp:

    people.insert("darlingnikles", Column("name", "Niki").timestamp(200L))
    people.insert("darlingnikles", Column("motto", "Told ya.").timestamp(201L))

Or even insert column names and values of a different type than those of the
`ColumnFamily`:

    people.insert("biscuitfoof", Column[AsciiString, AsciiString]("name", "Biscuit"))
    people.insert("biscuitfoof", Column[AsciiString, AsciiString]("motto", "Mlalm."))

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

    val uuid = LexicalUUID(people.clock)

    people.insert(uuid, Column("one", "two")) // converted to hex automatically

    people.insert("key", Column(uuid, "what")) // converted to a byte array


TODO counter column families

Things What Ain't Done Yet
==========================

* Anything relating to super columns
* Range queries
* Authentication
* Counting
* Meta data (e.g., `describe_*`)

Why? I don't need it yet.


Thanks
======

Many thanks to:

* Cliff Moon
* James Golick
* Robert J. Macomber


License
-------

Copyright (c) 2010 Coda Hale
Copyright (c) 2011 Twitter, Inc.

Published under The MIT License, see LICENSE
