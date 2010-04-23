package com.codahale.cassie

/**
 * A readable, writable Cassandra column family.
 *
 * @author coda
 */
trait ColumnFamily[Name, Value]
        extends ReadableColumnFamily[Name, Value]
        with    WritableColumnFamily[Name, Value]
