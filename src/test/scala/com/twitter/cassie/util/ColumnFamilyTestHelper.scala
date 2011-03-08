package com.twitter.cassie.util

import com.twitter.cassie.MockCassandraClient.SimpleProvider
import com.twitter.cassie.clocks.MicrosecondEpochClock
import com.twitter.cassie.codecs.Utf8Codec
import com.twitter.cassie.{MockCassandraClient, ReadConsistency, WriteConsistency, ColumnFamily}

trait ColumnFamilyTestHelper {
  val mockCassandraClient = new MockCassandraClient
  var columnFamily = new ColumnFamily("ks", "cf", new SimpleProvider(mockCassandraClient.client),
        MicrosecondEpochClock.get(), Utf8Codec.get(), Utf8Codec.get(), Utf8Codec.get(),
        ReadConsistency.Quorum, WriteConsistency.Quorum)
}