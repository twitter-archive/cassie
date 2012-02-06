// Copyright 2012 Twitter, Inc.

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

// http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.twitter.cassie.util

import com.twitter.cassie.clocks.MicrosecondEpochClock
import com.twitter.cassie.codecs.Utf8Codec
import com.twitter.cassie.MockCassandraClient.SimpleProvider
import com.twitter.cassie._
import com.twitter.finagle.stats.NullStatsReceiver
import java.nio.ByteBuffer
import org.apache.cassandra.finagle.thrift
import org.mockito.Matchers.any

trait ColumnFamilyTestHelper {
  type ColumnList = java.util.List[thrift.ColumnOrSuperColumn]
  type KeyColumnMap = java.util.Map[java.nio.ByteBuffer, ColumnList]

  def cosc(cf: ColumnFamily[String, String, String], c: Column[String, String]) = {
    new thrift.ColumnOrSuperColumn().setColumn(Column.convert(cf.nameCodec, cf.valueCodec, cf.clock, c))
  }

  def c(cf: ColumnFamily[String, String, String], name: String, value: String, timestamp: Long) = {
    val cosc = new thrift.ColumnOrSuperColumn
    cosc.setColumn(
      Column.convert(
        Utf8Codec,
        Utf8Codec,
        cf.clock,
        cf.newColumn(name, value, timestamp)
      )
    )
    cosc
  }

  def cc(name: String, value: Long) = {
    val cosc = new thrift.ColumnOrSuperColumn()
    cosc.setCounter_column(new thrift.CounterColumn(Utf8Codec.encode(name), value))
    cosc
  }

  def b(keyString: String) = ByteBuffer.wrap(keyString.getBytes)

  def setup = {
    val mcc = new MockCassandraClient
    val cf = new ColumnFamily("ks", "cf", new SimpleProvider(mcc.client),
      Utf8Codec, Utf8Codec, Utf8Codec, NullStatsReceiver)
    (mcc.client, cf)
  }

  def setupCounters = {
    val mcc = new MockCassandraClient
    val cf = new CounterColumnFamily("ks", "cf", new SimpleProvider(mcc.client),
      Utf8Codec, Utf8Codec, NullStatsReceiver)
    (mcc.client, cf)
  }

  def anyByteBuffer() = any(classOf[ByteBuffer])
  def anyColumnParent() = any(classOf[thrift.ColumnParent])
  def anyColumnPath() = any(classOf[thrift.ColumnPath])
  def anySlicePredicate() = any(classOf[thrift.SlicePredicate])
  def anyColumn() = any(classOf[thrift.Column])
  def anyConsistencyLevel() = any(classOf[thrift.ConsistencyLevel])
  def anyCounterColumn() = any(classOf[thrift.CounterColumn])
  def anyKeyRange() = any(classOf[thrift.KeyRange])
  def anyInt() = any(classOf[Int])

  def pred(start: String, end: String, count: Int, order: Order = Order.Normal) =
    new thrift.SlicePredicate().setSlice_range(
      new thrift.SliceRange().setStart(b(start)).setFinish(b(end))
        .setReversed(order.reversed).setCount(count))

}