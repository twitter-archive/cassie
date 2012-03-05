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
package com.twitter.cassie.stress

import com.twitter.cassie.codecs.{LongCodec, Utf8Codec}
import com.twitter.cassie._
import com.twitter.conversions.time._
import com.twitter.finagle.stats.OstrichStatsReceiver
import com.twitter.finagle.stress.Stats
import com.twitter.util.{Time, ScheduledThreadPoolTimer}

class BatchMutationBuilderStresser extends Stresser {

  private[this] val cluster = new Cluster("localhost", new OstrichStatsReceiver())
  private[this] val keyspace = cluster.keyspace("test").connect()
  private[this] val cf = keyspace.columnFamily("standard", Utf8Codec, LongCodec, LongCodec)

  private[this] val beginTime = Time.now
  private[this] val timer = new ScheduledThreadPoolTimer()

  timer.schedule(10.seconds) {
    println("@@ %ds".format(beginTime.untilNow.inSeconds))
    Stats.prettyPrintStats()
  }

  private[this] val row = "foo"
  private[this] val data = 0 until 100 toSeq

  def dispatchLoop() {
    val batch = cf.batch
    data foreach { col =>
      batch.insert(row, Column(col, col))
    }

    batch.execute ensure {
      dispatchLoop()
    }
  }

}