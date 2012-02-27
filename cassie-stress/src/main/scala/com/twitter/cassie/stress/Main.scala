package com.twitter.cassie.stress

import com.twitter.cassie.codecs.{LongCodec, Utf8Codec}
import com.twitter.cassie._
import com.twitter.conversions.time._
import com.twitter.finagle.stats.OstrichStatsReceiver
import com.twitter.util.{Time, ScheduledThreadPoolTimer}
import com.twitter.finagle.stress.Stats

object Main {

  val row = "foo"
  val data = 0 until 100 toSeq

  def dispatchLoop(cf: CounterColumnFamily[String, Long]) {

    val batch = cf.batch
    data foreach { case(col) =>
      batch.insert(row, CounterColumn(col, 1))
    }

    batch.execute ensure {
      dispatchLoop(cf)
    }
  }

  private[this] def run(concurrency: Int, cf: CounterColumnFamily[String, Long]) {
    0 until concurrency foreach { _ => dispatchLoop(cf) }
  }

  def main(arg: Array[String]) {
    val cluster = new Cluster("localhost", new OstrichStatsReceiver())
    val keyspace = cluster.keyspace("test").connect()
    val cf = keyspace.counterColumnFamily("standard", Utf8Codec, LongCodec)

    val beginTime = Time.now
    val timer = new ScheduledThreadPoolTimer()
    timer.schedule(10.seconds) {
      println("@@ %ds".format(beginTime.untilNow.inSeconds))
      Stats.prettyPrintStats()
    }

    run(10, cf)
  }
}