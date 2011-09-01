package com.twitter.cassie.util

import com.twitter.util.Future
import com.twitter.finagle.stats.StatsReceiver

object FutureUtil {
  private val errPrefix = "errors_%s_%s"

  def timeFutureWithFailures[T](stats: StatsReceiver, name: String)(f: => Future[T]): Future[T] = {
    stats.timeFuture(name) {
      f
    }.onFailure { throwable =>
      stats.counter(errPrefix.format(name, throwable.getClass.getSimpleName)).incr()
    }
  }
}
