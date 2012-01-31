package com.twitter.cassie

import com.twitter.cassie.codecs.ThriftCodec
import com.twitter.cassie.connection.ClientProvider
import com.twitter.cassie.util.FutureUtil
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.tracing.Trace
import com.twitter.util.Future
import org.apache.cassandra.finagle.thrift
import org.apache.cassandra.finagle.thrift.Cassandra.ServiceToClient


object BaseColumnFamily {
  val annPredCodec = new ThriftCodec[thrift.SlicePredicate](classOf[thrift.SlicePredicate])
}

private[cassie] abstract class BaseColumnFamily(keyspace: String, cf: String, provider: ClientProvider, stats: StatsReceiver) {

  import BaseColumnFamily._
  import FutureUtil._

  val baseAnnotations = Map("keyspace" -> keyspace, "columnfamily" -> cf)

  protected def trace(annotations: Map[String, Any]) {
    Trace.recordBinaries(baseAnnotations)
    Trace.recordBinaries(annotations)
  }

  def withConnection[T](
    name: String,
    traceAnnotations: Map[String, Any] = Map.empty)(f: ServiceToClient => Future[T]): Future[T] = {
    timeFutureWithFailures(stats, name) {
      trace(traceAnnotations)
      provider.map(f)
    }
  }
}