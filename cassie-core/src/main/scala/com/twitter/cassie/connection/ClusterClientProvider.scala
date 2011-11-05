package com.twitter.cassie.connection

import java.net.InetSocketAddress
import java.util.concurrent.TimeUnit
import org.apache.cassandra.finagle.thrift.Cassandra.ServiceToClient
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.cassandra.finagle.thrift.{UnavailableException, TimedOutException}
import com.twitter.finagle.Service
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.thrift.{ThriftClientRequest, ThriftClientFramedCodec}
import com.twitter.util.Duration
import com.twitter.util.{Future, Throw, Timer, TimerTask, Time}
import com.twitter.finagle.stats.{StatsReceiver, NullStatsReceiver}
import com.twitter.finagle.{CodecFactory, Codec, ClientCodecConfig}
import com.twitter.finagle.tracing.{Tracer, NullTracer}
import org.apache.thrift.protocol.{TBinaryProtocol, TProtocolFactory}

import com.twitter.finagle.service.{RetryingFilter, Backoff, TimeoutFilter}
import com.twitter.finagle.{WriteException, RequestTimeoutException, ChannelException}

sealed case class RetryPolicy()

object RetryPolicy {
  val Idempotent = RetryPolicy()
  val NonIdempotent = RetryPolicy()
}

private[cassie] class ClusterClientProvider(val hosts: CCluster,
                            val keyspace: String,
                            val retries: Int = 5,
                            val timeout: Duration = Duration(5, TimeUnit.SECONDS),
                            val requestTimeout: Duration = Duration(1, TimeUnit.SECONDS),
                            val connectTimeout: Duration = Duration(1, TimeUnit.SECONDS),
                            val minConnectionsPerHost: Int = 1,
                            val maxConnectionsPerHost: Int = 5,
                            val hostConnectionMaxWaiters: Int = 100,
                            val statsReceiver: StatsReceiver = NullStatsReceiver,
                            val tracerFactory: Tracer.Factory = NullTracer.factory,
                            val retryPolicy: RetryPolicy = RetryPolicy.Idempotent) extends ClientProvider {

  implicit val fakeTimer = new Timer {
    def schedule(when: Time)(f: => Unit): TimerTask = throw new Exception("illegal use!")
    def schedule(when: Time, period: Duration)(f: => Unit): TimerTask = throw new Exception("illegal use!")
    def stop() { throw new Exception("illegal use!") }
  }

  private val idempotentRetryFilter = RetryingFilter[ThriftClientRequest, Array[Byte]](Backoff.const(Duration(0, TimeUnit.MILLISECONDS)) take (retries), statsReceiver) {
    case Throw(ex: WriteException) => {
      statsReceiver.counter("WriteException").incr
      true
    }
    case Throw(ex: RequestTimeoutException) => {
      statsReceiver.counter("RequestTimeoutException").incr
      true
    }
    case Throw(ex: ChannelException) => {
      statsReceiver.counter("ChannelException").incr
      true
    }
    case Throw(ex: UnavailableException) => {
      statsReceiver.counter("UnavailableException").incr
      true
    }
    case Throw(ex: TimedOutException) => {
      statsReceiver.counter("TimedOutException").incr
      true
    }
  }

  private val nonIdempotentRetryFilter = RetryingFilter[ThriftClientRequest, Array[Byte]](Backoff.const(Duration(0, TimeUnit.MILLISECONDS)) take (retries), statsReceiver) {
    case Throw(ex: WriteException) => {
      statsReceiver.counter("WriteException").incr
      true
    }
    case Throw(ex: UnavailableException) => {
      statsReceiver.counter("UnavailableException").incr
      true
    }
  }

  val retryFilter = retryPolicy match {
    case RetryPolicy.Idempotent => idempotentRetryFilter
    case RetryPolicy.NonIdempotent => nonIdempotentRetryFilter
  }

  val timeoutFilter = new TimeoutFilter[ThriftClientRequest, Array[Byte]](timeout)

  private var service = ClientBuilder()
      .cluster(hosts)
      .name("cassie")
      .codec(CassandraThriftFramedCodec())
      // We don' use timeout here, because we add our out TimeoutFilter below.
      .requestTimeout(requestTimeout)
      .connectTimeout(connectTimeout)
      .hostConnectionCoresize(minConnectionsPerHost)
      .hostConnectionLimit(maxConnectionsPerHost)
      .reportTo(statsReceiver)
      .tracerFactory(tracerFactory)
      .hostConnectionMaxWaiters(hostConnectionMaxWaiters)
      .build()

  service = timeoutFilter andThen retryFilter andThen service

  private val client = new ServiceToClient(service, new TBinaryProtocol.Factory())

  def map[A](f: ServiceToClient => Future[A]) = f(client)

  override def close(): Unit = {
    hosts.close
    service.release()
    ()
  }

  /**
   * Convenience methods for passing in a codec factory.
   */
  object CassandraThriftFramedCodec {
    def apply() = new CassandraThriftFramedCodecFactory
    def get() = apply()
  }

  /**
   * Create a CassandraThriftFramedCodec with a BinaryProtocol
   */
  class CassandraThriftFramedCodecFactory
    extends CodecFactory[ThriftClientRequest, Array[Byte]]#Client
  {
    def apply(config: ClientCodecConfig) = {
      new CassandraThriftFramedCodec(new TBinaryProtocol.Factory(), config)
    }
  }

  class CassandraThriftFramedCodec(protocolFactory: TProtocolFactory, config: ClientCodecConfig) extends ThriftClientFramedCodec(protocolFactory: TProtocolFactory, config: ClientCodecConfig) {
    override def prepareService(cs: Service[ThriftClientRequest, Array[Byte]]) = {
      super.prepareService(cs) flatMap { service =>
        val client = new ServiceToClient(service, new TBinaryProtocol.Factory())
        client.set_keyspace(keyspace) map { _ => service }
      }
    }
  }
}
