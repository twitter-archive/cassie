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

import com.twitter.finagle.service.{RetryingFilter, Backoff}
import com.twitter.finagle.{WriteException, TimedoutRequestException, ChannelException}
/**
 * Manages connections to the nodes in a Cassandra cluster.
 *
 * @param retryAttempts the number of times a query should be attempted before
 *                      throwing an exception
 * @param requestTimeoutInMS the amount of time, in milliseconds, the client will
 *                        wait for a response from the server before considering
 *                        the query to have failed
 * @param minConnectionsPerHost the minimum number of connections to maintain to
 *                              the node
 * @param maxConnectionsPerHost the maximum number of connections to maintain to
 *                              the ndoe
 * @param removeAfterIdleForMS the amount of time, in milliseconds, after which
 *                             idle connections should be closed and removed
 *                             from the pool
 */

private[cassie] class ClusterClientProvider(val hosts: CCluster,
                            val keyspace: String,
                            val retryAttempts: Int = 5,
                            val requestTimeoutInMS: Int = 10000,
                            val connectionTimeoutInMS: Int = 10000,
                            val minConnectionsPerHost: Int = 1,
                            val maxConnectionsPerHost: Int = 5,
                            val removeAfterIdleForMS: Int = 60000,
                            val statsReceiver: StatsReceiver = NullStatsReceiver,
                            val tracer: Tracer = NullTracer) extends ClientProvider {

  implicit val fakeTimer = new Timer {
    def schedule(when: Time)(f: => Unit): TimerTask = throw new Exception("illegal use!")
    def schedule(when: Time, period: Duration)(f: => Unit): TimerTask = throw new Exception("illegal use!")
    def stop() { throw new Exception("illegal use!") }
  }
  private val filter = RetryingFilter[ThriftClientRequest, Array[Byte]](Backoff.const(Duration(0, TimeUnit.MILLISECONDS)) take (retryAttempts), statsReceiver) {
    case Throw(ex: WriteException) => {
      statsReceiver.counter("WriteException").incr
      true
    }
    case Throw(ex: TimedoutRequestException) => {
      statsReceiver.counter("TimedoutRequestException").incr
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

  private var service = ClientBuilder()
      .cluster(hosts)
      .codec(CassandraThriftFramedCodec())
      .requestTimeout(Duration(requestTimeoutInMS, TimeUnit.MILLISECONDS))
      .connectionTimeout(Duration(connectionTimeoutInMS, TimeUnit.MILLISECONDS))
      .hostConnectionCoresize(minConnectionsPerHost)
      .hostConnectionLimit(maxConnectionsPerHost)
      .hostConnectionIdleTime(Duration(removeAfterIdleForMS, TimeUnit.MILLISECONDS))
      .reportTo(statsReceiver)
      .tracer(tracer)
      .build()

  service = filter andThen service

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
