package com.twitter.cassie.connection

import java.net.InetSocketAddress
import java.util.concurrent.TimeUnit
import org.apache.cassandra.finagle.thrift.Cassandra.ServiceToClient
import org.apache.thrift.protocol.TBinaryProtocol
import com.twitter.finagle.Service
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.thrift.{ThriftClientRequest, ThriftClientFramedCodec}
import com.twitter.util.Duration
import com.twitter.util.Future
import com.twitter.finagle.stats.{StatsReceiver, NullStatsReceiver}
import com.twitter.finagle.{Codec, ClientCodecConfig}
import org.apache.thrift.protocol.{TBinaryProtocol, TProtocolFactory}


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
                            val statsReceiver: StatsReceiver = NullStatsReceiver ) extends ClientProvider {

  private var service = ClientBuilder()
      .cluster(hosts)
      .codec(new CassandraThriftFramedCodec(new TBinaryProtocol.Factory(), ClientCodecConfig(Some("cassie"))))
      .retries(retryAttempts)
      .requestTimeout(Duration(requestTimeoutInMS, TimeUnit.MILLISECONDS))
      .connectionTimeout(Duration(connectionTimeoutInMS, TimeUnit.MILLISECONDS))
      .hostConnectionCoresize(minConnectionsPerHost)
      .hostConnectionLimit(maxConnectionsPerHost)
      .hostConnectionIdleTime(Duration(removeAfterIdleForMS, TimeUnit.MILLISECONDS))
      .reportTo(statsReceiver)
      .build()

  private val client = new ServiceToClient(service, new TBinaryProtocol.Factory())

  def map[A](f: ServiceToClient => Future[A]) = f(client)

  override def close(): Unit = {
    hosts.close
    service.release()
    ()
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
