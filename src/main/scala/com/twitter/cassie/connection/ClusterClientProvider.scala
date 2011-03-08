package com.twitter.cassie.connection

import java.net.InetSocketAddress
import java.util.concurrent.TimeUnit
import org.apache.cassandra.thrift.Cassandra.ServiceToClient
import org.apache.thrift.protocol.TBinaryProtocol
import com.twitter.finagle.Service
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.Protocol
import com.twitter.finagle.thrift.{ThriftClientRequest, ThriftClientFramedCodec}
import com.twitter.util.Duration
import com.twitter.util.Future
import com.twitter.finagle.stats.OstrichStatsReceiver


/**
 * Manages connections to the nodes in a Cassandra cluster.
 *
 * @param retryAttempts the number of times a query should be attempted before
 *                      throwing an exception
 * @param readTimeoutInMS the amount of time, in milliseconds, the client will
 *                        wait for a response from the server before considering
 *                        the query to have failed
 * @param minConnectionsPerHost the minimum number of connections to maintain to
 *                              the node
 * @param maxConnectionsPerHost the maximum number of connections to maintain to
 *                              the ndoe
 * @param removeAfterIdleForMS the amount of time, in milliseconds, after which
 *                             idle connections should be closed and removed
 *                             from the pool
 * @param framed true if the server will only accept framed connections
 * @author coda
 */
private[cassie] class ClusterClientProvider(val hosts: Set[InetSocketAddress],
                            val keyspace: String,
                            val retryAttempts: Int = 5,
                            val readTimeoutInMS: Int = 10000,
                            val connectionTimeoutInMS: Int = 10000,
                            val minConnectionsPerHost: Int = 1,
                            val maxConnectionsPerHost: Int = 5,
                            val removeAfterIdleForMS: Int = 60000) extends ClientProvider {

  private val service = ClientBuilder()
      .hosts(hosts.toSeq)
      .protocol(CassandraProtocol(keyspace))
      .retries(retryAttempts)
      .requestTimeout(Duration(readTimeoutInMS, TimeUnit.MILLISECONDS))
      .connectionTimeout(Duration(connectionTimeoutInMS, TimeUnit.MILLISECONDS))
      .hostConnectionCoresize(minConnectionsPerHost)
      .hostConnectionLimit(maxConnectionsPerHost)
      .reportTo(new OstrichStatsReceiver)
      .hostConnectionIdleTime(Duration(removeAfterIdleForMS, TimeUnit.MILLISECONDS))
      .build()

  private val client = new ServiceToClient(service, new TBinaryProtocol.Factory())

  def map[A](f: ServiceToClient => Future[A]) = f(client)

  override def close() = service.release()

  case class CassandraProtocol(keyspace: String) extends Protocol[ThriftClientRequest, Array[Byte]]
  {
    def codec = ThriftClientFramedCodec()
    override def prepareChannel(cs: Service[ThriftClientRequest, Array[Byte]]) = {
      // perform connection setup
      val client = new ServiceToClient(cs, new TBinaryProtocol.Factory())
      client.set_keyspace(keyspace).map{ _ => cs }
    }
  }
}
