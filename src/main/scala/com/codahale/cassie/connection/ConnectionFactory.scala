package com.codahale.cassie.connection

import org.apache.commons.pool.BasePoolableObjectFactory
import com.codahale.logula.Logging

/**
 * A factory for [[com.codahale.connection.Connection]]s.
 *
 * @param factory the [[com.codahale.connection.ClientFactory]] responsible for
 *                creating new Cassandra `Client` instances
 * @author coda
 */
class ConnectionFactory(factory: ClientFactory)
        extends BasePoolableObjectFactory with Logging {

  def host = factory.host

  def makeObject = {
    log.fine("Creating a new connection to %s", factory)
    new Connection(factory)
  }

  override def validateObject(obj: Any) = obj match {
    case conn: Connection =>
      log.fine("Validating %s", conn)
      val healthy = conn.isHealthy()
      if (!healthy) {
        log.warning("%s is unhealthy", conn)
      }
      healthy
    case _ => false
  }

  override def destroyObject(obj: Any) = obj match {
    case conn: Connection =>
      log.fine("Destroying %s", conn)
      conn.close()
    case _ =>
  }


  override def toString = "ConnectionFactory(%s)".format(host)
}
