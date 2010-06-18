package com.codahale.cassie.connection

import org.apache.commons.pool.BasePoolableObjectFactory
import com.codahale.logula.Logging

/**
 * A factory for `Connection` objects.
 *
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
    case conn: Connection => conn.isHealthy()
    case _ => false
  }

  override def destroyObject(obj: Any) = obj match {
    case conn: Connection => conn.close()
    case _ =>
  }
}
