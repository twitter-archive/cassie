package com.codahale.cassie.connection

import org.apache.cassandra.thrift.Cassandra.Client
import org.apache.thrift.transport.{TTransportException}
import org.apache.cassandra.thrift.TimedOutException
import com.codahale.logula.Logging

/**
 * A connection to a Cassandra node. Handles reconnection on temporary errors.
 *
 * @author coda
 */
class Connection(val factory: ClientFactory) extends Logging {
  private var _client: Option[Client] = None

  /**
   * Returns `true` if the connection is open.
   */
  def isOpen = synchronized { _client.isDefined }

  /**
   * Returns `true` if the connection is open and connected to a working node.
   */
  def isHealthy() = synchronized { _client match {
    case Some(client) =>
      try {
        client.describe_version
        true
      } catch {
        case e: Exception =>
          log.warning(e, "%s is unhealthy", this)
          false
      }
    case None => false
  } }

  /**
   * Given a function, executes it with the client connection, connecting if
   * need be. Returns either `Some(f(client))` or, if an error occured during
   * the process, `None`.
   *
   * (If a transport error occured, the connection will close itself.)
   */
  def map[A](f: Client => A): Option[A] = synchronized {
    try {
      if (open()) {
        _client.map(f)
      } else {
        log.warning("Unable to connect to %s", this)
        None
      }
    } catch {
      case e: TTransportException =>
        log.warning(e, "Error executing request on %s", this)
        close()
        None
      case e: TimedOutException =>
        log.warning(e, "Request timed out on %s", this)
        None
    }
  }

  /**
   * Ensures the connection, if not already open, is open.
   */
  def open() = synchronized {
    if (_client.isEmpty) {
      try {
        _client = Some(factory.build)
      } catch {
        case e: Exception => log.warning(e, "Unable to open connection to %s", factory)
      }
    }
    _client.isDefined
  }

  /**
   * Closes the connection.
   */
  def close() = synchronized {
    _client.map { factory.destroy(_) }
    _client = None
  }


  override def toString = "Connection(%s)".format(factory.host)
}
