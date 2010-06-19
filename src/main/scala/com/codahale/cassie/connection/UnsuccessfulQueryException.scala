package com.codahale.cassie.connection

import java.net.InetSocketAddress

/**
 * An exception thrown when a query cannot be executed.
 *
 * @author coda
 */
class UnsuccessfulQueryException(msg: String) extends Exception(msg)
