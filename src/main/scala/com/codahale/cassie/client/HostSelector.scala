package com.codahale.cassie.client

import java.net.InetSocketAddress

/**
 * A generic class which control which node in a cluster a multi-node
 * ClientProvider will connect to next.
 *
 * @author coda
 */
trait HostSelector {
  def next: InetSocketAddress
}
