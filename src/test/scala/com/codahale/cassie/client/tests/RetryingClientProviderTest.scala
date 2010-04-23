package com.codahale.cassie.client.tests

import org.scalatest.matchers.MustMatchers
import org.scalatest.mock.MockitoSugar
import org.apache.commons.pool.ObjectPool
import org.mockito.Mockito.when
import org.apache.cassandra.thrift.Cassandra.Client
import org.apache.thrift.transport.TTransportException
import org.scalatest.{OneInstancePerTest, Spec}
import com.codahale.cassie.client.RetryingClientProvider
import java.util.logging.{Logger, Level}
import java.io.IOException

class RetryingClientProviderTest extends Spec with MustMatchers with MockitoSugar with OneInstancePerTest {
  Logger.getLogger("com.codahale.cassie.client.tests.RetryingClientProviderTest").setLevel(Level.OFF)

  describe("retrying a command") {
    val client1 = mock[Client]
    when(client1.describe_version).thenReturn("one")

    val client2 = mock[Client]
    when(client2.describe_version).thenReturn("two")

    val client3 = mock[Client]
    when(client3.describe_version).thenThrow(new TTransportException("OH GOD"))

    val client4 = mock[Client]
    when(client4.describe_version).thenReturn("four")

    val mockPool = mock[ObjectPool]
    when(mockPool.borrowObject).thenReturn(client1, client2, client3, client4, client3, client3, client3)

    val provider = new RetryingClientProvider {
      val pool = mockPool
      val maxRetry = 2
    }

    it("retries queries") {
      1.to(3).map { _ => provider.map { c => c.describe_version } } must equal(List("one", "two", "four"))
    }

    it("surfaces connection exceptions when a maximum of errors has been met") {
      evaluating {
        1.to(6).map { _ => provider.map { c => c.describe_version } }
      } must produce[IOException]      
    }

  }
}
