package com.twitter.cassie;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.twitter.util.Future;
import com.twitter.util.Promise;

import com.twitter.cassie.ColumnFamily;
import com.twitter.cassie.connection.ClientProvider;
import com.twitter.cassie.codecs.Utf8Codec;
import com.twitter.cassie.ReadConsistency;
import com.twitter.cassie.WriteConsistency;

import org.apache.cassandra.finagle.thrift.*;
import org.junit.Test;
import org.junit.Before;
import static junit.framework.Assert.assertEquals;
import static org.mockito.Mockito.*;

import org.apache.cassandra.finagle.thrift.Cassandra.ServiceToClient;

public final class MockCassandraClient {
  public final ServiceToClient client;

  public MockCassandraClient() {
    this.client = mock(ServiceToClient.class);
  }

  public static final class SimpleProvider implements ClientProvider {
    public final ServiceToClient client;
    public boolean closed = false;
    public SimpleProvider(ServiceToClient client) {
      this.client = client;
    }
    @Override
    public <A>Future<A> map(scala.Function1<ServiceToClient, Future<A>> func) {
      assert !closed;
      return func.apply(client);
    }
    @Override
    public void close() { closed = true; }
  }
}
