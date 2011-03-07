package com.twitter.cassie;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.twitter.util.Future;
import com.twitter.util.Promise;

import com.twitter.cassie.ColumnFamily;
import com.twitter.cassie.clocks.MicrosecondEpochClock;
import com.twitter.cassie.connection.ClientProvider;
import com.twitter.cassie.codecs.Utf8Codec;
import com.twitter.cassie.ReadConsistency;
import com.twitter.cassie.WriteConsistency;

import org.junit.Test;
import org.junit.Before;
import static junit.framework.Assert.assertEquals;
import static org.mockito.Mockito.*;

import org.apache.cassandra.thrift.Cassandra.ServiceToClient;
import org.apache.cassandra.thrift.*;

public final class MockCassandraClient {
  public static ByteBuffer anyByteBuffer() {
    return any(ByteBuffer.class);
  }
  public static ColumnParent anyColumnParent() {
    return any(ColumnParent.class);
  }
  public static ColumnPath anyColumnPath() {
    return any(ColumnPath.class);
  }
  public static SlicePredicate anySlicePredicate() {
    return any(SlicePredicate.class);
  }
  public static org.apache.cassandra.thrift.Column anyColumn() {
    return any(org.apache.cassandra.thrift.Column.class);
  }
  public static ConsistencyLevel anyConsistencyLevel() {
    return any(ConsistencyLevel.class);
  }

  public final ServiceToClient client;

  public MockCassandraClient() {
    this.client = mock(ServiceToClient.class);
    // stub out some standard cases
    when(client.batch_mutate(anyMap(), anyConsistencyLevel()))
        .thenReturn(new Fulfillment(null));
    when(client.remove(anyByteBuffer(), anyColumnPath(), anyInt(), anyConsistencyLevel()))
        .thenReturn(new Fulfillment(null));
    when(client.insert(anyByteBuffer(), anyColumnParent(), anyColumn(), anyConsistencyLevel()))
        .thenReturn(new Fulfillment(null));
    when(client.get_slice(anyByteBuffer(), anyColumnParent(), anySlicePredicate(),
        anyConsistencyLevel()))
        .thenReturn(new Fulfillment(new ArrayList<ColumnOrSuperColumn>()));
    when(client.multiget_slice(anyListOf(ByteBuffer.class), anyColumnParent(),
        anySlicePredicate(), anyConsistencyLevel()))
        .thenReturn(new Fulfillment(new HashMap<ByteBuffer,List<ColumnOrSuperColumn>>()));
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

  public static class Fulfillment<A> extends Promise<A> {
    public Fulfillment(A result) {
      super();
      this.setValue(result);
    }
  }
}
