package com.twitter.cassie.jtests;

import java.nio.ByteBuffer;

import com.twitter.cassie.ColumnFamily;
import com.twitter.cassie.MockCassandraClient;
import com.twitter.cassie.ReadConsistency;
import com.twitter.cassie.WriteConsistency;
import com.twitter.cassie.codecs.Codec;
import com.twitter.cassie.codecs.Utf8Codec;

import org.apache.cassandra.finagle.thrift.*;

import org.junit.Test;
import org.junit.Before;
import static junit.framework.Assert.assertEquals;
import org.mockito.ArgumentCaptor;
import static org.mockito.Mockito.*;
import static org.mockito.Matchers.*;
import com.twitter.finagle.stats.NullStatsReceiver$;

public class ColumnFamilyTest {
  protected MockCassandraClient mock;

  public final Codec<String> codec = Utf8Codec.get();

  @Before
  public void before() throws Exception {
    mock = new MockCassandraClient();
  }

  @Test
  public void test() {
    ColumnFamily cf = new ColumnFamily("ks", "cf", new MockCassandraClient.SimpleProvider(mock.client),
        Utf8Codec.get(), Utf8Codec.get(), Utf8Codec.get(), NullStatsReceiver$.MODULE$,
        ReadConsistency.Quorum(), WriteConsistency.Quorum());
    cf.getColumn("key", "name");
    ColumnParent cp = new ColumnParent("cf");
    ArgumentCaptor<SlicePredicate> pred = ArgumentCaptor.forClass(SlicePredicate.class);
    verify(mock.client).get_slice(eq(codec.encode("key")), eq(cp),
        pred.capture(), eq(ConsistencyLevel.QUORUM));
    for (ByteBuffer name : pred.getValue().getColumn_names()) {
      assertEquals(codec.decode(name), "name");
    }
  }
}
