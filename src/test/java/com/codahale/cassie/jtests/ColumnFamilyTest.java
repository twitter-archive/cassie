package com.codahale.cassie.jtests;

import java.nio.ByteBuffer;

import com.codahale.cassie.ColumnFamily;
import com.codahale.cassie.MockCassandraClient;
import com.codahale.cassie.codecs.Codec;
import com.codahale.cassie.codecs.Utf8Codec;

import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ConsistencyLevel;

import org.junit.Test;
import org.junit.Before;
import static junit.framework.Assert.assertEquals;
import org.mockito.ArgumentCaptor;
import static org.mockito.Mockito.*;
import static org.mockito.Matchers.*;

public class ColumnFamilyTest {
  protected MockCassandraClient mock;

  public final Codec<String> codec = Utf8Codec.get();

  @Before
  public void before() throws Exception {
    mock = new MockCassandraClient();
  }

  @Test
  public void test() {
    mock.cf.getColumn("key", "name");
    ColumnParent cp = new ColumnParent("cf");
    ArgumentCaptor<SlicePredicate> pred = ArgumentCaptor.forClass(SlicePredicate.class);
    verify(mock.client).get_slice(eq(codec.encode("key")), eq(cp),
        pred.capture(), eq(ConsistencyLevel.QUORUM));
    for (ByteBuffer name : pred.getValue().getColumn_names()) {
      assertEquals(codec.decode(name), "name");
    }
  }
}
