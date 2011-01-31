package com.twitter.cassie.jtests;

import java.nio.ByteBuffer;

import com.twitter.cassie.Cluster;
import com.twitter.cassie.Keyspace;

import org.junit.Test;
import org.junit.Before;
import static junit.framework.Assert.assertEquals;

public class ClusterTest {
  public Cluster cluster;

  @Before
  public void before() throws Exception {
    cluster = new Cluster("host1,host2");
  }

  @Test
  public void test() {
    Keyspace ks = cluster.keyspace("blah").performMapping(false).connect();
    assertEquals(ks.name(), "blah");
  }
}
