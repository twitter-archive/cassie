// Copyright 2012 Twitter, Inc.

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

// http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.twitter.cassie.jtests;

import com.twitter.cassie.Cluster;
import com.twitter.cassie.Keyspace;
import com.twitter.finagle.stats.NullStatsReceiver$;
import com.twitter.util.Duration;
import java.nio.ByteBuffer;
import org.junit.Before;
import org.junit.Test;
import static junit.framework.Assert.assertEquals;

public class ClusterTest {
  public Cluster cluster;

  @Before
  public void before() throws Exception {
    cluster = new Cluster("host1,host2", NullStatsReceiver$.MODULE$).mapHostsEvery(new Duration(0));
  }

  @Test
  public void test() {
    Keyspace ks = cluster.keyspace("blah").connect();
    assertEquals(ks.name(), "blah");
  }
}
