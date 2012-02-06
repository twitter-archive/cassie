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

import com.twitter.cassie.codecs.Codec;
import com.twitter.cassie.codecs.Utf8Codec;
import com.twitter.cassie.ColumnFamily;
import com.twitter.cassie.MockCassandraClient;
import com.twitter.cassie.ReadConsistency;
import com.twitter.cassie.WriteConsistency;
import com.twitter.finagle.stats.NullStatsReceiver$;
import java.nio.ByteBuffer;
import org.apache.cassandra.finagle.thrift.*;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import static junit.framework.Assert.assertEquals;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

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
