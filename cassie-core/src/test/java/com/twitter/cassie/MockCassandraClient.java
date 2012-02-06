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

package com.twitter.cassie;

import com.twitter.cassie.codecs.Utf8Codec;
import com.twitter.cassie.ColumnFamily;
import com.twitter.cassie.connection.ClientProvider;
import com.twitter.cassie.ReadConsistency;
import com.twitter.cassie.WriteConsistency;
import com.twitter.util.Future;
import com.twitter.util.Promise;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.apache.cassandra.finagle.thrift.*;
import org.junit.Before;
import org.junit.Test;
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
