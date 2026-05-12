/*
 * Copyright (C) 2026 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.v2.templates.dbutils.connection;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.teleport.v2.spanner.migrations.exceptions.ConnectionException;
import com.google.cloud.teleport.v2.spanner.migrations.shard.SpannerShard;
import java.util.HashMap;
import java.util.Map;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mockito;

@RunWith(JUnit4.class)
public final class SpannerConnectionHelperTest {

  @After
  public void tearDown() {
    // Reset the static client map so tests don't leak between cases.
    new SpannerConnectionHelper().setClientMap(new HashMap<>());
  }

  @Test
  public void connectionKeyReturnsProjectInstanceDatabase() {
    SpannerShard shard = new SpannerShard("shard1", "myproj", "myinst", "mydb");
    assertThat(SpannerConnectionHelper.connectionKey(shard)).isEqualTo("myproj/myinst/mydb");
  }

  @Test
  public void connectionKeyHandlesDifferentValues() {
    SpannerShard a = new SpannerShard("s", "p1", "i1", "d1");
    SpannerShard b = new SpannerShard("s", "p2", "i2", "d2");
    assertThat(SpannerConnectionHelper.connectionKey(a))
        .isNotEqualTo(SpannerConnectionHelper.connectionKey(b));
  }

  @Test
  public void getConnectionReturnsClientForRegisteredKey() throws Exception {
    SpannerConnectionHelper helper = new SpannerConnectionHelper();
    DatabaseClient client = Mockito.mock(DatabaseClient.class);
    Map<String, DatabaseClient> map = new HashMap<>();
    map.put("p/i/d", client);
    helper.setClientMap(map);

    assertThat(helper.getConnection("p/i/d")).isSameAs(client);
  }

  @Test
  public void getConnectionThrowsWhenPoolEmpty() {
    SpannerConnectionHelper helper = new SpannerConnectionHelper();
    helper.setClientMap(new HashMap<>());

    assertThatThrownBy(() -> helper.getConnection("any-key"))
        .isInstanceOf(ConnectionException.class);
  }

  @Test
  public void getConnectionThrowsForUnknownKey() {
    SpannerConnectionHelper helper = new SpannerConnectionHelper();
    Map<String, DatabaseClient> map = new HashMap<>();
    map.put("a/b/c", Mockito.mock(DatabaseClient.class));
    helper.setClientMap(map);

    assertThatThrownBy(() -> helper.getConnection("does/not/exist"))
        .isInstanceOf(ConnectionException.class);
  }

  @Test
  public void isConnectionPoolInitializedReflectsClientMapState() {
    SpannerConnectionHelper helper = new SpannerConnectionHelper();
    helper.setClientMap(new HashMap<>());
    assertThat(helper.isConnectionPoolInitialized()).isFalse();

    Map<String, DatabaseClient> map = new HashMap<>();
    map.put("p/i/d", Mockito.mock(DatabaseClient.class));
    helper.setClientMap(map);
    assertThat(helper.isConnectionPoolInitialized()).isTrue();
  }
}
