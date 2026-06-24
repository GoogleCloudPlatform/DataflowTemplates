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
package com.google.cloud.teleport.v2.templates.source.spanner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.teleport.v2.spanner.migrations.connection.ConnectionHelperRequest;
import com.google.cloud.teleport.v2.spanner.migrations.exceptions.ConnectionException;
import com.google.cloud.teleport.v2.spanner.migrations.shard.SpannerShard;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.io.gcp.spanner.SpannerAccessor;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

@RunWith(JUnit4.class)
public final class SpannerConnectionHelperTest {

  @After
  public void tearDown() {
    // Reset the static maps so tests don't leak between cases.
    new SpannerConnectionHelper().close();
  }

  @Test
  public void testInit() {
    SpannerShard shard = new SpannerShard("p", "i", "d");
    ConnectionHelperRequest request =
        new ConnectionHelperRequest(List.of(shard), null, 10, null, null, null);

    SpannerAccessor mockAccessor = mock(SpannerAccessor.class);
    DatabaseClient mockClient = mock(DatabaseClient.class);
    when(mockAccessor.getDatabaseClient()).thenReturn(mockClient);

    try (MockedStatic<SpannerAccessor> spannerAccessorMockedStatic =
        mockStatic(SpannerAccessor.class)) {
      spannerAccessorMockedStatic
          .when(() -> SpannerAccessor.getOrCreate(any()))
          .thenReturn(mockAccessor);

      SpannerConnectionHelper helper = new SpannerConnectionHelper();
      helper.init(request);

      assertThat(helper.isConnectionPoolInitialized()).isTrue();
      assertThat(helper.getConnection(SpannerConnectionHelper.connectionKey(shard)))
          .isSameAs(mockClient);
    } catch (ConnectionException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void connectionKeyReturnsProjectInstanceDatabase() {
    SpannerShard shard = new SpannerShard("myproj", "myinst", "mydb");
    assertThat(SpannerConnectionHelper.connectionKey(shard)).isEqualTo("myproj/myinst/mydb");
  }

  @Test
  public void connectionKeyHandlesDifferentValues() {
    SpannerShard a = new SpannerShard("p1", "i1", "d1");
    SpannerShard b = new SpannerShard("p2", "i2", "d2");
    assertThat(SpannerConnectionHelper.connectionKey(a))
        .isNotEqualTo(SpannerConnectionHelper.connectionKey(b));
  }

  @Test
  public void getConnectionReturnsClientForRegisteredKey() throws Exception {
    SpannerConnectionHelper helper = new SpannerConnectionHelper();
    SpannerAccessor mockAccessor = Mockito.mock(SpannerAccessor.class);
    DatabaseClient mockClient = Mockito.mock(DatabaseClient.class);
    when(mockAccessor.getDatabaseClient()).thenReturn(mockClient);

    Map<String, SpannerAccessor> map = new HashMap<>();
    map.put("p/i/d", mockAccessor);
    helper.setAccessorMap(map);

    assertThat(helper.getConnection("p/i/d")).isSameAs(mockClient);
  }

  @Test
  public void getConnectionThrowsWhenPoolEmpty() {
    SpannerConnectionHelper helper = new SpannerConnectionHelper();
    helper.setAccessorMap(new HashMap<>());

    assertThatThrownBy(() -> helper.getConnection("any-key"))
        .isInstanceOf(ConnectionException.class);
  }

  @Test
  public void getConnectionThrowsForUnknownKey() {
    SpannerConnectionHelper helper = new SpannerConnectionHelper();
    Map<String, SpannerAccessor> map = new HashMap<>();
    map.put("a/b/c", Mockito.mock(SpannerAccessor.class));
    helper.setAccessorMap(map);

    assertThatThrownBy(() -> helper.getConnection("does/not/exist"))
        .isInstanceOf(ConnectionException.class);
  }

  @Test
  public void isConnectionPoolInitializedReflectsClientMapState() {
    SpannerConnectionHelper helper = new SpannerConnectionHelper();
    helper.setAccessorMap(new HashMap<>());
    assertThat(helper.isConnectionPoolInitialized()).isFalse();

    Map<String, SpannerAccessor> map = new HashMap<>();
    map.put("p/i/d", Mockito.mock(SpannerAccessor.class));
    helper.setAccessorMap(map);
    assertThat(helper.isConnectionPoolInitialized()).isTrue();
  }

  @Test
  public void initDoesNotReinitializeIfNotEmpty() throws Exception {
    SpannerConnectionHelper helper = new SpannerConnectionHelper();
    Map<String, SpannerAccessor> map = new HashMap<>();
    map.put("p/i/d", Mockito.mock(SpannerAccessor.class));
    helper.setAccessorMap(map);

    // This should just return without doing anything (and log it)
    helper.init(null);
    assertThat(helper.isConnectionPoolInitialized()).isTrue();
  }

  @Test
  public void testGetConnection_nonExistentKey() throws Exception {
    SpannerConnectionHelper helper = new SpannerConnectionHelper();
    Map<String, SpannerAccessor> map = new HashMap<>();
    map.put("p/i/d", Mockito.mock(SpannerAccessor.class));
    helper.setAccessorMap(map);

    assertThatThrownBy(() -> helper.getConnection("non-existent"))
        .isInstanceOf(ConnectionException.class)
        .hasMessageContaining("No Spanner connection found for key: non-existent");
  }

  @Test
  public void testClose() {
    SpannerConnectionHelper helper = new SpannerConnectionHelper();
    helper.close();
  }
}
