/*
 * Copyright (C) 2025 Google LLC
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.OptionsMap;
import com.google.cloud.teleport.v2.spanner.migrations.shard.CassandraShard;
import com.google.cloud.teleport.v2.spanner.migrations.shard.Shard;
import com.google.cloud.teleport.v2.spanner.migrations.utils.CassandraDriverConfigLoader;
import com.google.cloud.teleport.v2.templates.exceptions.ConnectionException;
import com.google.cloud.teleport.v2.templates.models.ConnectionHelperRequest;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

public class CassandraConnectionHelperTest {

  private CqlSessionBuilder cqlSessionBuilder;
  private CqlSession cqlSession;
  private OptionsMap optionsMap;
  private CassandraShard cassandraShard;
  private CassandraConnectionHelper connectionHelper;
  private DriverConfigLoader driverConfigLoader;

  @Before
  public void setUp() {
    connectionHelper = new CassandraConnectionHelper();
    cassandraShard = mock(CassandraShard.class);
    optionsMap = mock(OptionsMap.class);
    driverConfigLoader = mock(DriverConfigLoader.class);
    cqlSessionBuilder = mock(CqlSessionBuilder.class);
    cqlSession = mock(CqlSession.class);
  }

  @Test
  public void testInit_ShouldInitializeConnectionPool() {
    when(cassandraShard.getHost()).thenReturn("localhost");
    when(cassandraShard.getPort()).thenReturn("9042");
    when(cassandraShard.getUserName()).thenReturn("user");
    when(cassandraShard.getPassword()).thenReturn("password");
    when(cassandraShard.getKeySpaceName()).thenReturn("mykeyspace");
    when(cassandraShard.getOptionsMap()).thenReturn(optionsMap);

    try (MockedStatic<CassandraDriverConfigLoader> mockFileReader =
        Mockito.mockStatic(CassandraDriverConfigLoader.class)) {
      mockFileReader
          .when(() -> CassandraDriverConfigLoader.fromOptionsMap(optionsMap))
          .thenReturn(driverConfigLoader);

      try (MockedStatic<CqlSession> mockedCqlSession = Mockito.mockStatic(CqlSession.class)) {
        mockedCqlSession.when(CqlSession::builder).thenReturn(cqlSessionBuilder);
        when(cqlSessionBuilder.withConfigLoader(driverConfigLoader)).thenReturn(cqlSessionBuilder);
        when(cqlSessionBuilder.build()).thenReturn(cqlSession);

        ConnectionHelperRequest request = mock(ConnectionHelperRequest.class);
        when(request.getShards()).thenReturn(Collections.singletonList(cassandraShard));
        when(request.getMaxConnections()).thenReturn(10);
        connectionHelper.setConnectionPoolMap(new ConcurrentHashMap<>());
        connectionHelper.init(request);

        assertTrue(connectionHelper.isConnectionPoolInitialized());
      }
    }
  }

  @Test
  public void testGetConnection_ShouldReturnValidSession() throws ConnectionException {
    String connectionKey = "localhost:9042/user/mykeyspace";
    connectionHelper.setConnectionPoolMap(Map.of(connectionKey, cqlSession));

    CqlSession session = connectionHelper.getConnection(connectionKey);
    assertNotNull(session);
    assertEquals(cqlSession, session);
  }

  @Test
  public void testGetConnection_ShouldThrowException_WhenConnectionNotFound() {
    assertThrows(ConnectionException.class, () -> connectionHelper.getConnection("invalidKey"));
  }

  @Test
  public void testGetConnection_ShouldThrowConnectionException_WhenPoolNotInitialized() {
    connectionHelper.setConnectionPoolMap(null);
    assertThrows(ConnectionException.class, () -> connectionHelper.getConnection("anyKey"));
  }

  @Test
  public void testSetConnectionPoolMap_ShouldOverrideConnectionPoolMap()
      throws ConnectionException {
    connectionHelper.setConnectionPoolMap(Map.of("localhost:9042/user/mykeyspace", cqlSession));

    CqlSession session = connectionHelper.getConnection("localhost:9042/user/mykeyspace");
    assertNotNull(session);
    assertEquals(cqlSession, session);
  }

  @Test
  public void testGetConnectionPoolNotFound() {
    connectionHelper.setConnectionPoolMap(Map.of());
    ConnectionException exception =
        assertThrows(
            ConnectionException.class, () -> connectionHelper.getConnection("nonexistentKey"));
    assertEquals("Connection pool is not initialized.", exception.getMessage());
  }

  @Test
  public void testGetConnectionWhenPoolNotInitialized() {
    connectionHelper.setConnectionPoolMap(null);
    ConnectionException exception =
        assertThrows(
            ConnectionException.class,
            () -> connectionHelper.getConnection("localhost:9042/testuser/testKeyspace"));
    assertEquals("Connection pool is not initialized.", exception.getMessage());
  }

  @Test
  public void testGetConnectionWithValidKey() throws ConnectionException {
    String connectionKey = "localhost:9042/testuser/testKeyspace";
    connectionHelper.setConnectionPoolMap(Map.of(connectionKey, cqlSession));

    CqlSession session = connectionHelper.getConnection(connectionKey);
    assertEquals(cqlSession, session);
  }

  @Test
  public void testInit_ShouldThrowIllegalArgumentException_WhenInvalidShardTypeIsProvided() {
    Shard invalidShard = mock(Shard.class);
    ConnectionHelperRequest request = mock(ConnectionHelperRequest.class);
    when(request.getShards()).thenReturn(Collections.singletonList(invalidShard));

    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> connectionHelper.init(request));
    assertEquals("Invalid shard object", exception.getMessage());
  }
}
