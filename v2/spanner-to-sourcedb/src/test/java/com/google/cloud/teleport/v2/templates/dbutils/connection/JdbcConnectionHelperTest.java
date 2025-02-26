/*
 * Copyright (C) 2024 Google LLC
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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.teleport.v2.spanner.migrations.shard.Shard;
import com.google.cloud.teleport.v2.templates.exceptions.ConnectionException;
import com.google.cloud.teleport.v2.templates.models.ConnectionHelperRequest;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.MockitoAnnotations;

@RunWith(JUnit4.class)
public class JdbcConnectionHelperTest {
  private JdbcConnectionHelper connectionHelper;

  @Mock private HikariDataSource mockDataSource;

  @Mock private Connection mockConnection;

  @Mock private HikariConfig mockHikariConfig;

  @Before
  public void setUp() {
    MockitoAnnotations.openMocks(this);
    connectionHelper = new JdbcConnectionHelper();
    mockDataSource = mock(HikariDataSource.class);
  }

  @Test
  public void testIsConnectionPoolInitialized() {
    // Connection pool not initialized
    connectionHelper.setConnectionPoolMap(null);
    assertFalse(connectionHelper.isConnectionPoolInitialized());

    // Mock initialization
    connectionHelper.setConnectionPoolMap(Map.of("key", mockDataSource));
    assertTrue(connectionHelper.isConnectionPoolInitialized());
  }

  @Test
  public void testGetConnection() throws Exception {
    String connectionRequestKey = "jdbc:mysql://localhost:3306/testdb/user";
    when(mockDataSource.getConnection()).thenReturn(mockConnection);

    connectionHelper.setConnectionPoolMap(Map.of(connectionRequestKey, mockDataSource));

    Connection connection = connectionHelper.getConnection(connectionRequestKey);

    assertEquals(connection, mockConnection);
    verify(mockDataSource).getConnection();
  }

  @Test
  public void testGetConnectionPoolNotFound() throws ConnectionException {
    connectionHelper.setConnectionPoolMap(Map.of());
    assertNull(connectionHelper.getConnection("invalid-key"));
  }

  @Test
  public void testInitConnectionPool() {
    ConnectionHelperRequest mockRequest = mock(ConnectionHelperRequest.class);
    Shard mockShard = mock(Shard.class);
    when(mockShard.getHost()).thenReturn("localhost");
    when(mockShard.getPort()).thenReturn("3306");
    when(mockShard.getDbName()).thenReturn("testdb");
    when(mockShard.getUserName()).thenReturn("testuser");
    when(mockShard.getPassword()).thenReturn("testpassword");
    when(mockShard.getConnectionProperties()).thenReturn("useSSL=false");

    List<Shard> mockShards = Collections.singletonList(mockShard);
    when(mockRequest.getShards()).thenReturn(mockShards);
    when(mockRequest.getDriver()).thenReturn("com.mysql.cj.jdbc.Driver");
    when(mockRequest.getMaxConnections()).thenReturn(10);
    when(mockRequest.getConnectionInitQuery()).thenReturn("SELECT 1");

    try (MockedConstruction<HikariDataSource> mockedConstruction =
        mockConstruction(
            HikariDataSource.class,
            (mock, context) -> when(mock.getConnection()).thenReturn(mock(Connection.class)))) {
      connectionHelper.init(mockRequest);

      assertTrue(connectionHelper.isConnectionPoolInitialized());
    }
  }
}
