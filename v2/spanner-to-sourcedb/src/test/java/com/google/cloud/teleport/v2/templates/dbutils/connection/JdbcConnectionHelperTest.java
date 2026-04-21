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

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;
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
    connectionHelper.setConnectionPoolMap(null); // Reset singleton state
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
    when(mockRequest.getJdbcUrlPrefix()).thenReturn("jdbc:mysql://");

    try (MockedConstruction<HikariDataSource> mockedDsConstruction =
        mockConstruction(
            HikariDataSource.class,
            (mock, context) -> {
              Connection mockConnection = mock(Connection.class);
              Statement mockStatement = mock(Statement.class);
              ResultSet mockResultSet = mock(ResultSet.class);
              when(mockResultSet.next()).thenReturn(true);
              when(mockResultSet.getBoolean(1)).thenReturn(false); // Not read-only
              when(mockStatement.executeQuery("SELECT @@global.read_only"))
                  .thenReturn(mockResultSet);
              when(mockConnection.createStatement()).thenReturn(mockStatement);
              when(mock.getConnection()).thenReturn(mockConnection);
            })) {
      try (MockedConstruction<HikariConfig> mockedConfigConstruction =
          mockConstruction(HikariConfig.class)) {
        connectionHelper.init(mockRequest);

        assertTrue(connectionHelper.isConnectionPoolInitialized());
        // Verify HikariConfig properties
        HikariConfig capturedConfig = mockedConfigConstruction.constructed().get(0);
        verify(capturedConfig).setJdbcUrl("jdbc:mysql://localhost:3306/testdb");
        verify(capturedConfig).setUsername("testuser");
        verify(capturedConfig).setPassword("testpassword");
        verify(capturedConfig).setDriverClassName("com.mysql.cj.jdbc.Driver");
        verify(capturedConfig).setMaximumPoolSize(10);
        verify(capturedConfig).setConnectionInitSql("SELECT 1");
        verify(capturedConfig).addDataSourceProperty("useSSL", "false");

        // Verify HikariDataSource was created with the config
        assertThat(mockedDsConstruction.constructed()).hasSize(1);
        ArgumentCaptor<HikariConfig> configCaptor = ArgumentCaptor.forClass(HikariConfig.class);
        HikariDataSource createdDataSource = mockedDsConstruction.constructed().get(0);
        assertThat(createdDataSource).isNotNull();
      }
    }
  }

  @Test
  public void testInit_readOnlyShard_throwsException() throws SQLException {
    ConnectionHelperRequest mockRequest = mock(ConnectionHelperRequest.class);
    Shard mockShard = mock(Shard.class);
    when(mockShard.getHost()).thenReturn("localhost");
    when(mockShard.getPort()).thenReturn("3306");
    when(mockShard.getDbName()).thenReturn("testdb");
    when(mockShard.getUserName()).thenReturn("testuser");
    when(mockShard.getPassword()).thenReturn("testpassword");
    when(mockRequest.getDriver()).thenReturn("com.mysql.cj.jdbc.Driver");
    when(mockRequest.getMaxConnections()).thenReturn(1);

    List<Shard> mockShards = Collections.singletonList(mockShard);
    when(mockRequest.getShards()).thenReturn(mockShards);

    // Mock the JDBC objects to simulate a read-only database
    ResultSet mockResultSet = mock(ResultSet.class);
    when(mockResultSet.next()).thenReturn(true);
    when(mockResultSet.getBoolean(1)).thenReturn(true); // Read-only is true

    Statement mockStatement = mock(Statement.class);
    when(mockStatement.executeQuery("SELECT @@global.read_only")).thenReturn(mockResultSet);

    Connection mockConnection = mock(Connection.class);
    when(mockConnection.createStatement()).thenReturn(mockStatement);

    try (MockedConstruction<HikariDataSource> mockedDsConstruction =
        mockConstruction(
            HikariDataSource.class,
            (mock, context) -> {
              when(mock.getConnection()).thenReturn(mockConnection);
            })) {

      try {
        connectionHelper.init(mockRequest);
        fail("Expected RuntimeException was not thrown");
      } catch (RuntimeException e) {
        assertTrue(e.getCause() instanceof SQLException);
        assertEquals(
            "Connection rejected: MySQL Shard is in read-only mode.", e.getCause().getMessage());
      }
    }
  }

  @Test
  public void testInit_writableShard_succeeds() throws SQLException {
    ConnectionHelperRequest mockRequest = mock(ConnectionHelperRequest.class);
    Shard mockShard = mock(Shard.class);
    when(mockShard.getHost()).thenReturn("localhost");
    when(mockShard.getPort()).thenReturn("3306");
    when(mockShard.getDbName()).thenReturn("testdb");
    when(mockShard.getUserName()).thenReturn("testuser");
    when(mockShard.getPassword()).thenReturn("testpassword");

    List<Shard> mockShards = Collections.singletonList(mockShard);
    when(mockRequest.getShards()).thenReturn(mockShards);

    // Mock the JDBC objects to simulate a writable database
    ResultSet mockResultSet = mock(ResultSet.class);
    when(mockResultSet.next()).thenReturn(true);
    when(mockResultSet.getBoolean(1)).thenReturn(false); // Read-only is false

    Statement mockStatement = mock(Statement.class);
    when(mockStatement.executeQuery("SELECT @@global.read_only")).thenReturn(mockResultSet);

    Connection mockConnection = mock(Connection.class);
    when(mockConnection.createStatement()).thenReturn(mockStatement);

    try (MockedConstruction<HikariDataSource> mockedDsConstruction =
        mockConstruction(
            HikariDataSource.class,
            (mock, context) -> {
              when(mock.getConnection()).thenReturn(mockConnection);
            })) {
      try (MockedConstruction<HikariConfig> mockedConfigConstruction =
          mockConstruction(HikariConfig.class)) {
        // No exception should be thrown
        connectionHelper.init(mockRequest);
        assertTrue(connectionHelper.isConnectionPoolInitialized());
      }
    }
  }
}
