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
package com.google.cloud.teleport.v2.templates.source.sqlserver;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.teleport.v2.spanner.migrations.connection.ConnectionHelperRequest;
import com.google.cloud.teleport.v2.spanner.migrations.connection.IConnectionHelper;
import com.google.cloud.teleport.v2.spanner.migrations.shard.Shard;
import com.google.cloud.teleport.v2.templates.dbutils.dao.source.IDao;
import com.google.cloud.teleport.v2.templates.dbutils.dml.IDMLGenerator;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class SQLServerSpToSrcSourceConnectorTest {

  @Mock private IConnectionHelper mockConnectionHelper;
  @Mock private Shard mockShard;

  private SQLServerSpToSrcSourceConnector connector;

  @Before
  public void setUp() {
    connector = new SQLServerSpToSrcSourceConnector(mockConnectionHelper);
  }

  @Test
  public void testGetDmlGenerator() {
    IDMLGenerator dmlGenerator = connector.getDmlGenerator();
    assertNotNull(dmlGenerator);
    assertTrue(dmlGenerator instanceof SQLServerDMLGenerator);
  }

  @Test
  public void testGetConnectionHelper() {
    assertEquals(mockConnectionHelper, connector.getConnectionHelper());
  }

  @Test
  public void testGetConnectionUrl() {
    when(mockShard.getHost()).thenReturn("localhost");
    when(mockShard.getPort()).thenReturn("1433");
    when(mockShard.getDbName()).thenReturn("testdb");

    String url = connector.getConnectionUrl(mockShard);
    assertEquals(
        "jdbc:sqlserver://localhost:1433;databaseName=testdb;trustServerCertificate=true;encrypt=false",
        url);
  }

  @Test
  public void testGetDao() {
    when(mockShard.getHost()).thenReturn("localhost");
    when(mockShard.getPort()).thenReturn("1433");
    when(mockShard.getDbName()).thenReturn("testdb");
    when(mockShard.getUserName()).thenReturn("user");

    IDao dao = connector.getDao(mockShard);
    assertNotNull(dao);
  }

  @Test
  public void testInitConnectionHelper() {
    when(mockConnectionHelper.isConnectionPoolInitialized()).thenReturn(false);
    doNothing().when(mockConnectionHelper).init(any(ConnectionHelperRequest.class));

    connector.initConnectionHelper(List.of(mockShard), 10);

    verify(mockConnectionHelper).init(any(ConnectionHelperRequest.class));
  }

  @Test
  public void testGetConnectionUrlWithConnectionProperties() {
    when(mockShard.getHost()).thenReturn("localhost");
    when(mockShard.getPort()).thenReturn("1433");
    when(mockShard.getDbName()).thenReturn("testdb");
    when(mockShard.getConnectionProperties()).thenReturn("applicationName=test;loginTimeout=30");

    String url = connector.getConnectionUrl(mockShard);
    assertEquals(
        "jdbc:sqlserver://localhost:1433;databaseName=testdb;trustServerCertificate=true;encrypt=false;applicationName=test;loginTimeout=30",
        url);
  }

  @Test
  public void testDefaultConstructor() {
    SQLServerSpToSrcSourceConnector defaultConnector = new SQLServerSpToSrcSourceConnector();
    assertNotNull(defaultConnector.getConnectionHelper());
  }

  @Test
  public void testInitConnectionHelperAlreadyInitialized() {
    when(mockConnectionHelper.isConnectionPoolInitialized()).thenReturn(true);

    connector.initConnectionHelper(List.of(mockShard), 10);

    org.mockito.Mockito.verify(mockConnectionHelper, org.mockito.Mockito.never())
        .init(any(ConnectionHelperRequest.class));
  }

  @Test
  public void testSupportsShardingAndShouldUpdateReadValues() {
    assertTrue(connector.supportsSharding());
    assertTrue(connector.shouldUpdateReadValuesToSpannerRecord());
  }

  @Test
  public void testValidateSuccess() throws Exception {
    java.sql.Connection mockConn = org.mockito.Mockito.mock(java.sql.Connection.class);
    java.sql.Statement mockStmt = org.mockito.Mockito.mock(java.sql.Statement.class);
    java.sql.ResultSet mockRs = org.mockito.Mockito.mock(java.sql.ResultSet.class);

    SQLServerSpToSrcSourceConnector spyConnector = org.mockito.Mockito.spy(connector);
    org.mockito.Mockito.doReturn(mockConn).when(spyConnector).createConnection(mockShard);
    when(mockConn.createStatement()).thenReturn(mockStmt);
    when(mockStmt.executeQuery(any())).thenReturn(mockRs);
    when(mockRs.next()).thenReturn(true);
    when(mockRs.getInt(1)).thenReturn(0);

    spyConnector.validate(List.of(mockShard), null);

    verify(mockStmt).executeQuery(any());
  }

  @Test
  public void testValidateReadOnlyThrowsException() throws Exception {
    java.sql.Connection mockConn = org.mockito.Mockito.mock(java.sql.Connection.class);
    java.sql.Statement mockStmt = org.mockito.Mockito.mock(java.sql.Statement.class);
    java.sql.ResultSet mockRs = org.mockito.Mockito.mock(java.sql.ResultSet.class);

    SQLServerSpToSrcSourceConnector spyConnector = org.mockito.Mockito.spy(connector);
    org.mockito.Mockito.doReturn(mockConn).when(spyConnector).createConnection(mockShard);
    when(mockConn.createStatement()).thenReturn(mockStmt);
    when(mockStmt.executeQuery(any())).thenReturn(mockRs);
    when(mockRs.next()).thenReturn(true);
    when(mockRs.getInt(1)).thenReturn(1);
    when(mockShard.getLogicalShardId()).thenReturn("shard1");

    RuntimeException exception =
        org.junit.Assert.assertThrows(
            RuntimeException.class, () -> spyConnector.validate(List.of(mockShard), null));
    assertTrue(exception.getMessage().contains("Error checking SQL Server read-only status"));
    assertTrue(exception.getCause().getMessage().contains("read-only mode for shard: shard1"));
  }

  @Test
  public void testValidateConnectionErrorThrowsException() throws Exception {
    SQLServerSpToSrcSourceConnector spyConnector = org.mockito.Mockito.spy(connector);
    org.mockito.Mockito.doThrow(new java.sql.SQLException("Connection failed"))
        .when(spyConnector)
        .createConnection(mockShard);
    when(mockShard.getLogicalShardId()).thenReturn("shard1");

    RuntimeException exception =
        org.junit.Assert.assertThrows(
            RuntimeException.class, () -> spyConnector.validate(List.of(mockShard), null));
    assertTrue(exception.getMessage().contains("Error checking SQL Server read-only status"));
  }

  @Test
  public void testGetInformationSchema() throws Exception {
    java.sql.Connection mockConn = org.mockito.Mockito.mock(java.sql.Connection.class);
    java.sql.DatabaseMetaData mockMeta = org.mockito.Mockito.mock(java.sql.DatabaseMetaData.class);
    java.sql.ResultSet mockTablesRs = org.mockito.Mockito.mock(java.sql.ResultSet.class);

    SQLServerSpToSrcSourceConnector spyConnector = org.mockito.Mockito.spy(connector);
    org.mockito.Mockito.doReturn(mockConn).when(spyConnector).createConnection(mockShard);
    when(mockConn.getMetaData()).thenReturn(mockMeta);
    when(mockMeta.getTables(any(), any(), any(), any())).thenReturn(mockTablesRs);
    when(mockTablesRs.next()).thenReturn(false);
    when(mockShard.getDbName()).thenReturn("testdb");

    com.google.cloud.teleport.v2.spanner.sourceddl.SourceSchema schema =
        spyConnector.getInformationSchema(List.of(mockShard));

    assertNotNull(schema);
    assertEquals("testdb", schema.databaseName());
  }

  @Test
  public void testClassifyException() {
    assertEquals(
        com.google.cloud.teleport.v2.templates.constants.Constants.PERMANENT_ERROR_TAG,
        connector.classifyException(new java.sql.SQLSyntaxErrorException("syntax error")));
    assertEquals(
        com.google.cloud.teleport.v2.templates.constants.Constants.PERMANENT_ERROR_TAG,
        connector.classifyException(new java.sql.SQLDataException("data error")));
    assertEquals(
        com.google.cloud.teleport.v2.templates.constants.Constants.PERMANENT_ERROR_TAG,
        connector.classifyException(new java.sql.SQLNonTransientConnectionException("conn error")));
    org.junit.Assert.assertNull(
        connector.classifyException(new java.sql.SQLTransientConnectionException("transient")));
    org.junit.Assert.assertNull(connector.classifyException(new RuntimeException("generic")));
  }

  @Test
  public void testParseShardConfig() throws Exception {
    java.io.File tempFile = java.io.File.createTempFile("shard-config", ".json");
    tempFile.deleteOnExit();
    String json =
        "{\"shardConfigs\":[{\"logicalShardId\":\"shard1\",\"host\":\"localhost\",\"user\":\"sa\",\"password\":\"password\",\"port\":\"1433\",\"dbName\":\"testdb\"}]}";
    java.nio.file.Files.writeString(tempFile.toPath(), json);

    List<Shard> shards = connector.parseShardConfig(tempFile.getAbsolutePath());
    assertNotNull(shards);
    assertEquals(1, shards.size());
    assertEquals("shard1", shards.get(0).getLogicalShardId());
  }

  @Test
  public void testCreateConnectionThrowsExceptionForInvalidHost() {
    when(mockShard.getHost()).thenReturn("invalidhost.local");
    when(mockShard.getPort()).thenReturn("1433");
    when(mockShard.getDbName()).thenReturn("testdb");
    when(mockShard.getUserName()).thenReturn("user");
    when(mockShard.getPassword()).thenReturn("password");
    when(mockShard.getConnectionProperties()).thenReturn("loginTimeout=1");

    org.junit.Assert.assertThrows(Exception.class, () -> connector.createConnection(mockShard));
  }
}
