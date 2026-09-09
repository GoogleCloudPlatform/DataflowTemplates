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

import static com.google.cloud.teleport.v2.templates.constants.Constants.PERMANENT_ERROR_TAG;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.teleport.v2.spanner.migrations.connection.ConnectionHelperRequest;
import com.google.cloud.teleport.v2.spanner.migrations.connection.IConnectionHelper;
import com.google.cloud.teleport.v2.spanner.migrations.shard.Shard;
import com.google.cloud.teleport.v2.spanner.sourceddl.SourceSchema;
import com.google.cloud.teleport.v2.templates.dbutils.dao.source.IDao;
import com.google.cloud.teleport.v2.templates.dbutils.dml.IDMLGenerator;
import java.io.File;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLSyntaxErrorException;
import java.sql.SQLTransientConnectionException;
import java.sql.Statement;
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
  public void testGetConnectionUrlWithCustomEncryptionProperties() {
    when(mockShard.getHost()).thenReturn("localhost");
    when(mockShard.getPort()).thenReturn("1433");
    when(mockShard.getDbName()).thenReturn("testdb");
    when(mockShard.getConnectionProperties())
        .thenReturn("encrypt=true;trustServerCertificate=false;loginTimeout=30");

    String url = connector.getConnectionUrl(mockShard);
    assertEquals(
        "jdbc:sqlserver://localhost:1433;databaseName=testdb;encrypt=true;trustServerCertificate=false;loginTimeout=30",
        url);
  }

  @Test
  public void testGetConnectionUrlWithLeadingSemicolonInProperties() {
    when(mockShard.getHost()).thenReturn("localhost");
    when(mockShard.getPort()).thenReturn("1433");
    when(mockShard.getDbName()).thenReturn("testdb");
    when(mockShard.getConnectionProperties()).thenReturn(";encrypt=true");

    String url = connector.getConnectionUrl(mockShard);
    assertEquals(
        "jdbc:sqlserver://localhost:1433;databaseName=testdb;trustServerCertificate=true;encrypt=true",
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

    verify(mockConnectionHelper, never()).init(any(ConnectionHelperRequest.class));
  }

  @Test
  public void testSupportsShardingAndShouldUpdateReadValues() {
    assertTrue(connector.supportsSharding());
    assertTrue(connector.shouldUpdateReadValuesToSpannerRecord());
  }

  @Test
  public void testValidateSuccess() throws Exception {
    Connection mockConn = mock(Connection.class);
    Statement mockStmt = mock(Statement.class);
    ResultSet mockRs = mock(ResultSet.class);

    SQLServerSpToSrcSourceConnector spyConnector = spy(connector);
    doReturn(mockConn).when(spyConnector).createConnection(mockShard);
    when(mockConn.createStatement()).thenReturn(mockStmt);
    when(mockStmt.executeQuery(any())).thenReturn(mockRs);
    when(mockRs.next()).thenReturn(true);
    when(mockRs.getInt(1)).thenReturn(0);

    spyConnector.validate(List.of(mockShard), null);

    verify(mockStmt).executeQuery(any());
  }

  @Test
  public void testValidateReadOnlyThrowsException() throws Exception {
    Connection mockConn = mock(Connection.class);
    Statement mockStmt = mock(Statement.class);
    ResultSet mockRs = mock(ResultSet.class);

    SQLServerSpToSrcSourceConnector spyConnector = spy(connector);
    doReturn(mockConn).when(spyConnector).createConnection(mockShard);
    when(mockConn.createStatement()).thenReturn(mockStmt);
    when(mockStmt.executeQuery(any())).thenReturn(mockRs);
    when(mockRs.next()).thenReturn(true);
    when(mockRs.getInt(1)).thenReturn(1);
    when(mockShard.getLogicalShardId()).thenReturn("shard1");

    RuntimeException exception =
        assertThrows(RuntimeException.class, () -> spyConnector.validate(List.of(mockShard), null));
    assertTrue(exception.getMessage().contains("Error checking SQL Server read-only status"));
    assertTrue(exception.getCause().getMessage().contains("read-only mode for shard: shard1"));
  }

  @Test
  public void testValidateConnectionErrorThrowsException() throws Exception {
    SQLServerSpToSrcSourceConnector spyConnector = spy(connector);
    doThrow(new SQLException("Connection failed")).when(spyConnector).createConnection(mockShard);
    when(mockShard.getLogicalShardId()).thenReturn("shard1");

    RuntimeException exception =
        assertThrows(RuntimeException.class, () -> spyConnector.validate(List.of(mockShard), null));
    assertTrue(exception.getMessage().contains("Error checking SQL Server read-only status"));
  }

  @Test
  public void testGetInformationSchema() throws Exception {
    Connection mockConn = mock(Connection.class);
    DatabaseMetaData mockMeta = mock(DatabaseMetaData.class);
    ResultSet mockTablesRs = mock(ResultSet.class);

    SQLServerSpToSrcSourceConnector spyConnector = spy(connector);
    doReturn(mockConn).when(spyConnector).createConnection(mockShard);
    when(mockConn.getMetaData()).thenReturn(mockMeta);
    when(mockMeta.getTables(any(), any(), any(), any())).thenReturn(mockTablesRs);
    when(mockTablesRs.next()).thenReturn(false);
    when(mockShard.getDbName()).thenReturn("testdb");

    SourceSchema schema = spyConnector.getInformationSchema(List.of(mockShard));

    assertNotNull(schema);
    assertEquals("testdb", schema.databaseName());
  }

  @Test
  public void testClassifyException() {
    assertEquals(
        PERMANENT_ERROR_TAG,
        connector.classifyException(new SQLSyntaxErrorException("syntax error")));
    assertEquals(
        PERMANENT_ERROR_TAG, connector.classifyException(new SQLDataException("data error")));
    assertEquals(
        PERMANENT_ERROR_TAG,
        connector.classifyException(new SQLNonTransientConnectionException("conn error")));
    assertNull(connector.classifyException(new SQLTransientConnectionException("transient")));
    assertNull(connector.classifyException(new RuntimeException("generic")));

    // SQL Server specific error codes
    assertEquals(
        PERMANENT_ERROR_TAG,
        connector.classifyException(new SQLException("Incorrect syntax", "42000", 102)));
    assertEquals(
        PERMANENT_ERROR_TAG,
        connector.classifyException(new SQLException("Invalid column name", "S0002", 207)));
    assertEquals(
        PERMANENT_ERROR_TAG,
        connector.classifyException(new SQLException("Invalid object name", "S0002", 208)));
    assertEquals(
        PERMANENT_ERROR_TAG,
        connector.classifyException(new SQLException("Constraint conflict", "23000", 547)));
    assertEquals(
        PERMANENT_ERROR_TAG,
        connector.classifyException(new SQLException("Duplicate key", "23000", 2627)));
    assertEquals(
        PERMANENT_ERROR_TAG,
        connector.classifyException(new SQLException("String truncation", "22001", 8152)));

    // SQL Server SQLState classes
    assertEquals(
        PERMANENT_ERROR_TAG,
        connector.classifyException(new SQLException("Syntax error", "42S02", 0)));
    assertEquals(
        PERMANENT_ERROR_TAG,
        connector.classifyException(new SQLException("Data error", "22003", 0)));
    assertEquals(
        PERMANENT_ERROR_TAG,
        connector.classifyException(new SQLException("Integrity error", "23505", 0)));

    // Transient / other SQL Server error codes (e.g. deadlock 1205)
    assertNull(
        connector.classifyException(new SQLException("Transaction deadlock", "40001", 1205)));
  }

  @Test
  public void testParseShardConfig() throws Exception {
    File tempFile = File.createTempFile("shard-config", ".json");
    tempFile.deleteOnExit();
    String json =
        "{\"shardConfigs\":[{\"logicalShardId\":\"shard1\",\"host\":\"localhost\",\"user\":\"sa\",\"password\":\"password\",\"port\":\"1433\",\"dbName\":\"testdb\"}]}";
    Files.writeString(tempFile.toPath(), json);

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

    assertThrows(Exception.class, () -> connector.createConnection(mockShard));
  }
}
