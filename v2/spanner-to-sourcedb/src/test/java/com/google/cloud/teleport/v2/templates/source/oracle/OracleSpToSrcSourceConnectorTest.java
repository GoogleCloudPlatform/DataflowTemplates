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
package com.google.cloud.teleport.v2.templates.source.oracle;

import static com.google.cloud.teleport.v2.templates.constants.Constants.PERMANENT_ERROR_TAG;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.teleport.v2.spanner.migrations.connection.ConnectionHelperRequest;
import com.google.cloud.teleport.v2.spanner.migrations.connection.IConnectionHelper;
import com.google.cloud.teleport.v2.spanner.migrations.connection.JdbcConnectionHelper;
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
import java.sql.SQLSyntaxErrorException;
import java.util.List;
import java.util.Properties;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class OracleSpToSrcSourceConnectorTest {

  @Mock private IConnectionHelper<Connection> mockConnectionHelper;

  private OracleSpToSrcSourceConnector connector;

  @Before
  public void setUp() {
    connector = new OracleSpToSrcSourceConnector(mockConnectionHelper);
  }

  @Test
  public void testDefaultConstructorAndCapabilities() {
    OracleSpToSrcSourceConnector defaultConnector = new OracleSpToSrcSourceConnector();
    assertNotNull(defaultConnector.getConnectionHelper());
    IDMLGenerator generator = defaultConnector.getDmlGenerator();
    assertTrue(generator instanceof OracleDMLGenerator);
    assertTrue(defaultConnector.supportsSharding());
    assertTrue(defaultConnector.shouldUpdateReadValuesToSpannerRecord());
  }

  @Test
  public void testGetConnectionUrlAndDao() {
    Shard shard = new Shard("shard1", "10.0.0.5", "1521", "hr", "pwd", "xepdb1", "", "", "");
    assertEquals("jdbc:oracle:thin:@//10.0.0.5:1521/xepdb1", connector.getConnectionUrl(shard));

    IDao dao = connector.getDao(shard);
    assertTrue(dao instanceof OracleJdbcDao);
  }

  @Test
  public void testInitConnectionHelperSetsDefaultImplicitStatementCache() {
    when(mockConnectionHelper.isConnectionPoolInitialized()).thenReturn(false);
    Shard shardEmpty = new Shard("s1", "h1", "1521", "u1", "p1", "pdb1", "", "", "");
    Shard shardSemicolon =
        new Shard(
            "s2",
            "h2",
            "1521",
            "u2",
            "p2",
            "pdb2",
            "",
            "",
            "oracle.net.CONNECT_TIMEOUT=5000;oracle.jdbc.ReadTimeout=30000");
    Shard shardAmpersand =
        new Shard(
            "s3",
            "h3",
            "1521",
            "u3",
            "p3",
            "pdb3",
            "",
            "",
            "oracle.net.CONNECT_TIMEOUT=5000&oracle.jdbc.ReadTimeout=30000");
    Shard shardNewline =
        new Shard(
            "s4", "h4", "1521", "u4", "p4", "pdb4", "", "", "oracle.net.CONNECT_TIMEOUT=5000");
    Shard shardUserOverride =
        new Shard(
            "s5",
            "h5",
            "1521",
            "u5",
            "p5",
            "pdb5",
            "",
            "",
            "oracle.jdbc.implicitStatementCacheSize=250");
    Shard shardDisabledCache =
        new Shard(
            "s6",
            "h6",
            "1521",
            "u6",
            "p6",
            "pdb6",
            "",
            "",
            "oracle.jdbc.implicitStatementCacheSize=0");
    Shard shardShortAlias =
        new Shard("s7", "h7", "1521", "u7", "p7", "pdb7", "", "", "implicitStatementCacheSize=50");

    List<Shard> shards =
        List.of(
            shardEmpty,
            shardSemicolon,
            shardAmpersand,
            shardNewline,
            shardUserOverride,
            shardDisabledCache,
            shardShortAlias);
    connector.initConnectionHelper(shards, 50);

    ArgumentCaptor<ConnectionHelperRequest> captor =
        ArgumentCaptor.forClass(ConnectionHelperRequest.class);
    verify(mockConnectionHelper).init(captor.capture());
    assertEquals("oracle.jdbc.OracleDriver", captor.getValue().getDriver());
    assertEquals(50, captor.getValue().getMaxConnections());

    for (Shard s : List.of(shardEmpty, shardSemicolon, shardAmpersand, shardNewline)) {
      Properties parsed = JdbcConnectionHelper.parseProperties(s.getConnectionProperties());
      assertEquals(
          OracleSpToSrcSourceConnector.DEFAULT_IMPLICIT_STATEMENT_CACHE_SIZE,
          parsed.getProperty(OracleSpToSrcSourceConnector.IMPLICIT_STATEMENT_CACHE_PROP));
    }
    assertEquals(
        "250",
        JdbcConnectionHelper.parseProperties(shardUserOverride.getConnectionProperties())
            .getProperty(OracleSpToSrcSourceConnector.IMPLICIT_STATEMENT_CACHE_PROP));
    assertEquals(
        "0",
        JdbcConnectionHelper.parseProperties(shardDisabledCache.getConnectionProperties())
            .getProperty(OracleSpToSrcSourceConnector.IMPLICIT_STATEMENT_CACHE_PROP));
    Properties shortAliasProps =
        JdbcConnectionHelper.parseProperties(shardShortAlias.getConnectionProperties());
    assertEquals("50", shortAliasProps.getProperty("implicitStatementCacheSize"));
    assertNull(
        shortAliasProps.getProperty(OracleSpToSrcSourceConnector.IMPLICIT_STATEMENT_CACHE_PROP));
  }

  @Test
  public void testInitConnectionHelperSkipsWhenAlreadyInitialized() {
    when(mockConnectionHelper.isConnectionPoolInitialized()).thenReturn(true);
    Shard shard = new Shard("s1", "h1", "1521", "u1", "p1", "pdb1", "", "", "");
    connector.initConnectionHelper(List.of(shard), 10);
    verify(mockConnectionHelper, never()).init(any());
  }

  @Test
  public void testClassifyExceptionWalksCauseChain() {
    assertEquals(
        PERMANENT_ERROR_TAG, connector.classifyException(new SQLSyntaxErrorException("ORA-00904")));
    assertEquals(
        PERMANENT_ERROR_TAG,
        connector.classifyException(new RuntimeException(new SQLDataException("ORA-01858"))));
    assertNull(connector.classifyException(new SQLException("ORA-00060 deadlock")));
    assertNull(connector.classifyException(null));
  }

  @Test
  public void testParseShardConfigAndInformationSchema() throws Exception {
    File tempFile = File.createTempFile("oracle-shards", ".json");
    tempFile.deleteOnExit();
    Files.writeString(
        tempFile.toPath(),
        "{\"shardConfigs\":[{\"logicalShardId\":\"shard1\",\"host\":\"localhost\",\"user\":\"hr\",\"password\":\"pwd\",\"port\":\"1521\",\"dbName\":\"xepdb1\"}]}");

    List<Shard> shards = connector.parseShardConfig(tempFile.getAbsolutePath());
    assertEquals(1, shards.size());
    assertEquals("shard1", shards.get(0).getLogicalShardId());

    Connection mockConn = mock(Connection.class);
    DatabaseMetaData mockMeta = mock(DatabaseMetaData.class);
    ResultSet mockTablesRs = mock(ResultSet.class);
    when(mockConn.getSchema()).thenReturn("HR");
    when(mockConn.getMetaData()).thenReturn(mockMeta);
    when(mockMeta.getTables(any(), any(), any(), any())).thenReturn(mockTablesRs);
    when(mockTablesRs.next()).thenReturn(false);

    OracleSpToSrcSourceConnector spyConnector = spy(connector);
    doReturn(mockConn).when(spyConnector).createConnection(shards.get(0));

    SourceSchema schema = spyConnector.getInformationSchema(shards);
    assertNotNull(schema);
    spyConnector.validate(shards, null);
  }

  @Test
  public void testWithDefaultStatementCacheHandlesTrailingSeparators() {
    Properties p1 =
        JdbcConnectionHelper.parseProperties(
            OracleSpToSrcSourceConnector.withDefaultStatementCache("a=1;"));
    assertEquals("1", p1.getProperty("a"));
    assertEquals("100", p1.getProperty(OracleSpToSrcSourceConnector.IMPLICIT_STATEMENT_CACHE_PROP));

    Properties p2 =
        JdbcConnectionHelper.parseProperties(
            OracleSpToSrcSourceConnector.withDefaultStatementCache("a=1&"));
    assertEquals("1", p2.getProperty("a"));
    assertEquals("100", p2.getProperty(OracleSpToSrcSourceConnector.IMPLICIT_STATEMENT_CACHE_PROP));

    assertThrows(
        IllegalArgumentException.class,
        () -> OracleSpToSrcSourceConnector.withDefaultStatementCache("malformed;entry"));
  }
}
