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
package com.google.cloud.teleport.v2.templates.source.mysql;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.teleport.v2.spanner.migrations.connection.JdbcConnectionHelper;
import com.google.cloud.teleport.v2.spanner.migrations.shard.Shard;
import com.google.cloud.teleport.v2.spanner.sourceddl.MySqlInformationSchemaScanner;
import com.google.cloud.teleport.v2.spanner.sourceddl.SourceDatabaseType;
import com.google.cloud.teleport.v2.spanner.sourceddl.SourceSchema;
import com.google.cloud.teleport.v2.templates.dbutils.dao.source.JdbcDao;
import com.google.common.collect.ImmutableMap;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;

/** Unit tests for {@link MySQLSourceConnector}. */
public class MySQLSourceConnectorTest {

  @Test
  public void testValidateNotReadOnly_NotReadOnly() throws Exception {
    Shard shard = new Shard();
    Connection mockConn = mock(Connection.class);
    Statement mockStmt = mock(Statement.class);
    ResultSet mockRs = mock(ResultSet.class);

    when(mockConn.createStatement()).thenReturn(mockStmt);
    when(mockStmt.executeQuery("SELECT @@read_only")).thenReturn(mockRs);
    when(mockRs.next()).thenReturn(true);
    when(mockRs.getInt(1)).thenReturn(0);

    try (MockedConstruction<HikariDataSource> mockDsConstruction =
        Mockito.mockConstruction(
            HikariDataSource.class,
            (mockDs, context) -> {
              when(mockDs.getConnection()).thenReturn(mockConn);
            })) {

      MySQLSourceConnector source = new MySQLSourceConnector();
      source.validateNotReadOnly(List.of(shard));
    }
  }

  @Test(expected = RuntimeException.class)
  public void testValidateNotReadOnly_ReadOnly() throws Exception {
    Shard shard = new Shard();
    Connection mockConn = mock(Connection.class);
    Statement mockStmt = mock(Statement.class);
    ResultSet mockRs = mock(ResultSet.class);

    when(mockConn.createStatement()).thenReturn(mockStmt);
    when(mockStmt.executeQuery("SELECT @@read_only")).thenReturn(mockRs);
    when(mockRs.next()).thenReturn(true);
    when(mockRs.getInt(1)).thenReturn(1);

    try (MockedConstruction<HikariDataSource> mockDsConstruction =
        Mockito.mockConstruction(
            HikariDataSource.class,
            (mockDs, context) -> {
              when(mockDs.getConnection()).thenReturn(mockConn);
            })) {

      MySQLSourceConnector source = new MySQLSourceConnector();
      source.validateNotReadOnly(List.of(shard));
    }
  }

  @Test
  public void testGetSourceSchema_Success() throws Exception {
    SourceSchema dummySchema =
        SourceSchema.builder(SourceDatabaseType.MYSQL)
            .databaseName("db")
            .tables(ImmutableMap.of())
            .build();

    try (MockedConstruction<HikariDataSource> mockDsConstruction =
            Mockito.mockConstruction(
                HikariDataSource.class,
                (mockDs, context) -> {
                  when(mockDs.getConnection()).thenReturn(mock(Connection.class));
                });
        MockedConstruction<MySqlInformationSchemaScanner> mockScannerConstruction =
            Mockito.mockConstruction(
                MySqlInformationSchemaScanner.class,
                (mockScanner, context) -> {
                  when(mockScanner.scan()).thenReturn(dummySchema);
                })) {

      MySQLSourceConnector source = new MySQLSourceConnector();
      SourceSchema result = source.getSourceSchema(new Shard());
      Assert.assertSame(dummySchema, result);
    }
  }

  @After
  public void tearDown() {
    MySQLSourceConnector connector = new MySQLSourceConnector();
    try {
      JdbcConnectionHelper helper = (JdbcConnectionHelper) connector.getConnectionHelper();
      helper.setConnectionPoolMap(null);
    } catch (Exception e) {
      // Ignore
    }
  }

  @Test
  public void testGetDmlGenerator() {
    MySQLSourceConnector connector = new MySQLSourceConnector();
    Assert.assertTrue(connector.getDmlGenerator() instanceof MySQLDMLGenerator);
  }

  @Test
  public void testGetConnectionHelper() {
    MySQLSourceConnector connector = new MySQLSourceConnector();
    Assert.assertTrue(connector.getConnectionHelper() instanceof JdbcConnectionHelper);
  }

  @Test
  public void testIsShardingSupported() {
    MySQLSourceConnector connector = new MySQLSourceConnector();
    Assert.assertTrue(connector.isShardingSupported());
  }

  @Test
  public void testGetConnectionUrl() {
    MySQLSourceConnector connector = new MySQLSourceConnector();
    Shard shard = new Shard();
    shard.setHost("localhost");
    shard.setPort("3306");
    shard.setDbName("test_db");
    Assert.assertEquals("jdbc:mysql://localhost:3306/test_db", connector.getConnectionUrl(shard));
  }

  @Test
  public void testGetDao() {
    MySQLSourceConnector connector = new MySQLSourceConnector();
    Shard shard = new Shard();
    shard.setLogicalShardId("shard1");
    shard.setHost("localhost");
    shard.setPort("3306");
    shard.setDbName("test_db");
    shard.setUser("user");
    shard.setPassword("pass");
    Assert.assertTrue(connector.getDao(shard) instanceof JdbcDao);
  }

  @Test
  public void testInitConnectionHelper() {
    MySQLSourceConnector connector = new MySQLSourceConnector();
    Shard shard = new Shard();
    shard.setLogicalShardId("shard1");
    shard.setHost("localhost");
    shard.setPort("3306");
    shard.setDbName("test_db");
    shard.setUser("user");
    shard.setPassword("pass");
    connector.initConnectionHelper(List.of(shard), 10);
    Assert.assertTrue(connector.getConnectionHelper().isConnectionPoolInitialized());
  }
}
