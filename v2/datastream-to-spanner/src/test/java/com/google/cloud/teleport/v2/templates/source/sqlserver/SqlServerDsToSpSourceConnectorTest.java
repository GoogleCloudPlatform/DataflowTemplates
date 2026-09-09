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
package com.google.cloud.teleport.v2.templates.source.sqlserver;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.services.datastream.v1.model.MysqlSourceConfig;
import com.google.api.services.datastream.v1.model.SourceConfig;
import com.google.api.services.datastream.v1.model.SqlServerSourceConfig;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.TransactionContext;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.templates.datastream.ChangeEventContext;
import com.google.cloud.teleport.v2.templates.datastream.ChangeEventConvertorTest;
import com.google.cloud.teleport.v2.templates.datastream.ChangeEventSequence;
import com.google.cloud.teleport.v2.templates.datastream.DatastreamConstants;
import java.io.IOException;
import org.json.JSONObject;
import org.junit.Test;

/** Unit tests for {@link SqlServerDsToSpSourceConnector}. */
public final class SqlServerDsToSpSourceConnectorTest {

  private final SqlServerDsToSpSourceConnector connector = new SqlServerDsToSpSourceConnector();

  private JsonNode getJsonNode(String json) throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    return mapper.readTree(json);
  }

  @Test
  public void testGetSourceType() {
    assertEquals("sqlserver", connector.getSourceType());
  }

  @Test
  public void testMatches() {
    SourceConfig sqlserverConfig =
        new SourceConfig().setSqlServerSourceConfig(new SqlServerSourceConfig());
    SourceConfig mysqlConfig = new SourceConfig().setMysqlSourceConfig(new MysqlSourceConfig());

    assertTrue(connector.matchesSourceConfig(sqlserverConfig));
    assertFalse(connector.matchesSourceConfig(mysqlConfig));
  }

  @Test
  public void testGetSortOrder() {
    assertEquals(
        SqlServerDsToSpSourceConnector.SQLSERVER_SORT_ORDER,
        connector.getSortOrder(Dialect.GOOGLE_STANDARD_SQL));
    assertEquals(
        SqlServerDsToSpSourceConnector.SQLSERVER_SORT_ORDER_PG_DIALECT,
        connector.getSortOrder(Dialect.POSTGRESQL));
  }

  @Test
  public void testCreateChangeEventContext() throws Exception {
    Ddl ddl = ChangeEventConvertorTest.getTestDdl();
    JSONObject changeEvent = ChangeEventConvertorTest.getTestChangeEvent("Users2");
    changeEvent.put(SqlServerDsToSpSourceConnector.SQLSERVER_TIMESTAMP_KEY, 1615159728L);
    changeEvent.put(
        SqlServerDsToSpSourceConnector.SQLSERVER_CHANGE_LSN_KEY, "00000021:0000010f:0001");
    changeEvent.put(DatastreamConstants.EVENT_SOURCE_TYPE_KEY, "sqlserver");

    ChangeEventContext context =
        connector.createChangeEventContext(
            getJsonNode(changeEvent.toString()), ddl, ddl, "shadow_");

    assertThat(context, instanceOf(SqlServerChangeEventContext.class));
  }

  @Test
  public void testCreateChangeEventSequenceFromChangeEventContext() throws Exception {
    Ddl ddl = ChangeEventConvertorTest.getTestDdl();
    JSONObject changeEvent = ChangeEventConvertorTest.getTestChangeEvent("Users2");
    changeEvent.put(SqlServerDsToSpSourceConnector.SQLSERVER_TIMESTAMP_KEY, 1615159728L);
    changeEvent.put(
        SqlServerDsToSpSourceConnector.SQLSERVER_CHANGE_LSN_KEY, "00000021:0000010f:0001");
    changeEvent.put(DatastreamConstants.EVENT_SOURCE_TYPE_KEY, "sqlserver");

    ChangeEventContext context =
        connector.createChangeEventContext(
            getJsonNode(changeEvent.toString()), ddl, ddl, "shadow_");

    ChangeEventSequence sequence =
        connector.createChangeEventSequenceFromChangeEventContext(context);

    assertThat(sequence, instanceOf(SqlServerChangeEventSequence.class));
    SqlServerChangeEventSequence sqlServerSequence = (SqlServerChangeEventSequence) sequence;
    assertEquals((Long) 1615159728L, sqlServerSequence.getTimestamp());
    assertEquals("00000021:0000010f:0001", sqlServerSequence.getLSN());
  }

  @Test
  public void testCreateChangeEventSequenceFromShadowTableWithSql() throws Exception {
    TransactionContext transactionContext = mock(TransactionContext.class);
    Ddl shadowTableDdl =
        Ddl.builder()
            .createTable("shadow_table_sqlserver")
            .column("id")
            .int64()
            .endColumn()
            .primaryKey()
            .asc("id")
            .end()
            .endTable()
            .build();

    ChangeEventContext mockContext = mock(ChangeEventContext.class);
    when(mockContext.getShadowTable()).thenReturn("shadow_table_sqlserver");
    when(mockContext.getPrimaryKey()).thenReturn(Key.of(1L));
    when(mockContext.getSafeShadowColumn(SqlServerDsToSpSourceConnector.SQLSERVER_TIMESTAMP_KEY))
        .thenReturn("timestamp");
    when(mockContext.getSafeShadowColumn(SqlServerDsToSpSourceConnector.SQLSERVER_CHANGE_LSN_KEY))
        .thenReturn("change_lsn");

    Struct mockRow = mock(Struct.class);
    when(mockRow.getLong("timestamp")).thenReturn(1615159728L);
    when(mockRow.getString("change_lsn")).thenReturn("00000021:0000010f:0001");

    ResultSet mockResultSet = mock(ResultSet.class);
    when(mockResultSet.next()).thenReturn(true);
    when(mockResultSet.getCurrentRowAsStruct()).thenReturn(mockRow);
    when(transactionContext.executeQuery(any(Statement.class))).thenReturn(mockResultSet);

    ChangeEventSequence sequence =
        connector.createChangeEventSequenceFromShadowTable(
            transactionContext, mockContext, shadowTableDdl, /* useSqlStatements= */ true);

    assertThat(sequence, instanceOf(SqlServerChangeEventSequence.class));
  }

  @Test
  public void testCreateChangeEventSequenceFromShadowTableDirectRead() throws Exception {
    TransactionContext transactionContext = mock(TransactionContext.class);
    Ddl shadowTableDdl = Ddl.builder().build();

    ChangeEventContext mockContext = mock(ChangeEventContext.class);
    when(mockContext.getShadowTable()).thenReturn("shadow_table_sqlserver");
    when(mockContext.getPrimaryKey()).thenReturn(Key.of(1L));
    when(mockContext.getSafeShadowColumn(SqlServerDsToSpSourceConnector.SQLSERVER_TIMESTAMP_KEY))
        .thenReturn("timestamp");
    when(mockContext.getSafeShadowColumn(SqlServerDsToSpSourceConnector.SQLSERVER_CHANGE_LSN_KEY))
        .thenReturn("change_lsn");

    Struct mockRow = mock(Struct.class);
    when(mockRow.getLong("timestamp")).thenReturn(1615159728L);
    when(mockRow.getString("change_lsn")).thenReturn("00000021:0000010f:0001");

    when(transactionContext.readRow(any(), any(), any())).thenReturn(mockRow);

    ChangeEventSequence sequence =
        connector.createChangeEventSequenceFromShadowTable(
            transactionContext, mockContext, shadowTableDdl, /* useSqlStatements= */ false);

    assertThat(sequence, instanceOf(SqlServerChangeEventSequence.class));
  }
}
