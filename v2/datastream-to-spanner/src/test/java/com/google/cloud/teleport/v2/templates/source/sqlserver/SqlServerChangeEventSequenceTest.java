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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.TransactionContext;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.templates.datastream.ChangeEventContext;
import com.google.cloud.teleport.v2.templates.datastream.ChangeEventSequence;
import com.google.cloud.teleport.v2.templates.datastream.ChangeEventSequenceComparisonException;
import com.google.cloud.teleport.v2.templates.datastream.ChangeEventSequenceCreationException;
import java.io.IOException;
import org.json.JSONObject;
import org.junit.Test;

/** Unit tests for testing change event comparison and sequence logic in SQL Server database. */
public final class SqlServerChangeEventSequenceTest {

  private final long previousEventTimestamp = 1615159727L;
  private final long eventTimestamp = 1615159728L;
  private final String lsn1 = "00000021:0000010f:0001";
  private final String lsn2 = "00000021:0000010f:0002";

  private JsonNode getJsonNode(String json) throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    return mapper.readTree(json);
  }

  @Test
  public void canOrderBasedOnTimestamp() {
    SqlServerChangeEventSequence oldEvent =
        new SqlServerChangeEventSequence(previousEventTimestamp, lsn1);
    SqlServerChangeEventSequence newEvent = new SqlServerChangeEventSequence(eventTimestamp, lsn1);

    assertTrue(oldEvent.compareTo(newEvent) < 0);
    assertTrue(newEvent.compareTo(oldEvent) > 0);
  }

  @Test
  public void canOrderBasedOnLsn() {
    SqlServerChangeEventSequence oldEvent = new SqlServerChangeEventSequence(eventTimestamp, lsn1);
    SqlServerChangeEventSequence newEvent = new SqlServerChangeEventSequence(eventTimestamp, lsn2);

    assertTrue(oldEvent.compareTo(newEvent) < 0);
    assertTrue(newEvent.compareTo(oldEvent) > 0);
  }

  @Test
  public void equalTimestampAndLsn() {
    SqlServerChangeEventSequence event1 = new SqlServerChangeEventSequence(eventTimestamp, lsn1);
    SqlServerChangeEventSequence event2 = new SqlServerChangeEventSequence(eventTimestamp, lsn1);

    assertEquals(0, event1.compareTo(event2));
  }

  @Test
  public void canOrderDumpEventAndCdcEventAtSameTimestamp() {
    SqlServerChangeEventSequence dumpEvent = new SqlServerChangeEventSequence(eventTimestamp, "");
    SqlServerChangeEventSequence cdcEvent = new SqlServerChangeEventSequence(eventTimestamp, lsn1);

    assertTrue(dumpEvent.compareTo(cdcEvent) < 0);
    assertTrue(cdcEvent.compareTo(dumpEvent) > 0);
  }

  @Test
  public void compareToThrowsExceptionOnDifferentType() {
    SqlServerChangeEventSequence sequence = new SqlServerChangeEventSequence(eventTimestamp, lsn1);
    ChangeEventSequence dummy = mock(ChangeEventSequence.class);

    assertThrows(ChangeEventSequenceComparisonException.class, () -> sequence.compareTo(dummy));
  }

  @Test
  public void testCreateFromChangeEvent() throws Exception {
    JSONObject json = new JSONObject();
    json.put(SqlServerDsToSpSourceConnector.SQLSERVER_TIMESTAMP_KEY, eventTimestamp);
    json.put(SqlServerDsToSpSourceConnector.SQLSERVER_CHANGE_LSN_KEY, lsn1);

    ChangeEventContext mockContext = mock(ChangeEventContext.class);
    when(mockContext.getChangeEvent()).thenReturn(getJsonNode(json.toString()));

    SqlServerChangeEventSequence sequence =
        SqlServerChangeEventSequence.createFromChangeEvent(mockContext);

    assertNotNull(sequence);
    assertEquals((Long) eventTimestamp, sequence.getTimestamp());
    assertEquals(lsn1, sequence.getLSN());
  }

  @Test
  public void testCreateFromChangeEventWithNullLsn() throws Exception {
    JSONObject json = new JSONObject();
    json.put(SqlServerDsToSpSourceConnector.SQLSERVER_TIMESTAMP_KEY, eventTimestamp);
    json.put(SqlServerDsToSpSourceConnector.SQLSERVER_CHANGE_LSN_KEY, JSONObject.NULL);

    ChangeEventContext mockContext = mock(ChangeEventContext.class);
    when(mockContext.getChangeEvent()).thenReturn(getJsonNode(json.toString()));

    SqlServerChangeEventSequence sequence =
        SqlServerChangeEventSequence.createFromChangeEvent(mockContext);

    assertNotNull(sequence);
    assertEquals((Long) eventTimestamp, sequence.getTimestamp());
    assertEquals("", sequence.getLSN());
  }

  @Test
  public void testCreateFromChangeEventWithMissingLsn() throws Exception {
    JSONObject json = new JSONObject();
    json.put(SqlServerDsToSpSourceConnector.SQLSERVER_TIMESTAMP_KEY, eventTimestamp);

    ChangeEventContext mockContext = mock(ChangeEventContext.class);
    when(mockContext.getChangeEvent()).thenReturn(getJsonNode(json.toString()));

    SqlServerChangeEventSequence sequence =
        SqlServerChangeEventSequence.createFromChangeEvent(mockContext);

    assertNotNull(sequence);
    assertEquals((Long) eventTimestamp, sequence.getTimestamp());
    assertEquals("", sequence.getLSN());
  }

  @Test
  public void testCreateFromShadowTableWithUseSqlStatements() throws Exception {
    TransactionContext transactionContext = mock(TransactionContext.class);
    Ddl shadowTableDdl =
        Ddl.builder()
            .createTable("shadow_table_sqlserver")
            .column("id")
            .int64()
            .endColumn()
            .column("timestamp")
            .int64()
            .endColumn()
            .column("change_lsn")
            .string()
            .max()
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
    when(mockRow.getLong("timestamp")).thenReturn(eventTimestamp);
    when(mockRow.getString("change_lsn")).thenReturn(lsn1);

    ResultSet mockResultSet = mock(ResultSet.class);
    when(mockResultSet.next()).thenReturn(true);
    when(mockResultSet.getCurrentRowAsStruct()).thenReturn(mockRow);
    when(transactionContext.executeQuery(any(Statement.class))).thenReturn(mockResultSet);

    SqlServerChangeEventSequence result =
        SqlServerChangeEventSequence.createFromShadowTable(
            transactionContext, mockContext, shadowTableDdl, /* useSqlStatements= */ true);

    assertNotNull(result);
    assertEquals((Long) eventTimestamp, result.getTimestamp());
    assertEquals(lsn1, result.getLSN());
  }

  @Test
  public void testCreateFromShadowTableWithUseSqlStatementsEmptyResultSet() throws Exception {
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

    ResultSet mockResultSet = mock(ResultSet.class);
    when(mockResultSet.next()).thenReturn(false);
    when(transactionContext.executeQuery(any(Statement.class))).thenReturn(mockResultSet);

    SqlServerChangeEventSequence result =
        SqlServerChangeEventSequence.createFromShadowTable(
            transactionContext, mockContext, shadowTableDdl, /* useSqlStatements= */ true);

    assertNull(result);
  }

  @Test
  public void testCreateFromShadowTableDirectRead() throws Exception {
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
    when(mockRow.getLong("timestamp")).thenReturn(eventTimestamp);
    when(mockRow.getString("change_lsn")).thenReturn(lsn1);

    when(transactionContext.readRow(any(), any(), any())).thenReturn(mockRow);

    SqlServerChangeEventSequence result =
        SqlServerChangeEventSequence.createFromShadowTable(
            transactionContext, mockContext, shadowTableDdl, /* useSqlStatements= */ false);

    assertNotNull(result);
    assertEquals((Long) eventTimestamp, result.getTimestamp());
    assertEquals(lsn1, result.getLSN());
  }

  @Test
  public void testCreateFromShadowTableDirectReadNullRow() throws Exception {
    TransactionContext transactionContext = mock(TransactionContext.class);
    Ddl shadowTableDdl = Ddl.builder().build();

    ChangeEventContext mockContext = mock(ChangeEventContext.class);
    when(mockContext.getShadowTable()).thenReturn("shadow_table_sqlserver");
    when(mockContext.getPrimaryKey()).thenReturn(Key.of(1L));
    when(mockContext.getSafeShadowColumn(SqlServerDsToSpSourceConnector.SQLSERVER_TIMESTAMP_KEY))
        .thenReturn("timestamp");
    when(mockContext.getSafeShadowColumn(SqlServerDsToSpSourceConnector.SQLSERVER_CHANGE_LSN_KEY))
        .thenReturn("change_lsn");

    when(transactionContext.readRow(any(), any(), any())).thenReturn(null);

    SqlServerChangeEventSequence result =
        SqlServerChangeEventSequence.createFromShadowTable(
            transactionContext, mockContext, shadowTableDdl, /* useSqlStatements= */ false);

    assertNull(result);
  }

  @Test
  public void testCreateFromShadowTableThrowsException() throws Exception {
    TransactionContext transactionContext = mock(TransactionContext.class);
    Ddl shadowTableDdl = Ddl.builder().build();

    ChangeEventContext mockContext = mock(ChangeEventContext.class);
    when(mockContext.getShadowTable()).thenReturn("shadow_table_sqlserver");
    when(mockContext.getPrimaryKey()).thenReturn(Key.of(1L));
    when(mockContext.getSafeShadowColumn(any())).thenThrow(new RuntimeException("database error"));

    assertThrows(
        ChangeEventSequenceCreationException.class,
        () ->
            SqlServerChangeEventSequence.createFromShadowTable(
                transactionContext, mockContext, shadowTableDdl, /* useSqlStatements= */ false));
  }

  @Test
  public void testToString() {
    SqlServerChangeEventSequence sequence = new SqlServerChangeEventSequence(12345L, "test-lsn");
    assertEquals(
        "SqlServerChangeEventSequence{timestamp=12345, lsn=test-lsn}", sequence.toString());
  }
}
