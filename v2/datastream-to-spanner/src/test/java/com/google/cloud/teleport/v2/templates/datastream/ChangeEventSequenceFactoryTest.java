/*
 * Copyright (C) 2021 Google LLC
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
package com.google.cloud.teleport.v2.templates.datastream;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.TransactionContext;
import org.json.JSONObject;
import org.junit.Test;

/** Unit tests for testing creation methods of ChangeEventSequenceFactory class. */
public final class ChangeEventSequenceFactoryTest {

  private final long eventTimestamp = 1615159728L;

  ChangeEventContext getMockMySqlChangeEventContext(
      boolean addMysqlPositionFields, boolean cdcEvent) throws Exception {
    // Create dummy mysql change event.
    JSONObject mysqlChangeEvent = new JSONObject();
    mysqlChangeEvent.put(
        DatastreamConstants.EVENT_SOURCE_TYPE_KEY, DatastreamConstants.MYSQL_SOURCE_TYPE);
    mysqlChangeEvent.put(
        DatastreamConstants.EVENT_CHANGE_TYPE_KEY, DatastreamConstants.INSERT_EVENT);
    mysqlChangeEvent.put(DatastreamConstants.MYSQL_TIMESTAMP_KEY, eventTimestamp);
    if (addMysqlPositionFields) {
      if (cdcEvent) {
        mysqlChangeEvent.put(DatastreamConstants.MYSQL_LOGFILE_KEY, "file1.log");
        mysqlChangeEvent.put(DatastreamConstants.MYSQL_LOGPOSITION_KEY, 2L);
      } else {
        mysqlChangeEvent.put(DatastreamConstants.MYSQL_LOGFILE_KEY, JSONObject.NULL);
        mysqlChangeEvent.put(DatastreamConstants.MYSQL_LOGPOSITION_KEY, JSONObject.NULL);
      }
    }

    ObjectMapper mapper = new ObjectMapper();
    mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    JsonNode jsonNode = mapper.readTree(mysqlChangeEvent.toString());

    // Prepare mock ChangeEventContext.
    ChangeEventContext mockContext = mock(ChangeEventContext.class);
    when(mockContext.getChangeEvent()).thenReturn(jsonNode);
    when(mockContext.getPrimaryKey()).thenReturn(Key.of("test"));
    when(mockContext.getShadowTable()).thenReturn("test");

    return mockContext;
  }

  ChangeEventContext getMockOracleChangeEventContext(
      boolean addOraclePositionFields, boolean cdcEvent) throws Exception {
    // Create dummy oracle change event.
    JSONObject oracleChangeEvent = new JSONObject();
    oracleChangeEvent.put(
        DatastreamConstants.EVENT_SOURCE_TYPE_KEY, DatastreamConstants.ORACLE_SOURCE_TYPE);
    oracleChangeEvent.put(
        DatastreamConstants.EVENT_CHANGE_TYPE_KEY, DatastreamConstants.INSERT_EVENT);
    oracleChangeEvent.put(DatastreamConstants.ORACLE_TIMESTAMP_KEY, eventTimestamp);
    if (addOraclePositionFields) {
      if (cdcEvent) {
        oracleChangeEvent.put(DatastreamConstants.ORACLE_SCN_KEY, 2L);
      } else {
        oracleChangeEvent.put(DatastreamConstants.ORACLE_SCN_KEY, JSONObject.NULL);
      }
    }
    ObjectMapper mapper = new ObjectMapper();
    mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    JsonNode jsonNode = mapper.readTree(oracleChangeEvent.toString());

    // Prepare mock ChangeEventContext.
    ChangeEventContext mockContext = mock(ChangeEventContext.class);
    when(mockContext.getChangeEvent()).thenReturn(jsonNode);
    when(mockContext.getPrimaryKey()).thenReturn(Key.of("test"));
    when(mockContext.getShadowTable()).thenReturn("test");

    return mockContext;
  }

  ChangeEventContext getMockPostgresChangeEventContext(
      boolean addPostgresPositionFields, boolean cdcEvent) throws Exception {
    // Create dummy postgres change event.
    JSONObject postgresChangeEvent = new JSONObject();
    postgresChangeEvent.put(
        DatastreamConstants.EVENT_SOURCE_TYPE_KEY, DatastreamConstants.POSTGRES_SOURCE_TYPE);
    postgresChangeEvent.put(
        DatastreamConstants.EVENT_CHANGE_TYPE_KEY, DatastreamConstants.INSERT_EVENT);
    postgresChangeEvent.put(DatastreamConstants.POSTGRES_TIMESTAMP_KEY, eventTimestamp);
    if (addPostgresPositionFields) {
      if (cdcEvent) {
        postgresChangeEvent.put(DatastreamConstants.POSTGRES_LSN_KEY, "13/314");
      } else {
        postgresChangeEvent.put(DatastreamConstants.POSTGRES_LSN_KEY, JSONObject.NULL);
      }
    }
    ObjectMapper mapper = new ObjectMapper();
    mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    JsonNode jsonNode = mapper.readTree(postgresChangeEvent.toString());

    // Prepare mock ChangeEventContext.
    ChangeEventContext mockContext = mock(ChangeEventContext.class);
    when(mockContext.getChangeEvent()).thenReturn(jsonNode);
    when(mockContext.getPrimaryKey()).thenReturn(Key.of("test"));
    when(mockContext.getShadowTable()).thenReturn("test");

    return mockContext;
  }

  ChangeEventContext getMockSqlServerChangeEventContext(
      boolean addSqlServerPositionFields, boolean cdcEvent) throws Exception {
    // Create dummy sqlserver change event.
    JSONObject sqlServerChangeEvent = new JSONObject();
    sqlServerChangeEvent.put(
        DatastreamConstants.EVENT_SOURCE_TYPE_KEY, DatastreamConstants.SQLSERVER_SOURCE_TYPE);
    sqlServerChangeEvent.put(
        DatastreamConstants.EVENT_CHANGE_TYPE_KEY, DatastreamConstants.INSERT_EVENT);
    sqlServerChangeEvent.put(DatastreamConstants.SQLSERVER_TIMESTAMP_KEY, eventTimestamp);
    if (addSqlServerPositionFields) {
      if (cdcEvent) {
        sqlServerChangeEvent.put(DatastreamConstants.SQLSERVER_LSN_KEY, "00000016:00000123:0001");
      } else {
        sqlServerChangeEvent.put(DatastreamConstants.SQLSERVER_LSN_KEY, JSONObject.NULL);
      }
    }
    ObjectMapper mapper = new ObjectMapper();
    mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    JsonNode jsonNode = mapper.readTree(sqlServerChangeEvent.toString());

    // Prepare mock ChangeEventContext.
    ChangeEventContext mockContext = mock(ChangeEventContext.class);
    when(mockContext.getChangeEvent()).thenReturn(jsonNode);
    when(mockContext.getPrimaryKey()).thenReturn(Key.of("test"));
    when(mockContext.getShadowTable()).thenReturn("test");

    return mockContext;
  }

  @Test
  public void canCreateMySqlChangeEventSequenceFromChangeEvent() throws Exception {

    ChangeEventContext mockContext =
        getMockMySqlChangeEventContext(/* addMysqlPositionFields= */ true, /* cdcEvent= */ true);

    ChangeEventSequence changeEventSequence =
        ChangeEventSequenceFactory.createChangeEventSequenceFromChangeEventContext(mockContext);

    assertThat(changeEventSequence, instanceOf(MySqlChangeEventSequence.class));
    MySqlChangeEventSequence mysqlChangeEventSequence =
        (MySqlChangeEventSequence) changeEventSequence;
    assertEquals(mysqlChangeEventSequence.getTimestamp(), new Long(eventTimestamp));
    assertEquals(mysqlChangeEventSequence.getLogFile(), "file1.log");
    assertEquals(mysqlChangeEventSequence.getLogPosition(), new Long(2));
  }

  @Test
  public void canCreateMySqlChangeEventSequenceFromBackfillEvent() throws Exception {

    ChangeEventContext mockContext =
        getMockMySqlChangeEventContext(/* addMysqlPositionFields= */ true, /* cdcEvent= */ false);

    ChangeEventSequence changeEventSequence =
        ChangeEventSequenceFactory.createChangeEventSequenceFromChangeEventContext(mockContext);

    assertThat(changeEventSequence, instanceOf(MySqlChangeEventSequence.class));
    MySqlChangeEventSequence mysqlChangeEventSequence =
        (MySqlChangeEventSequence) changeEventSequence;
    assertEquals(mysqlChangeEventSequence.getTimestamp(), new Long(eventTimestamp));
    assertEquals(mysqlChangeEventSequence.getLogFile(), "");
    assertEquals(mysqlChangeEventSequence.getLogPosition(), new Long(-1));
  }

  @Test
  public void canCreateMySqlChangeEventSequenceFromBackfillEventWithNoPositionFields()
      throws Exception {

    ChangeEventContext mockContext =
        getMockMySqlChangeEventContext(/* addMysqlPositionFields= */ false, /* cdcEvent= */ false);

    ChangeEventSequence changeEventSequence =
        ChangeEventSequenceFactory.createChangeEventSequenceFromChangeEventContext(mockContext);

    assertThat(changeEventSequence, instanceOf(MySqlChangeEventSequence.class));
    MySqlChangeEventSequence mysqlChangeEventSequence =
        (MySqlChangeEventSequence) changeEventSequence;
    assertEquals(mysqlChangeEventSequence.getTimestamp(), new Long(eventTimestamp));
    assertEquals(mysqlChangeEventSequence.getLogFile(), "");
    assertEquals(mysqlChangeEventSequence.getLogPosition(), new Long(-1));
  }

  @Test
  public void canCreateMySqlChangeEventSequenceFromShadowTable() throws Exception {

    long previousEventTimestamp = 1615159727L;

    ChangeEventContext mockContext =
        getMockMySqlChangeEventContext(/* addMysqlPositionFields= */ true, /* cdcEvent= */ true);
    when(mockContext.getSafeShadowColumn(DatastreamConstants.MYSQL_TIMESTAMP_KEY))
        .thenReturn(DatastreamConstants.MYSQL_TIMESTAMP_SHADOW_INFO.getLeft());
    when(mockContext.getSafeShadowColumn(DatastreamConstants.MYSQL_LOGFILE_KEY))
        .thenReturn(DatastreamConstants.MYSQL_LOGFILE_SHADOW_INFO.getLeft());
    when(mockContext.getSafeShadowColumn(DatastreamConstants.MYSQL_LOGPOSITION_KEY))
        .thenReturn(DatastreamConstants.MYSQL_LOGPOSITION_SHADOW_INFO.getLeft());

    // Mock transaction which can read a row from shadow table.
    TransactionContext mockTransaction = mock(TransactionContext.class);
    Struct mockRow = mock(Struct.class);
    when(mockRow.getLong(DatastreamConstants.MYSQL_TIMESTAMP_SHADOW_INFO.getLeft()))
        .thenReturn(previousEventTimestamp);
    when(mockRow.getLong(DatastreamConstants.MYSQL_LOGPOSITION_SHADOW_INFO.getLeft()))
        .thenReturn(1L);
    when(mockRow.getString(DatastreamConstants.MYSQL_LOGFILE_SHADOW_INFO.getLeft()))
        .thenReturn("oldlogfile.log");
    when(mockTransaction.readRow(any(String.class), any(Key.class), any(Iterable.class)))
        .thenReturn(mockRow);

    ChangeEventSequence changeEventSequence =
        ChangeEventSequenceFactory.createChangeEventSequenceFromShadowTable(
            mockTransaction, mockContext, null, false);

    assertThat(changeEventSequence, instanceOf(MySqlChangeEventSequence.class));
    MySqlChangeEventSequence mysqlChangeEventSequence =
        (MySqlChangeEventSequence) changeEventSequence;
    assertEquals(mysqlChangeEventSequence.getTimestamp(), new Long(previousEventTimestamp));
    assertEquals(mysqlChangeEventSequence.getLogFile(), "oldlogfile.log");
    assertEquals(mysqlChangeEventSequence.getLogPosition(), new Long(1));
  }

  @Test
  public void canCreateMySqlChangeEventSequenceFromShadowTableForDumpEvent() throws Exception {

    long previousEventTimestamp = 1615159727L;

    ChangeEventContext mockContext =
        getMockMySqlChangeEventContext(/* addMysqlPositionFields= */ true, /* cdcEvent= */ true);
    when(mockContext.getSafeShadowColumn(DatastreamConstants.MYSQL_TIMESTAMP_KEY))
        .thenReturn(DatastreamConstants.MYSQL_TIMESTAMP_SHADOW_INFO.getLeft());
    when(mockContext.getSafeShadowColumn(DatastreamConstants.MYSQL_LOGFILE_KEY))
        .thenReturn(DatastreamConstants.MYSQL_LOGFILE_SHADOW_INFO.getLeft());
    when(mockContext.getSafeShadowColumn(DatastreamConstants.MYSQL_LOGPOSITION_KEY))
        .thenReturn(DatastreamConstants.MYSQL_LOGPOSITION_SHADOW_INFO.getLeft());

    // Mock transaction which can read a row from shadow table.
    TransactionContext mockTransaction = mock(TransactionContext.class);
    Struct mockRow = mock(Struct.class);
    when(mockRow.getLong(DatastreamConstants.MYSQL_TIMESTAMP_SHADOW_INFO.getLeft()))
        .thenReturn(previousEventTimestamp);
    when(mockRow.getLong(DatastreamConstants.MYSQL_LOGPOSITION_SHADOW_INFO.getLeft()))
        .thenReturn(-1L);
    when(mockRow.getString(DatastreamConstants.MYSQL_LOGFILE_SHADOW_INFO.getLeft())).thenReturn("");
    when(mockTransaction.readRow(any(String.class), any(Key.class), any(Iterable.class)))
        .thenReturn(mockRow);

    ChangeEventSequence changeEventSequence =
        ChangeEventSequenceFactory.createChangeEventSequenceFromShadowTable(
            mockTransaction, mockContext, null, false);

    assertThat(changeEventSequence, instanceOf(MySqlChangeEventSequence.class));
    MySqlChangeEventSequence mysqlChangeEventSequence =
        (MySqlChangeEventSequence) changeEventSequence;
    assertEquals(mysqlChangeEventSequence.getTimestamp(), new Long(previousEventTimestamp));
    assertEquals(mysqlChangeEventSequence.getLogFile(), "");
    assertEquals(mysqlChangeEventSequence.getLogPosition(), new Long(-1));
  }

  @Test
  public void cannotCreateMySqlChangeEventSequenceWhenMissingRecordInShadowTable()
      throws Exception {

    ChangeEventContext mockContext =
        getMockMySqlChangeEventContext(/* addMysqlPositionFields= */ true, /* cdcEvent= */ true);
    when(mockContext.getSafeShadowColumn(DatastreamConstants.MYSQL_TIMESTAMP_KEY))
        .thenReturn(DatastreamConstants.MYSQL_TIMESTAMP_SHADOW_INFO.getLeft());
    when(mockContext.getSafeShadowColumn(DatastreamConstants.MYSQL_LOGFILE_KEY))
        .thenReturn(DatastreamConstants.MYSQL_LOGFILE_SHADOW_INFO.getLeft());
    when(mockContext.getSafeShadowColumn(DatastreamConstants.MYSQL_LOGPOSITION_KEY))
        .thenReturn(DatastreamConstants.MYSQL_LOGPOSITION_SHADOW_INFO.getLeft());

    // mock transaction which cannot find a row from shadow table.
    TransactionContext mockTransaction = mock(TransactionContext.class);
    when(mockTransaction.readRow(any(String.class), any(Key.class), any(Iterable.class)))
        .thenReturn(null);

    ChangeEventSequence mysqlChangeEventSequence =
        ChangeEventSequenceFactory.createChangeEventSequenceFromShadowTable(
            mockTransaction, mockContext, null, false);

    assertNull(mysqlChangeEventSequence);
  }

  @Test
  public void canCreateOracleChangeEventSequenceFromChangeEvent() throws Exception {

    ChangeEventContext mockContext =
        getMockOracleChangeEventContext(/* addOraclePositionFields= */ true, /* cdcEvent= */ true);

    ChangeEventSequence changeEventSequence =
        ChangeEventSequenceFactory.createChangeEventSequenceFromChangeEventContext(mockContext);

    assertThat(changeEventSequence, instanceOf(OracleChangeEventSequence.class));
    OracleChangeEventSequence oracleChangeEventSequence =
        (OracleChangeEventSequence) changeEventSequence;
    assertEquals(oracleChangeEventSequence.getTimestamp(), new Long(eventTimestamp));
    assertEquals(oracleChangeEventSequence.getSCN(), new Long(2));
  }

  @Test
  public void canCreateOracleChangeEventSequenceFromBackfillEvent() throws Exception {

    ChangeEventContext mockContext =
        getMockOracleChangeEventContext(/* addOraclePositionFields= */ true, /* cdcEvent= */ false);

    ChangeEventSequence changeEventSequence =
        ChangeEventSequenceFactory.createChangeEventSequenceFromChangeEventContext(mockContext);

    assertThat(changeEventSequence, instanceOf(OracleChangeEventSequence.class));
    OracleChangeEventSequence oracleChangeEventSequence =
        (OracleChangeEventSequence) changeEventSequence;
    assertEquals(oracleChangeEventSequence.getTimestamp(), new Long(eventTimestamp));
    assertEquals(oracleChangeEventSequence.getSCN(), new Long(-1));
  }

  @Test
  public void canCreateOracleChangeEventSequenceFromBackfillEventWithNoPositionFields()
      throws Exception {

    ChangeEventContext mockContext =
        getMockOracleChangeEventContext(
            /* addOraclePositionFields= */ false, /* cdcEvent= */ false);

    ChangeEventSequence changeEventSequence =
        ChangeEventSequenceFactory.createChangeEventSequenceFromChangeEventContext(mockContext);

    assertThat(changeEventSequence, instanceOf(OracleChangeEventSequence.class));
    OracleChangeEventSequence oracleChangeEventSequence =
        (OracleChangeEventSequence) changeEventSequence;
    assertEquals(oracleChangeEventSequence.getTimestamp(), new Long(eventTimestamp));
    assertEquals(oracleChangeEventSequence.getSCN(), new Long(-1));
  }

  @Test
  public void canCreateOracleChangeEventSequenceFromShadowTable() throws Exception {

    long previousEventTimestamp = 1615159727L;

    ChangeEventContext mockContext =
        getMockOracleChangeEventContext(/* addOraclePositionFields= */ true, /* cdcEvent= */ true);
    when(mockContext.getSafeShadowColumn(DatastreamConstants.ORACLE_TIMESTAMP_KEY))
        .thenReturn(DatastreamConstants.ORACLE_TIMESTAMP_SHADOW_INFO.getLeft());
    when(mockContext.getSafeShadowColumn(DatastreamConstants.ORACLE_SCN_KEY))
        .thenReturn(DatastreamConstants.ORACLE_SCN_SHADOW_INFO.getLeft());

    // Mock transaction which can read a row from shadow table.
    TransactionContext mockTransaction = mock(TransactionContext.class);
    Struct mockRow = mock(Struct.class);
    when(mockRow.getLong(DatastreamConstants.ORACLE_TIMESTAMP_SHADOW_INFO.getLeft()))
        .thenReturn(previousEventTimestamp);
    when(mockRow.getLong(DatastreamConstants.ORACLE_SCN_SHADOW_INFO.getLeft())).thenReturn(1L);
    when(mockTransaction.readRow(any(String.class), any(Key.class), any(Iterable.class)))
        .thenReturn(mockRow);

    ChangeEventSequence changeEventSequence =
        ChangeEventSequenceFactory.createChangeEventSequenceFromShadowTable(
            mockTransaction, mockContext, null, false);

    assertThat(changeEventSequence, instanceOf(OracleChangeEventSequence.class));
    OracleChangeEventSequence oracleChangeEventSequence =
        (OracleChangeEventSequence) changeEventSequence;
    assertEquals(oracleChangeEventSequence.getTimestamp(), new Long(previousEventTimestamp));
    assertEquals(oracleChangeEventSequence.getSCN(), new Long(1));
  }

  @Test
  public void canCreateOracleChangeEventSequenceFromShadowTableForDumpEvent() throws Exception {

    long previousEventTimestamp = 1615159727L;

    ChangeEventContext mockContext =
        getMockOracleChangeEventContext(/* addOraclePositionFields= */ true, /* cdcEvent= */ true);
    when(mockContext.getSafeShadowColumn(DatastreamConstants.ORACLE_TIMESTAMP_KEY))
        .thenReturn(DatastreamConstants.ORACLE_TIMESTAMP_SHADOW_INFO.getLeft());
    when(mockContext.getSafeShadowColumn(DatastreamConstants.ORACLE_SCN_KEY))
        .thenReturn(DatastreamConstants.ORACLE_SCN_SHADOW_INFO.getLeft());

    // Mock transaction which can read a row from shadow table.
    TransactionContext mockTransaction = mock(TransactionContext.class);
    Struct mockRow = mock(Struct.class);
    when(mockRow.getLong(DatastreamConstants.ORACLE_TIMESTAMP_SHADOW_INFO.getLeft()))
        .thenReturn(previousEventTimestamp);
    when(mockRow.getLong(DatastreamConstants.ORACLE_SCN_SHADOW_INFO.getLeft())).thenReturn(-1L);
    when(mockTransaction.readRow(any(String.class), any(Key.class), any(Iterable.class)))
        .thenReturn(mockRow);

    ChangeEventSequence changeEventSequence =
        ChangeEventSequenceFactory.createChangeEventSequenceFromShadowTable(
            mockTransaction, mockContext, null, false);

    assertThat(changeEventSequence, instanceOf(OracleChangeEventSequence.class));
    OracleChangeEventSequence oracleChangeEventSequence =
        (OracleChangeEventSequence) changeEventSequence;
    assertEquals(oracleChangeEventSequence.getTimestamp(), new Long(previousEventTimestamp));
    assertEquals(oracleChangeEventSequence.getSCN(), new Long(-1));
  }

  @Test
  public void cannotCreateOracleChangeEventSequenceWhenMissingRecordInShadowTable()
      throws Exception {

    ChangeEventContext mockContext =
        getMockOracleChangeEventContext(/* addOraclePositionFields= */ true, /* cdcEvent= */ true);
    when(mockContext.getSafeShadowColumn(DatastreamConstants.ORACLE_TIMESTAMP_KEY))
        .thenReturn(DatastreamConstants.ORACLE_TIMESTAMP_SHADOW_INFO.getLeft());
    when(mockContext.getSafeShadowColumn(DatastreamConstants.ORACLE_SCN_KEY))
        .thenReturn(DatastreamConstants.ORACLE_SCN_SHADOW_INFO.getLeft());

    // mock transaction which cannot find a row from shadow table.
    TransactionContext mockTransaction = mock(TransactionContext.class);
    when(mockTransaction.readRow(any(String.class), any(Key.class), any(Iterable.class)))
        .thenReturn(null);

    ChangeEventSequence oracleChangeEventSequence =
        ChangeEventSequenceFactory.createChangeEventSequenceFromShadowTable(
            mockTransaction, mockContext, null, false);

    assertNull(oracleChangeEventSequence);
  }

  @Test
  public void canCreatePostgresChangeEventSequenceFromChangeEvent() throws Exception {

    ChangeEventContext mockContext =
        getMockPostgresChangeEventContext(
            /* addPostgresPositionFields= */ true, /* cdcEvent= */ true);

    ChangeEventSequence changeEventSequence =
        ChangeEventSequenceFactory.createChangeEventSequenceFromChangeEventContext(mockContext);

    assertThat(changeEventSequence, instanceOf(PostgresChangeEventSequence.class));
    PostgresChangeEventSequence postgresChangeEventSequence =
        (PostgresChangeEventSequence) changeEventSequence;
    assertEquals(postgresChangeEventSequence.getTimestamp(), new Long(eventTimestamp));
    assertEquals(postgresChangeEventSequence.getLSN(), "13/314");
  }

  @Test
  public void canCreatePostgresChangeEventSequenceFromBackfillEvent() throws Exception {

    ChangeEventContext mockContext =
        getMockPostgresChangeEventContext(
            /* addPostgresPositionFields= */ true, /* cdcEvent= */ false);

    ChangeEventSequence changeEventSequence =
        ChangeEventSequenceFactory.createChangeEventSequenceFromChangeEventContext(mockContext);

    assertThat(changeEventSequence, instanceOf(PostgresChangeEventSequence.class));
    PostgresChangeEventSequence postgresChangeEventSequence =
        (PostgresChangeEventSequence) changeEventSequence;
    assertEquals(postgresChangeEventSequence.getTimestamp(), new Long(eventTimestamp));
    assertEquals(postgresChangeEventSequence.getLSN(), "");
  }

  @Test
  public void canCreatePostgresChangeEventSequenceFromBackfillEventWithNoPositionFields()
      throws Exception {

    ChangeEventContext mockContext =
        getMockPostgresChangeEventContext(
            /* addPostgresPositionFields= */ false, /* cdcEvent= */ false);

    ChangeEventSequence changeEventSequence =
        ChangeEventSequenceFactory.createChangeEventSequenceFromChangeEventContext(mockContext);

    assertThat(changeEventSequence, instanceOf(PostgresChangeEventSequence.class));
    PostgresChangeEventSequence postgresChangeEventSequence =
        (PostgresChangeEventSequence) changeEventSequence;
    assertEquals(postgresChangeEventSequence.getTimestamp(), new Long(eventTimestamp));
    assertEquals(postgresChangeEventSequence.getLSN(), "");
  }

  @Test
  public void canCreatePostgresChangeEventSequenceFromShadowTable() throws Exception {

    long previousEventTimestamp = 1615159727L;

    ChangeEventContext mockContext =
        getMockPostgresChangeEventContext(
            /* addPostgresPositionFields= */ true, /* cdcEvent= */ true);
    when(mockContext.getSafeShadowColumn(DatastreamConstants.POSTGRES_TIMESTAMP_KEY))
        .thenReturn(DatastreamConstants.POSTGRES_TIMESTAMP_SHADOW_INFO.getLeft());
    when(mockContext.getSafeShadowColumn(DatastreamConstants.POSTGRES_LSN_KEY))
        .thenReturn(DatastreamConstants.POSTGRES_LSN_SHADOW_INFO.getLeft());

    // Mock transaction which can read a row from shadow table.
    TransactionContext mockTransaction = mock(TransactionContext.class);
    Struct mockRow = mock(Struct.class);
    when(mockRow.getLong(DatastreamConstants.POSTGRES_TIMESTAMP_SHADOW_INFO.getLeft()))
        .thenReturn(previousEventTimestamp);
    when(mockRow.getString(DatastreamConstants.POSTGRES_LSN_SHADOW_INFO.getLeft()))
        .thenReturn("13/314");
    when(mockTransaction.readRow(any(String.class), any(Key.class), any(Iterable.class)))
        .thenReturn(mockRow);

    ChangeEventSequence changeEventSequence =
        ChangeEventSequenceFactory.createChangeEventSequenceFromShadowTable(
            mockTransaction, mockContext, null, false);

    assertThat(changeEventSequence, instanceOf(PostgresChangeEventSequence.class));
    PostgresChangeEventSequence postgresChangeEventSequence =
        (PostgresChangeEventSequence) changeEventSequence;
    assertEquals(postgresChangeEventSequence.getTimestamp(), new Long(previousEventTimestamp));
    assertEquals(postgresChangeEventSequence.getLSN(), "13/314");
  }

  @Test
  public void canCreatePostgresChangeEventSequenceFromShadowTableForDumpEvent() throws Exception {

    long previousEventTimestamp = 1615159727L;

    ChangeEventContext mockContext =
        getMockPostgresChangeEventContext(
            /* addPostgresPositionFields= */ true, /* cdcEvent= */ true);
    when(mockContext.getSafeShadowColumn(DatastreamConstants.POSTGRES_TIMESTAMP_KEY))
        .thenReturn(DatastreamConstants.POSTGRES_TIMESTAMP_SHADOW_INFO.getLeft());
    when(mockContext.getSafeShadowColumn(DatastreamConstants.POSTGRES_LSN_KEY))
        .thenReturn(DatastreamConstants.POSTGRES_LSN_SHADOW_INFO.getLeft());

    // Mock transaction which can read a row from shadow table.
    TransactionContext mockTransaction = mock(TransactionContext.class);
    Struct mockRow = mock(Struct.class);
    when(mockRow.getLong(DatastreamConstants.POSTGRES_TIMESTAMP_SHADOW_INFO.getLeft()))
        .thenReturn(previousEventTimestamp);
    when(mockRow.getString(DatastreamConstants.POSTGRES_LSN_SHADOW_INFO.getLeft())).thenReturn("");
    when(mockTransaction.readRow(any(String.class), any(Key.class), any(Iterable.class)))
        .thenReturn(mockRow);

    ChangeEventSequence changeEventSequence =
        ChangeEventSequenceFactory.createChangeEventSequenceFromShadowTable(
            mockTransaction, mockContext, null, false);

    assertThat(changeEventSequence, instanceOf(PostgresChangeEventSequence.class));
    PostgresChangeEventSequence postgresChangeEventSequence =
        (PostgresChangeEventSequence) changeEventSequence;
    assertEquals(postgresChangeEventSequence.getTimestamp(), new Long(previousEventTimestamp));
    assertEquals(postgresChangeEventSequence.getLSN(), "");
  }

  @Test
  public void cannotCreatePostgresChangeEventSequenceWhenMissingRecordInShadowTable()
      throws Exception {

    ChangeEventContext mockContext =
        getMockPostgresChangeEventContext(
            /* addPostgresPositionFields= */ true, /* cdcEvent= */ true);
    when(mockContext.getSafeShadowColumn(DatastreamConstants.POSTGRES_TIMESTAMP_KEY))
        .thenReturn(DatastreamConstants.POSTGRES_TIMESTAMP_SHADOW_INFO.getLeft());
    when(mockContext.getSafeShadowColumn(DatastreamConstants.POSTGRES_LSN_KEY))
        .thenReturn(DatastreamConstants.POSTGRES_LSN_SHADOW_INFO.getLeft());

    // mock transaction which cannot find a row from shadow table.
    TransactionContext mockTransaction = mock(TransactionContext.class);
    when(mockTransaction.readRow(any(String.class), any(Key.class), any(Iterable.class)))
        .thenReturn(null);

    ChangeEventSequence postgresChangeEventSequence =
        ChangeEventSequenceFactory.createChangeEventSequenceFromShadowTable(
            mockTransaction, mockContext, null, false);

    assertNull(postgresChangeEventSequence);
  }

  @Test
  public void canCreateSqlServerChangeEventSequenceFromChangeEvent() throws Exception {

    ChangeEventContext mockContext =
        getMockSqlServerChangeEventContext(
            /* addSqlServerPositionFields= */ true, /* cdcEvent= */ true);

    ChangeEventSequence changeEventSequence =
        ChangeEventSequenceFactory.createChangeEventSequenceFromChangeEventContext(mockContext);

    assertThat(changeEventSequence, instanceOf(SqlServerChangeEventSequence.class));
    SqlServerChangeEventSequence sqlServerChangeEventSequence =
        (SqlServerChangeEventSequence) changeEventSequence;
    assertEquals(sqlServerChangeEventSequence.getTimestamp(), new Long(eventTimestamp));
    assertEquals(sqlServerChangeEventSequence.getLsn(), "00000016:00000123:0001");
  }

  @Test
  public void canCreateSqlServerChangeEventSequenceFromBackfillEvent() throws Exception {

    ChangeEventContext mockContext =
        getMockSqlServerChangeEventContext(
            /* addSqlServerPositionFields= */ true, /* cdcEvent= */ false);

    ChangeEventSequence changeEventSequence =
        ChangeEventSequenceFactory.createChangeEventSequenceFromChangeEventContext(mockContext);

    assertThat(changeEventSequence, instanceOf(SqlServerChangeEventSequence.class));
    SqlServerChangeEventSequence sqlServerChangeEventSequence =
        (SqlServerChangeEventSequence) changeEventSequence;
    assertEquals(sqlServerChangeEventSequence.getTimestamp(), new Long(eventTimestamp));
    assertEquals(sqlServerChangeEventSequence.getLsn(), "");
  }

  @Test
  public void canCreateSqlServerChangeEventSequenceFromShadowTable() throws Exception {

    long previousEventTimestamp = 1615159727L;

    ChangeEventContext mockContext =
        getMockSqlServerChangeEventContext(
            /* addSqlServerPositionFields= */ true, /* cdcEvent= */ true);
    when(mockContext.getSafeShadowColumn(DatastreamConstants.SQLSERVER_TIMESTAMP_KEY))
        .thenReturn(DatastreamConstants.SQLSERVER_TIMESTAMP_KEY);
    when(mockContext.getSafeShadowColumn(DatastreamConstants.SQLSERVER_LSN_KEY))
        .thenReturn(DatastreamConstants.SQLSERVER_LSN_KEY);

    // Mock transaction which can read a row from shadow table.
    TransactionContext mockTransaction = mock(TransactionContext.class);
    Struct mockRow = mock(Struct.class);
    when(mockRow.getLong(DatastreamConstants.SQLSERVER_TIMESTAMP_KEY))
        .thenReturn(previousEventTimestamp);
    when(mockRow.getString(DatastreamConstants.SQLSERVER_LSN_KEY))
        .thenReturn("00000016:00000123:0001");
    when(mockTransaction.readRow(any(String.class), any(Key.class), any(Iterable.class)))
        .thenReturn(mockRow);

    ChangeEventSequence changeEventSequence =
        ChangeEventSequenceFactory.createChangeEventSequenceFromShadowTable(
            mockTransaction, mockContext, null, false);

    assertThat(changeEventSequence, instanceOf(SqlServerChangeEventSequence.class));
    SqlServerChangeEventSequence sqlServerChangeEventSequence =
        (SqlServerChangeEventSequence) changeEventSequence;
    assertEquals(sqlServerChangeEventSequence.getTimestamp(), new Long(previousEventTimestamp));
    assertEquals(sqlServerChangeEventSequence.getLsn(), "00000016:00000123:0001");
  }
}
