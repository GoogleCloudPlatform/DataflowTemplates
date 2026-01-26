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
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.TransactionContext;
import com.google.cloud.spanner.Value;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import java.io.IOException;
import java.util.Map;
import org.json.JSONObject;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

/**
 * Unit tests 1) ChangeEventContextFactory methods for mysql change events. 2) shadow table
 * generation for mysql change events.
 */
public final class MySqlChangeEventContextTest {

  private JsonNode getJsonNode(String json) throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    return mapper.readTree(json);
  }

  @Test
  public void canGenerateShadowTableMutation() throws Exception {

    long eventTimestamp = 1615159728L;

    // Test Ddl
    Ddl ddl = ChangeEventConvertorTest.getTestDdl();

    // Test Change Event
    JSONObject changeEvent = ChangeEventConvertorTest.getTestChangeEvent("Users2");
    changeEvent.put(DatastreamConstants.MYSQL_TIMESTAMP_KEY, eventTimestamp);
    changeEvent.put(DatastreamConstants.MYSQL_LOGFILE_KEY, "file1.log");
    changeEvent.put(DatastreamConstants.MYSQL_LOGPOSITION_KEY, 1L);
    changeEvent.put(
        DatastreamConstants.EVENT_SOURCE_TYPE_KEY, DatastreamConstants.MYSQL_SOURCE_TYPE);

    ChangeEventContext changeEventContext =
        ChangeEventContextFactory.createChangeEventContext(
            getJsonNode(changeEvent.toString()),
            ddl,
            ddl,
            "shadow_",
            DatastreamConstants.MYSQL_SOURCE_TYPE);
    Mutation shadowMutation = changeEventContext.getShadowTableMutation();
    Map<String, Value> actual = shadowMutation.asMap();

    // Expected result
    Map<String, Value> expected =
        ChangeEventConvertorTest.getExpectedMapForTestChangeEventWithoutJsonField();
    expected.put(
        DatastreamConstants.MYSQL_TIMESTAMP_SHADOW_INFO.getLeft(), Value.int64(eventTimestamp));
    expected.put(
        DatastreamConstants.MYSQL_LOGFILE_SHADOW_INFO.getLeft(), Value.string("file1.log"));
    expected.put(DatastreamConstants.MYSQL_LOGPOSITION_SHADOW_INFO.getLeft(), Value.int64(1));

    // Verify if MySqlChangeEventContext was actually created.
    assertThat(changeEventContext, instanceOf(MySqlChangeEventContext.class));
    // Verify shadow mutation
    assertThat(actual, is(expected));
    assertEquals(shadowMutation.getTable(), "shadow_Users2");
    assertEquals(shadowMutation.getOperation(), Mutation.Op.INSERT_OR_UPDATE);
  }

  @Test
  public void canGenerateShadowTableMutationForBackfillEvents() throws Exception {

    long eventTimestamp = 1615159728L;

    // Test Ddl
    Ddl ddl = ChangeEventConvertorTest.getTestDdl();

    // Test Change Event
    JSONObject changeEvent = ChangeEventConvertorTest.getTestChangeEvent("Users2");
    changeEvent.put(DatastreamConstants.MYSQL_TIMESTAMP_KEY, eventTimestamp);
    changeEvent.put(DatastreamConstants.MYSQL_LOGFILE_KEY, JSONObject.NULL);
    changeEvent.put(DatastreamConstants.MYSQL_LOGPOSITION_KEY, JSONObject.NULL);
    changeEvent.put(
        DatastreamConstants.EVENT_SOURCE_TYPE_KEY, DatastreamConstants.MYSQL_SOURCE_TYPE);

    ChangeEventContext changeEventContext =
        ChangeEventContextFactory.createChangeEventContext(
            getJsonNode(changeEvent.toString()),
            ddl,
            ddl,
            "shadow_",
            DatastreamConstants.MYSQL_SOURCE_TYPE);
    Mutation shadowMutation = changeEventContext.getShadowTableMutation();
    Map<String, Value> actual = shadowMutation.asMap();

    // Expected result
    Map<String, Value> expected =
        ChangeEventConvertorTest.getExpectedMapForTestChangeEventWithoutJsonField();
    expected.put(
        DatastreamConstants.MYSQL_TIMESTAMP_SHADOW_INFO.getLeft(), Value.int64(eventTimestamp));
    expected.put(DatastreamConstants.MYSQL_LOGFILE_SHADOW_INFO.getLeft(), Value.string(""));
    expected.put(DatastreamConstants.MYSQL_LOGPOSITION_SHADOW_INFO.getLeft(), Value.int64(-1));

    // Verify if MySqlChangeEventContext was actually created.
    assertThat(changeEventContext, instanceOf(MySqlChangeEventContext.class));
    // Verify shadow mutation
    assertThat(actual, is(expected));
    assertEquals(shadowMutation.getTable(), "shadow_Users2");
    assertEquals(shadowMutation.getOperation(), Mutation.Op.INSERT_OR_UPDATE);
  }

  @Test
  public void canGenerateShadowTableMutationForBackfillEventsWithMissingSortOrderKeys()
      throws Exception {

    long eventTimestamp = 1615159728L;

    // Test Ddl
    Ddl ddl = ChangeEventConvertorTest.getTestDdl();

    // Test Change Event which does not contain sort order fields like log file and log position.
    JSONObject changeEvent = ChangeEventConvertorTest.getTestChangeEvent("Users2");
    changeEvent.put(DatastreamConstants.MYSQL_TIMESTAMP_KEY, eventTimestamp);
    changeEvent.put(
        DatastreamConstants.EVENT_SOURCE_TYPE_KEY, DatastreamConstants.MYSQL_SOURCE_TYPE);

    ChangeEventContext changeEventContext =
        ChangeEventContextFactory.createChangeEventContext(
            getJsonNode(changeEvent.toString()),
            ddl,
            ddl,
            "shadow_",
            DatastreamConstants.MYSQL_SOURCE_TYPE);
    Mutation shadowMutation = changeEventContext.getShadowTableMutation();
    Map<String, Value> actual = shadowMutation.asMap();

    // Expected result
    Map<String, Value> expected =
        ChangeEventConvertorTest.getExpectedMapForTestChangeEventWithoutJsonField();
    expected.put(
        DatastreamConstants.MYSQL_TIMESTAMP_SHADOW_INFO.getLeft(), Value.int64(eventTimestamp));
    expected.put(DatastreamConstants.MYSQL_LOGFILE_SHADOW_INFO.getLeft(), Value.string(""));
    expected.put(DatastreamConstants.MYSQL_LOGPOSITION_SHADOW_INFO.getLeft(), Value.int64(-1));

    // Verify if MySqlChangeEventContext was actually created.
    assertThat(changeEventContext, instanceOf(MySqlChangeEventContext.class));
    // Verify shadow mutation
    assertThat(actual, is(expected));
    assertEquals(shadowMutation.getTable(), "shadow_Users2");
    assertEquals(shadowMutation.getOperation(), Mutation.Op.INSERT_OR_UPDATE);
  }

  @Test
  public void canGenerateShadowTableMutationWithCollision() throws Exception {
    long eventTimestamp = 1615159728L;
    Ddl ddl =
        Ddl.builder()
            .createTable("MyTable")
            .column("log_file")
            .int64()
            .max()
            .endColumn()
            .column("data")
            .string()
            .max()
            .endColumn()
            .primaryKey()
            .asc("log_file")
            .end()
            .endTable()
            .build();
    Ddl shadowDdl =
        Ddl.builder()
            .createTable("shadow_MyTable")
            .column("log_file")
            .int64()
            .max()
            .endColumn()
            .column("data")
            .string()
            .max()
            .endColumn()
            .column("shadow_log_file")
            .string()
            .max()
            .endColumn()
            .primaryKey()
            .asc("log_file")
            .end()
            .endTable()
            .build();
    JSONObject changeEvent = new JSONObject();
    changeEvent.put("log_file", 3);
    changeEvent.put(DatastreamConstants.EVENT_TABLE_NAME_KEY, "MyTable");
    changeEvent.put(
        DatastreamConstants.EVENT_SOURCE_TYPE_KEY, DatastreamConstants.MYSQL_SOURCE_TYPE);
    changeEvent.put(DatastreamConstants.MYSQL_TIMESTAMP_KEY, eventTimestamp);
    changeEvent.put(DatastreamConstants.MYSQL_LOGFILE_KEY, "mysql-bin.00001");
    changeEvent.put(DatastreamConstants.MYSQL_LOGPOSITION_KEY, 100L);

    ChangeEventContext changeEventContext =
        ChangeEventContextFactory.createChangeEventContext(
            getJsonNode(changeEvent.toString()),
            ddl,
            shadowDdl,
            "shadow_",
            DatastreamConstants.MYSQL_SOURCE_TYPE);
    Mutation shadowMutation = changeEventContext.getShadowTableMutation();
    Map<String, Value> actual = shadowMutation.asMap();

    // The PK column 'log_file' should be present.
    assertEquals(actual.get("log_file"), Value.int64(3));
    // The conflicting shadow column should be renamed to 'shadow_log_file'.
    assertEquals(actual.get("shadow_log_file"), Value.string("mysql-bin.00001"));
    // The other shadow columns should be present with their default names.
    assertEquals(actual.get("timestamp"), Value.int64(eventTimestamp));
    assertEquals(actual.get("log_position"), Value.int64(100L));
  }

  @Test
  public void testReadDataTable() throws Exception {
    long eventTimestamp = 1615159728L;
    Ddl ddl = ChangeEventConvertorTest.getTestDdl();
    JSONObject changeEvent = ChangeEventConvertorTest.getTestChangeEvent("Users");
    changeEvent.put(DatastreamConstants.MYSQL_TIMESTAMP_KEY, eventTimestamp);
    changeEvent.put(
        DatastreamConstants.EVENT_SOURCE_TYPE_KEY, DatastreamConstants.MYSQL_SOURCE_TYPE);

    ChangeEventContext context =
        ChangeEventContextFactory.createChangeEventContext(
            getJsonNode(changeEvent.toString()),
            ddl,
            ddl,
            "shadow_",
            DatastreamConstants.MYSQL_SOURCE_TYPE);

    TransactionContext transactionContext = mock(TransactionContext.class);
    ResultSet resultSet = mock(ResultSet.class);

    // Test the case where the row is found.
    when(transactionContext.executeQuery(any(Statement.class))).thenReturn(resultSet);
    when(resultSet.next()).thenReturn(true);

    context.readDataTableRowWithExclusiveLock(transactionContext, ddl);

    ArgumentCaptor<Statement> statementCaptor = ArgumentCaptor.forClass(Statement.class);
    verify(transactionContext, times(1)).executeQuery(statementCaptor.capture());
    verify(resultSet, times(1)).next();
    verify(resultSet, times(1)).getCurrentRowAsStruct();

    // Verify the generated SQL
    String expectedSql =
        "@{LOCK_SCANNED_RANGES=exclusive} SELECT first_name, last_name FROM Users WHERE first_name=@first_name AND last_name=@last_name";
    Statement capturedStatement = statementCaptor.getValue();
    assertEquals(expectedSql, capturedStatement.getSql());
    assertEquals("A", capturedStatement.getParameters().get("first_name").getString());
    assertEquals("B", capturedStatement.getParameters().get("last_name").getString());

    // Test the case where the row is not found.
    Mockito.reset(transactionContext, resultSet);
    when(transactionContext.executeQuery(any(Statement.class))).thenReturn(resultSet);
    when(resultSet.next()).thenReturn(false);

    context.readDataTableRowWithExclusiveLock(transactionContext, ddl);
    verify(transactionContext, times(1)).executeQuery(any(Statement.class));
    verify(resultSet, times(1)).next();
    verify(resultSet, times(0)).getCurrentRowAsStruct();
  }

  @Test
  public void testCanGenerateDataDmlStatement() throws Exception {
    long eventTimestamp = 1615159728L;
    // DDL with generated PK
    Ddl ddl =
        Ddl.builder()
            .createTable("Users")
            .column("id")
            .string()
            .max()
            .generatedAs("uuid()")
            .endColumn()
            .column("first_name")
            .string()
            .max()
            .endColumn()
            .column("last_name")
            .string()
            .max()
            .endColumn()
            .primaryKey()
            .asc("id")
            .end()
            .endTable()
            .createTable("shadow_Users")
            .column("id")
            .string()
            .max()
            .endColumn()
            .column("first_name")
            .string()
            .max()
            .endColumn()
            .column("last_name")
            .string()
            .max()
            .endColumn()
            .primaryKey()
            .asc("id")
            .end()
            .endTable()
            .build();

    JSONObject changeEvent = new JSONObject();
    changeEvent.put(DatastreamConstants.EVENT_TABLE_NAME_KEY, "Users");
    changeEvent.put(DatastreamConstants.EVENT_CHANGE_TYPE_KEY, DatastreamConstants.DELETE_EVENT);
    changeEvent.put(
        DatastreamConstants.EVENT_SOURCE_TYPE_KEY, DatastreamConstants.MYSQL_SOURCE_TYPE);
    changeEvent.put(DatastreamConstants.MYSQL_TIMESTAMP_KEY, eventTimestamp);
    // Source keys (mapped to non-generated columns in Spanner)
    changeEvent.put("first_name", "John");
    changeEvent.put("last_name", "Doe");
    changeEvent.put("id", "aa");

    ChangeEventContext context =
        ChangeEventContextFactory.createChangeEventContext(
            getJsonNode(changeEvent.toString()),
            ddl,
            ddl,
            "shadow_",
            DatastreamConstants.MYSQL_SOURCE_TYPE);

    Statement dmlStatement = context.getDataDmlStatement(ddl);

    // Verify DML generation
    String expectedSql =
        "DELETE FROM Users WHERE first_name = @first_name AND last_name = @last_name";
    assertEquals(expectedSql, dmlStatement.getSql());
    assertEquals("John", dmlStatement.getParameters().get("first_name").getString());
    assertEquals("Doe", dmlStatement.getParameters().get("last_name").getString());
  }
}
