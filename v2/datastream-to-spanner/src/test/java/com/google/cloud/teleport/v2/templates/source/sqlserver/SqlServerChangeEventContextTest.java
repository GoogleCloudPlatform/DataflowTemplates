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
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Value;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.migrations.exceptions.ChangeEventConvertorException;
import com.google.cloud.teleport.v2.templates.datastream.ChangeEventContext;
import com.google.cloud.teleport.v2.templates.datastream.ChangeEventConvertorTest;
import com.google.cloud.teleport.v2.templates.datastream.DatastreamConstants;
import java.io.IOException;
import java.util.Map;
import org.json.JSONObject;
import org.junit.Test;

/**
 * Unit tests for {@link SqlServerChangeEventContext} shadow table mutation generation and dialect
 * handling.
 */
public final class SqlServerChangeEventContextTest {

  private static final String SQLSERVER_SOURCE_TYPE = "sqlserver";
  private final long eventTimestamp = 1615159728L;
  private final String eventLsn = "00000021:0000010f:0001";

  private JsonNode getJsonNode(String json) throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    return mapper.readTree(json);
  }

  @Test
  public void canGenerateShadowTableMutation() throws Exception {
    Ddl ddl = ChangeEventConvertorTest.getTestDdl();

    JSONObject changeEvent = ChangeEventConvertorTest.getTestChangeEvent("Users2");
    changeEvent.put(SqlServerDsToSpSourceConnector.SQLSERVER_TIMESTAMP_KEY, eventTimestamp);
    changeEvent.put(SqlServerDsToSpSourceConnector.SQLSERVER_CHANGE_LSN_KEY, eventLsn);
    changeEvent.put(DatastreamConstants.EVENT_SOURCE_TYPE_KEY, SQLSERVER_SOURCE_TYPE);

    ChangeEventContext changeEventContext =
        new SqlServerDsToSpSourceConnector()
            .createChangeEventContext(getJsonNode(changeEvent.toString()), ddl, ddl, "shadow_");
    Mutation shadowMutation = changeEventContext.getShadowTableMutation();
    Map<String, Value> actual = shadowMutation.asMap();

    Map<String, Value> expected =
        ChangeEventConvertorTest.getExpectedMapForTestChangeEventWithoutJsonField();
    expected.put(
        SqlServerDsToSpSourceConnector.SQLSERVER_TIMESTAMP_SHADOW_INFO.getLeft(),
        Value.int64(eventTimestamp));
    expected.put(
        SqlServerDsToSpSourceConnector.SQLSERVER_CHANGE_LSN_SHADOW_INFO.getLeft(),
        Value.string(eventLsn));

    assertThat(changeEventContext, instanceOf(SqlServerChangeEventContext.class));
    assertThat(actual, is(expected));
    assertEquals("shadow_Users2", shadowMutation.getTable());
    assertEquals(Mutation.Op.INSERT_OR_UPDATE, shadowMutation.getOperation());
    assertEquals("shadow_Users2", changeEventContext.getShadowTable());
  }

  @Test
  public void canGenerateShadowTableMutationDirectConstructor() throws Exception {
    Ddl ddl = ChangeEventConvertorTest.getTestDdl();

    JSONObject changeEvent = ChangeEventConvertorTest.getTestChangeEvent("Users2");
    changeEvent.put(SqlServerDsToSpSourceConnector.SQLSERVER_TIMESTAMP_KEY, eventTimestamp);
    changeEvent.put(SqlServerDsToSpSourceConnector.SQLSERVER_CHANGE_LSN_KEY, eventLsn);
    changeEvent.put(DatastreamConstants.EVENT_SOURCE_TYPE_KEY, SQLSERVER_SOURCE_TYPE);

    SqlServerChangeEventContext context =
        new SqlServerChangeEventContext(getJsonNode(changeEvent.toString()), ddl, ddl, "shadow_");

    Mutation shadowMutation = context.getShadowTableMutation();
    Map<String, Value> actual = shadowMutation.asMap();

    assertEquals(Value.int64(eventTimestamp), actual.get("timestamp"));
    assertEquals(Value.string(eventLsn), actual.get("change_lsn"));
    assertEquals("shadow_Users2", shadowMutation.getTable());
    assertEquals(Mutation.Op.INSERT_OR_UPDATE, shadowMutation.getOperation());
  }

  @Test
  public void canGenerateShadowTableMutationForBackfillEventsWithNullLsn() throws Exception {
    Ddl ddl = ChangeEventConvertorTest.getTestDdl();

    JSONObject changeEvent = ChangeEventConvertorTest.getTestChangeEvent("Users2");
    changeEvent.put(SqlServerDsToSpSourceConnector.SQLSERVER_TIMESTAMP_KEY, eventTimestamp);
    changeEvent.put(SqlServerDsToSpSourceConnector.SQLSERVER_CHANGE_LSN_KEY, JSONObject.NULL);
    changeEvent.put(DatastreamConstants.EVENT_SOURCE_TYPE_KEY, SQLSERVER_SOURCE_TYPE);

    ChangeEventContext changeEventContext =
        new SqlServerDsToSpSourceConnector()
            .createChangeEventContext(getJsonNode(changeEvent.toString()), ddl, ddl, "shadow_");
    Mutation shadowMutation = changeEventContext.getShadowTableMutation();
    Map<String, Value> actual = shadowMutation.asMap();

    Map<String, Value> expected =
        ChangeEventConvertorTest.getExpectedMapForTestChangeEventWithoutJsonField();
    expected.put(
        SqlServerDsToSpSourceConnector.SQLSERVER_TIMESTAMP_SHADOW_INFO.getLeft(),
        Value.int64(eventTimestamp));
    expected.put(
        SqlServerDsToSpSourceConnector.SQLSERVER_CHANGE_LSN_SHADOW_INFO.getLeft(),
        Value.string(""));

    assertThat(changeEventContext, instanceOf(SqlServerChangeEventContext.class));
    assertThat(actual, is(expected));
    assertEquals("shadow_Users2", shadowMutation.getTable());
    assertEquals(Mutation.Op.INSERT_OR_UPDATE, shadowMutation.getOperation());
  }

  @Test
  public void canGenerateShadowTableMutationForBackfillEventsWithMissingLsnKey() throws Exception {
    Ddl ddl = ChangeEventConvertorTest.getTestDdl();

    JSONObject changeEvent = ChangeEventConvertorTest.getTestChangeEvent("Users2");
    changeEvent.put(SqlServerDsToSpSourceConnector.SQLSERVER_TIMESTAMP_KEY, eventTimestamp);
    // Do not set SQLSERVER_CHANGE_LSN_KEY
    changeEvent.put(DatastreamConstants.EVENT_SOURCE_TYPE_KEY, SQLSERVER_SOURCE_TYPE);

    ChangeEventContext changeEventContext =
        new SqlServerDsToSpSourceConnector()
            .createChangeEventContext(getJsonNode(changeEvent.toString()), ddl, ddl, "shadow_");
    Mutation shadowMutation = changeEventContext.getShadowTableMutation();
    Map<String, Value> actual = shadowMutation.asMap();

    Map<String, Value> expected =
        ChangeEventConvertorTest.getExpectedMapForTestChangeEventWithoutJsonField();
    expected.put(
        SqlServerDsToSpSourceConnector.SQLSERVER_TIMESTAMP_SHADOW_INFO.getLeft(),
        Value.int64(eventTimestamp));
    expected.put(
        SqlServerDsToSpSourceConnector.SQLSERVER_CHANGE_LSN_SHADOW_INFO.getLeft(),
        Value.string(""));

    assertThat(changeEventContext, instanceOf(SqlServerChangeEventContext.class));
    assertThat(actual, is(expected));
    assertEquals("shadow_Users2", shadowMutation.getTable());
    assertEquals(Mutation.Op.INSERT_OR_UPDATE, shadowMutation.getOperation());
  }

  @Test
  public void canGenerateShadowTableMutationWithPostgresDialect() throws Exception {
    Ddl ddl =
        Ddl.builder(Dialect.POSTGRESQL)
            .createTable("Users")
            .column("id")
            .pgInt8()
            .endColumn()
            .column("name")
            .pgVarchar()
            .size(50)
            .endColumn()
            .primaryKey()
            .asc("id")
            .end()
            .endTable()
            .createTable("shadow_Users")
            .column("id")
            .pgInt8()
            .endColumn()
            .column("timestamp")
            .pgInt8()
            .endColumn()
            .column("change_lsn")
            .pgVarchar()
            .size(2621440)
            .endColumn()
            .primaryKey()
            .asc("id")
            .end()
            .endTable()
            .build();

    JSONObject changeEvent = new JSONObject();
    changeEvent.put("id", 1L);
    changeEvent.put("name", "John");
    changeEvent.put(DatastreamConstants.EVENT_TABLE_NAME_KEY, "Users");
    changeEvent.put(DatastreamConstants.EVENT_SOURCE_TYPE_KEY, SQLSERVER_SOURCE_TYPE);
    changeEvent.put(SqlServerDsToSpSourceConnector.SQLSERVER_TIMESTAMP_KEY, eventTimestamp);
    changeEvent.put(SqlServerDsToSpSourceConnector.SQLSERVER_CHANGE_LSN_KEY, eventLsn);

    SqlServerChangeEventContext context =
        new SqlServerChangeEventContext(getJsonNode(changeEvent.toString()), ddl, ddl, "shadow_");

    Mutation shadowMutation = context.getShadowTableMutation();
    Map<String, Value> actual = shadowMutation.asMap();

    assertEquals(Value.int64(1L), actual.get("id"));
    assertEquals(Value.int64(eventTimestamp), actual.get("timestamp"));
    assertEquals(Value.string(eventLsn), actual.get("change_lsn"));
    assertEquals("shadow_Users", shadowMutation.getTable());
    assertEquals(Mutation.Op.INSERT_OR_UPDATE, shadowMutation.getOperation());
  }

  @Test
  public void canGenerateShadowTableMutationWithLsnCollision() throws Exception {
    Ddl ddl =
        Ddl.builder()
            .createTable("MyTable")
            .column("change_lsn")
            .int64()
            .max()
            .endColumn()
            .column("data")
            .string()
            .max()
            .endColumn()
            .primaryKey()
            .asc("change_lsn")
            .end()
            .endTable()
            .build();
    Ddl shadowDdl =
        Ddl.builder()
            .createTable("shadow_MyTable")
            .column("change_lsn")
            .int64()
            .max()
            .endColumn()
            .column("data")
            .string()
            .max()
            .endColumn()
            .column("shadow_change_lsn")
            .string()
            .max()
            .endColumn()
            .column("timestamp")
            .int64()
            .endColumn()
            .primaryKey()
            .asc("change_lsn")
            .end()
            .endTable()
            .build();

    JSONObject changeEvent = new JSONObject();
    changeEvent.put("change_lsn", 42L);
    changeEvent.put("data", "test-data");
    changeEvent.put(DatastreamConstants.EVENT_TABLE_NAME_KEY, "MyTable");
    changeEvent.put(DatastreamConstants.EVENT_SOURCE_TYPE_KEY, SQLSERVER_SOURCE_TYPE);
    changeEvent.put(SqlServerDsToSpSourceConnector.SQLSERVER_TIMESTAMP_KEY, eventTimestamp);
    changeEvent.put(SqlServerDsToSpSourceConnector.SQLSERVER_CHANGE_LSN_KEY, eventLsn);

    ChangeEventContext context =
        new SqlServerDsToSpSourceConnector()
            .createChangeEventContext(
                getJsonNode(changeEvent.toString()), ddl, shadowDdl, "shadow_");

    Mutation shadowMutation = context.getShadowTableMutation();
    Map<String, Value> actual = shadowMutation.asMap();

    // Data table primary key column 'change_lsn'
    assertEquals(Value.int64(42L), actual.get("change_lsn"));
    // Conflicting shadow column renamed to 'shadow_change_lsn'
    assertEquals(Value.string(eventLsn), actual.get("shadow_change_lsn"));
    // Other shadow column retains default name
    assertEquals(Value.int64(eventTimestamp), actual.get("timestamp"));
  }

  @Test
  public void canGenerateShadowTableMutationWithTimestampCollision() throws Exception {
    Ddl ddl =
        Ddl.builder()
            .createTable("MyTable")
            .column("timestamp")
            .int64()
            .max()
            .endColumn()
            .column("data")
            .string()
            .max()
            .endColumn()
            .primaryKey()
            .asc("timestamp")
            .end()
            .endTable()
            .build();
    Ddl shadowDdl =
        Ddl.builder()
            .createTable("shadow_MyTable")
            .column("timestamp")
            .int64()
            .max()
            .endColumn()
            .column("data")
            .string()
            .max()
            .endColumn()
            .column("shadow_timestamp")
            .int64()
            .endColumn()
            .column("change_lsn")
            .string()
            .max()
            .endColumn()
            .primaryKey()
            .asc("timestamp")
            .end()
            .endTable()
            .build();

    JSONObject changeEvent = new JSONObject();
    changeEvent.put("timestamp", 999L);
    changeEvent.put("data", "sample");
    changeEvent.put(DatastreamConstants.EVENT_TABLE_NAME_KEY, "MyTable");
    changeEvent.put(DatastreamConstants.EVENT_SOURCE_TYPE_KEY, SQLSERVER_SOURCE_TYPE);
    changeEvent.put(SqlServerDsToSpSourceConnector.SQLSERVER_TIMESTAMP_KEY, eventTimestamp);
    changeEvent.put(SqlServerDsToSpSourceConnector.SQLSERVER_CHANGE_LSN_KEY, eventLsn);

    ChangeEventContext context =
        new SqlServerDsToSpSourceConnector()
            .createChangeEventContext(
                getJsonNode(changeEvent.toString()), ddl, shadowDdl, "shadow_");

    Mutation shadowMutation = context.getShadowTableMutation();
    Map<String, Value> actual = shadowMutation.asMap();

    // Data table primary key column 'timestamp'
    assertEquals(Value.int64(999L), actual.get("timestamp"));
    // Conflicting shadow column renamed to 'shadow_timestamp'
    assertEquals(Value.int64(eventTimestamp), actual.get("shadow_timestamp"));
    // Other shadow column retains default name
    assertEquals(Value.string(eventLsn), actual.get("change_lsn"));
  }

  @Test
  public void cannotGenerateShadowTableMutationMissingTimestamp() throws Exception {
    Ddl ddl = ChangeEventConvertorTest.getTestDdl();

    JSONObject changeEvent = ChangeEventConvertorTest.getTestChangeEvent("Users2");
    // Missing SQLSERVER_TIMESTAMP_KEY
    changeEvent.put(SqlServerDsToSpSourceConnector.SQLSERVER_CHANGE_LSN_KEY, eventLsn);
    changeEvent.put(DatastreamConstants.EVENT_SOURCE_TYPE_KEY, SQLSERVER_SOURCE_TYPE);

    assertThrows(
        ChangeEventConvertorException.class,
        () ->
            new SqlServerChangeEventContext(
                getJsonNode(changeEvent.toString()), ddl, ddl, "shadow_"));
  }
}
