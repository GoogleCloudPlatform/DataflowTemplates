/*
 * Copyright (C) 2022 Google LLC
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

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Value;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import java.io.IOException;
import java.util.Map;
import org.json.JSONObject;
import org.junit.Test;

/**
 * Unit tests 1) ChangeEventContextFactory methods for Postgres change events. 2) shadow table
 * generation for Postgres change events.
 */
public final class PostgresChangeEventContextTest {

  private final long eventTimestamp = 1615159728L;

  private JsonNode getJsonNode(String json) throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    return mapper.readTree(json);
  }

  @Test
  public void canGenerateShadowTableMutation() throws Exception {

    // Test Ddl
    Ddl ddl = ChangeEventConvertorTest.getTestDdl();

    // Test Change Event
    JSONObject changeEvent = ChangeEventConvertorTest.getTestChangeEvent("Users2");
    changeEvent.put(DatastreamConstants.POSTGRES_TIMESTAMP_KEY, eventTimestamp);
    changeEvent.put(DatastreamConstants.POSTGRES_LSN_KEY, "1/867");
    changeEvent.put(
        DatastreamConstants.EVENT_SOURCE_TYPE_KEY, DatastreamConstants.POSTGRES_SOURCE_TYPE);

    ChangeEventContext changeEventContext =
        ChangeEventContextFactory.createChangeEventContext(
            getJsonNode(changeEvent.toString()),
            ddl,
            "shadow_",
            DatastreamConstants.POSTGRES_SOURCE_TYPE);
    Mutation shadowMutation = changeEventContext.getShadowTableMutation();
    Map<String, Value> actual = shadowMutation.asMap();

    // Expected result
    Map<String, Value> expected =
        ChangeEventConvertorTest.getExpectedMapForTestChangeEventWithoutJsonField();
    expected.put(
        DatastreamConstants.POSTGRES_TIMESTAMP_SHADOW_INFO.getLeft(), Value.int64(eventTimestamp));
    expected.put(DatastreamConstants.POSTGRES_LSN_SHADOW_INFO.getLeft(), Value.string("1/867"));

    // Verify if PostgresChangeEventContext was actually created.
    assertThat(changeEventContext, instanceOf(PostgresChangeEventContext.class));
    // Verify shadow mutation
    assertThat(actual, is(expected));
    assertEquals(shadowMutation.getTable(), "shadow_Users2");
    assertEquals(shadowMutation.getOperation(), Mutation.Op.INSERT_OR_UPDATE);
  }

  @Test
  public void canGenerateShadowTableMutationForBackfillEvent() throws Exception {

    // Test Ddl
    Ddl ddl = ChangeEventConvertorTest.getTestDdl();

    // Test Change Event
    JSONObject changeEvent = ChangeEventConvertorTest.getTestChangeEvent("Users2");
    changeEvent.put(DatastreamConstants.POSTGRES_TIMESTAMP_KEY, eventTimestamp);
    changeEvent.put(DatastreamConstants.POSTGRES_LSN_KEY, JSONObject.NULL);
    changeEvent.put(
        DatastreamConstants.EVENT_SOURCE_TYPE_KEY, DatastreamConstants.POSTGRES_SOURCE_TYPE);

    ChangeEventContext changeEventContext =
        ChangeEventContextFactory.createChangeEventContext(
            getJsonNode(changeEvent.toString()),
            ddl,
            "shadow_",
            DatastreamConstants.POSTGRES_SOURCE_TYPE);
    Mutation shadowMutation = changeEventContext.getShadowTableMutation();
    Map<String, Value> actual = shadowMutation.asMap();

    // Expected result
    Map<String, Value> expected =
        ChangeEventConvertorTest.getExpectedMapForTestChangeEventWithoutJsonField();
    expected.put(
        DatastreamConstants.POSTGRES_TIMESTAMP_SHADOW_INFO.getLeft(), Value.int64(eventTimestamp));
    expected.put(DatastreamConstants.POSTGRES_LSN_SHADOW_INFO.getLeft(), Value.string(""));

    // Verify if PostgresChangeEventContext was actually created.
    assertThat(changeEventContext, instanceOf(PostgresChangeEventContext.class));
    // Verify shadow mutation
    assertThat(actual, is(expected));
    assertEquals(shadowMutation.getTable(), "shadow_Users2");
    assertEquals(shadowMutation.getOperation(), Mutation.Op.INSERT_OR_UPDATE);
  }

  @Test
  public void canGenerateShadowTableMutationForBackfillEventWithMissingKeys() throws Exception {

    // Test Ddl
    Ddl ddl = ChangeEventConvertorTest.getTestDdl();

    // Test Change Event
    JSONObject changeEvent = ChangeEventConvertorTest.getTestChangeEvent("Users2");
    changeEvent.put(DatastreamConstants.POSTGRES_TIMESTAMP_KEY, eventTimestamp);
    changeEvent.put(
        DatastreamConstants.EVENT_SOURCE_TYPE_KEY, DatastreamConstants.POSTGRES_SOURCE_TYPE);

    ChangeEventContext changeEventContext =
        ChangeEventContextFactory.createChangeEventContext(
            getJsonNode(changeEvent.toString()),
            ddl,
            "shadow_",
            DatastreamConstants.POSTGRES_SOURCE_TYPE);
    Mutation shadowMutation = changeEventContext.getShadowTableMutation();
    Map<String, Value> actual = shadowMutation.asMap();

    // Expected result
    Map<String, Value> expected =
        ChangeEventConvertorTest.getExpectedMapForTestChangeEventWithoutJsonField();
    expected.put(
        DatastreamConstants.POSTGRES_TIMESTAMP_SHADOW_INFO.getLeft(), Value.int64(eventTimestamp));
    expected.put(DatastreamConstants.POSTGRES_LSN_SHADOW_INFO.getLeft(), Value.string(""));

    // Verify if PostgresChangeEventContext was actually created.
    assertThat(changeEventContext, instanceOf(PostgresChangeEventContext.class));
    // Verify shadow mutation
    assertThat(actual, is(expected));
    assertEquals(shadowMutation.getTable(), "shadow_Users2");
    assertEquals(shadowMutation.getOperation(), Mutation.Op.INSERT_OR_UPDATE);
  }
}
