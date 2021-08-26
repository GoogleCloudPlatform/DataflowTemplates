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

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Value;
import com.google.cloud.teleport.v2.templates.spanner.ddl.Ddl;
import java.io.IOException;
import java.util.Map;
import org.json.JSONObject;
import org.junit.Test;

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
        ChangeEventContextFactory.createChangeEventContext(getJsonNode(changeEvent.toString()),
            ddl, "shadow_", DatastreamConstants.MYSQL_SOURCE_TYPE);
    Mutation shadowMutation = changeEventContext.getShadowTableMutation();
    Map<String, Value> actual = shadowMutation.asMap();

    // Expected result
    Map<String, Value> expected = ChangeEventConvertorTest.getExpectedMapForTestChangeEvent();
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
        ChangeEventContextFactory.createChangeEventContext(getJsonNode(changeEvent.toString()),
            ddl, "shadow_", DatastreamConstants.MYSQL_SOURCE_TYPE);
    Mutation shadowMutation = changeEventContext.getShadowTableMutation();
    Map<String, Value> actual = shadowMutation.asMap();

    // Expected result
    Map<String, Value> expected = ChangeEventConvertorTest.getExpectedMapForTestChangeEvent();
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
        ChangeEventContextFactory.createChangeEventContext(getJsonNode(changeEvent.toString()),
            ddl, "shadow_", DatastreamConstants.MYSQL_SOURCE_TYPE);
    Mutation shadowMutation = changeEventContext.getShadowTableMutation();
    Map<String, Value> actual = shadowMutation.asMap();

    // Expected result
    Map<String, Value> expected = ChangeEventConvertorTest.getExpectedMapForTestChangeEvent();
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
}
