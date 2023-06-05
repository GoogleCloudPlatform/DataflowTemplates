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
package com.google.cloud.teleport.v2.templates;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.ReadContext;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Statement;
import com.google.cloud.teleport.v2.templates.datastream.DatastreamConstants;
import com.google.cloud.teleport.v2.templates.session.ReadSessionFileTest;
import com.google.cloud.teleport.v2.templates.session.Session;
import com.google.cloud.teleport.v2.templates.spanner.ddl.Ddl;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

/** Unit tests SpannerTransactionWriterDoFn class. */
public class SpannerTransactionWriterDoFnTest {
  @Mock private DatabaseClient databaseClient;
  @Mock private ReadContext queryReadContext;
  @Mock private ResultSet queryResultSet;

  public static JsonNode parseChangeEvent(String json) {
    try {
      ObjectMapper mapper = new ObjectMapper();
      mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
      return mapper.readTree(json);
    } catch (IOException e) {
      // No action. Return null.
    }
    return null;
  }

  @Before
  public void setUp() throws IOException {
    mockDbClient();
  }

  private void mockDbClient() throws IOException {
    databaseClient = mock(DatabaseClient.class);
    queryReadContext = mock(ReadContext.class);
    queryResultSet = mock(ResultSet.class);
    when(databaseClient.singleUse()).thenReturn(queryReadContext);
    when(queryReadContext.executeQuery(any(Statement.class))).thenReturn(queryResultSet);
    when(queryResultSet.next()).thenReturn(true, false); // only return one row
    when(queryResultSet.getJson(any(String.class)))
        .thenReturn("{\"a\": 1.3542, \"b\": {\"c\": 48.198136676310106}}");
  }

  @Test
  public void transformChangeEventViaSessionFileNamesTest() {
    Session session = ReadSessionFileTest.getSessionObject();
    SpannerTransactionWriterDoFn spannerTransactionWriterDoFn =
        new SpannerTransactionWriterDoFn(
            SpannerConfig.create(), null, session, new HashMap<>(), "", "", false);
    JSONObject changeEvent = new JSONObject();
    changeEvent.put("product_id", "A");
    changeEvent.put("quantity", 1);
    changeEvent.put("user_id", "B");
    changeEvent.put(DatastreamConstants.EVENT_TABLE_NAME_KEY, "cart");
    JsonNode ce = parseChangeEvent(changeEvent.toString());

    JsonNode actualEvent = spannerTransactionWriterDoFn.transformChangeEventViaSessionFile(ce);

    JSONObject changeEventNew = new JSONObject();
    changeEventNew.put("new_product_id", "A");
    changeEventNew.put("new_quantity", 1);
    changeEventNew.put("new_user_id", "B");
    changeEventNew.put(DatastreamConstants.EVENT_TABLE_NAME_KEY, "new_cart");
    JsonNode expectedEvent = parseChangeEvent(changeEventNew.toString());
    assertEquals(expectedEvent, actualEvent);
  }

  @Test
  public void transformChangeEventViaSessionFileSynthPKTest() {
    Session session = ReadSessionFileTest.getSessionObject();
    SpannerTransactionWriterDoFn spannerTransactionWriterDoFn =
        new SpannerTransactionWriterDoFn(
            SpannerConfig.create(), null, session, new HashMap<>(), "", "", false);
    JSONObject changeEvent = new JSONObject();
    changeEvent.put("name", "A");
    changeEvent.put(DatastreamConstants.EVENT_TABLE_NAME_KEY, "people");
    changeEvent.put(DatastreamConstants.EVENT_UUID_KEY, "abc-123");
    JsonNode ce = parseChangeEvent(changeEvent.toString());

    JsonNode actualEvent = spannerTransactionWriterDoFn.transformChangeEventViaSessionFile(ce);

    JSONObject changeEventNew = new JSONObject();
    changeEventNew.put("new_name", "A");
    changeEventNew.put(DatastreamConstants.EVENT_TABLE_NAME_KEY, "new_people");
    changeEventNew.put(DatastreamConstants.EVENT_UUID_KEY, "abc-123");
    changeEventNew.put("synth_id", "abc-123");
    JsonNode expectedEvent = parseChangeEvent(changeEventNew.toString());
    assertEquals(expectedEvent, actualEvent);
  }

  @Test
  public void transformChangeEventDataTest() throws Exception {
    SpannerTransactionWriterDoFn spannerTransactionWriterDoFn =
        new SpannerTransactionWriterDoFn(
            SpannerConfig.create(), null, null, new HashMap<>(), "", "", true);
    JSONObject changeEvent = new JSONObject();
    changeEvent.put("first_name", "A");
    changeEvent.put("last_name", "{\"a\": 1.3542, \"b\": {\"c\": 48.19813667631011}}");
    changeEvent.put(DatastreamConstants.EVENT_TABLE_NAME_KEY, "Users");
    JsonNode ce = parseChangeEvent(changeEvent.toString());

    JsonNode actualEvent =
        spannerTransactionWriterDoFn.transformChangeEventData(ce, databaseClient, getTestDdl());

    changeEvent = new JSONObject();
    changeEvent.put("first_name", "A");
    changeEvent.put("last_name", "{\"a\": 1.3542, \"b\": {\"c\": 48.198136676310106}}");
    changeEvent.put(DatastreamConstants.EVENT_TABLE_NAME_KEY, "Users");
    JsonNode expectedEvent = parseChangeEvent(changeEvent.toString());

    assertEquals(expectedEvent, actualEvent);
  }

  static Ddl getTestDdl() {
    Ddl ddl =
        Ddl.builder()
            .createTable("Users")
            .column("first_name")
            .string()
            .max()
            .endColumn()
            .column("last_name")
            .json()
            .endColumn()
            .endTable()
            .build();
    return ddl;
  }

  @Test
  public void shardedConfigDataTest() throws Exception {
    Map<String, String> shardingConfig = new HashMap<>();
    shardingConfig.put("db_01", "1");
    shardingConfig.put("db_02", "2");
    SpannerTransactionWriterDoFn spannerTransactionWriterDoFn =
        new SpannerTransactionWriterDoFn(
            SpannerConfig.create(), null, null, shardingConfig, "", "", true);
    JSONObject changeEvent = new JSONObject();
    changeEvent.put("first_name", "A");
    changeEvent.put("last_name", "{\"a\": 1.3542, \"b\": {\"c\": 48.19813667631011}}");
    changeEvent.put(DatastreamConstants.EVENT_TABLE_NAME_KEY, "Users");
    JsonNode ce = parseChangeEvent(changeEvent.toString());

    JsonNode actualEvent =
        spannerTransactionWriterDoFn.transformChangeEventData(ce, databaseClient, getTestDdl());

    changeEvent = new JSONObject();
    changeEvent.put("first_name", "A");
    changeEvent.put("last_name", "{\"a\": 1.3542, \"b\": {\"c\": 48.198136676310106}}");
    changeEvent.put(DatastreamConstants.EVENT_TABLE_NAME_KEY, "Users");
    JsonNode expectedEvent = parseChangeEvent(changeEvent.toString());

    assertEquals(expectedEvent, actualEvent);
  }

  @Test
  public void transformChangeEventViaShardedSessionFileTest() {
    Map<String, String> shardingConfig = new HashMap<>();
    shardingConfig.put("db_01", "1");
    shardingConfig.put("db_02", "2");
    Session session = ReadSessionFileTest.getShardedSessionObject();
    SpannerTransactionWriterDoFn spannerTransactionWriterDoFn =
        new SpannerTransactionWriterDoFn(
            SpannerConfig.create(), null, session, shardingConfig, "", "", false);
    JSONObject changeEvent = new JSONObject();
    changeEvent.put("name", "A");
    changeEvent.put(DatastreamConstants.EVENT_SCHEMA_KEY, "db_01");
    changeEvent.put(DatastreamConstants.EVENT_TABLE_NAME_KEY, "people");
    changeEvent.put(DatastreamConstants.EVENT_UUID_KEY, "abc-123");
    JsonNode ce = parseChangeEvent(changeEvent.toString());

    JsonNode actualEvent = spannerTransactionWriterDoFn.transformChangeEventViaSessionFile(ce);

    JSONObject changeEventNew = new JSONObject();
    changeEventNew.put("new_name", "A");
    changeEventNew.put(DatastreamConstants.EVENT_SCHEMA_KEY, "db_01");
    changeEventNew.put(DatastreamConstants.EVENT_TABLE_NAME_KEY, "new_people");
    changeEventNew.put(DatastreamConstants.EVENT_UUID_KEY, "abc-123");
    changeEventNew.put("migration_shard_id", "1");
    JsonNode expectedEvent = parseChangeEvent(changeEventNew.toString());
    assertEquals(expectedEvent, actualEvent);
  }
}
