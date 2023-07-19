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
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.migrations.schema.ColumnPK;
import com.google.cloud.teleport.v2.spanner.migrations.schema.NameAndCols;
import com.google.cloud.teleport.v2.spanner.migrations.schema.Schema;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SourceColumnDefinition;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SourceColumnType;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SourceTable;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SpannerColumnDefinition;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SpannerColumnType;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SpannerTable;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SyntheticPKey;
import com.google.cloud.teleport.v2.spanner.migrations.transformation.TransformationContext;
import com.google.cloud.teleport.v2.templates.datastream.DatastreamConstants;
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
    Schema schema = getSchemaObject();
    SpannerTransactionWriterDoFn spannerTransactionWriterDoFn =
        new SpannerTransactionWriterDoFn(
            SpannerConfig.create(), null, schema, new TransformationContext(), "", "", false, true);
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
    Schema schema = getSchemaObject();
    SpannerTransactionWriterDoFn spannerTransactionWriterDoFn =
        new SpannerTransactionWriterDoFn(
            SpannerConfig.create(), null, schema, new TransformationContext(), "", "", false, true);
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
            SpannerConfig.create(), null, null, new TransformationContext(), "", "", true, true);
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

  private static Schema getSchemaObject() {
    // Add Synthetic PKs.
    Map<String, SyntheticPKey> syntheticPKeys = getSyntheticPks();
    // Add SrcSchema.
    Map<String, SourceTable> srcSchema = getSampleSrcSchema();
    // Add SpSchema.
    Map<String, SpannerTable> spSchema = getSampleSpSchema();
    // Add ToSpanner.
    Map<String, NameAndCols> toSpanner = getToSpanner();
    // Add SrcToID.
    Map<String, NameAndCols> srcToId = getSrcToId();
    Schema expectedSchema = new Schema(spSchema, syntheticPKeys, srcSchema);
    expectedSchema.setToSpanner(toSpanner);
    expectedSchema.setToSource(new HashMap<String, NameAndCols>());
    expectedSchema.setSrcToID(srcToId);
    expectedSchema.setSpannerToID(new HashMap<String, NameAndCols>());
    return expectedSchema;
  }

  private static Map<String, SyntheticPKey> getSyntheticPks() {
    Map<String, SyntheticPKey> syntheticPKeys = new HashMap<String, SyntheticPKey>();
    syntheticPKeys.put("t2", new SyntheticPKey("c6", 0));
    return syntheticPKeys;
  }

  private static Map<String, SourceTable> getSampleSrcSchema() {
    Map<String, SourceTable> srcSchema = new HashMap<String, SourceTable>();
    Map<String, SourceColumnDefinition> t1SrcColDefs =
        new HashMap<String, SourceColumnDefinition>();
    t1SrcColDefs.put(
        "c1",
        new SourceColumnDefinition(
            "product_id", new SourceColumnType("varchar", new Long[] {20L}, null)));
    t1SrcColDefs.put(
        "c2", new SourceColumnDefinition("quantity", new SourceColumnType("bigint", null, null)));
    t1SrcColDefs.put(
        "c3",
        new SourceColumnDefinition(
            "user_id", new SourceColumnType("varchar", new Long[] {20L}, null)));
    srcSchema.put(
        "t1",
        new SourceTable(
            "cart",
            "my_schema",
            new String[] {"c3", "c1", "c2"},
            t1SrcColDefs,
            new ColumnPK[] {new ColumnPK("c3", 1), new ColumnPK("c1", 2)}));
    Map<String, SourceColumnDefinition> t2SrcColDefs =
        new HashMap<String, SourceColumnDefinition>();
    t2SrcColDefs.put(
        "c5",
        new SourceColumnDefinition(
            "name", new SourceColumnType("varchar", new Long[] {20L}, null)));
    srcSchema.put(
        "t2", new SourceTable("people", "my_schema", new String[] {"c5"}, t2SrcColDefs, null));
    return srcSchema;
  }

  private static Map<String, SpannerTable> getSampleSpSchema() {
    Map<String, SpannerTable> spSchema = new HashMap<String, SpannerTable>();
    Map<String, SpannerColumnDefinition> t1SpColDefs =
        new HashMap<String, SpannerColumnDefinition>();
    t1SpColDefs.put(
        "c1",
        new SpannerColumnDefinition("new_product_id", new SpannerColumnType("STRING", false)));
    t1SpColDefs.put(
        "c2", new SpannerColumnDefinition("new_quantity", new SpannerColumnType("INT64", false)));
    t1SpColDefs.put(
        "c3", new SpannerColumnDefinition("new_user_id", new SpannerColumnType("STRING", false)));
    spSchema.put(
        "t1",
        new SpannerTable(
            "new_cart",
            new String[] {"c1", "c2", "c3"},
            t1SpColDefs,
            new ColumnPK[] {new ColumnPK("c3", 1), new ColumnPK("c1", 2)},
            ""));
    Map<String, SpannerColumnDefinition> t2SpColDefs =
        new HashMap<String, SpannerColumnDefinition>();
    t2SpColDefs.put(
        "c5", new SpannerColumnDefinition("new_name", new SpannerColumnType("STRING", false)));
    t2SpColDefs.put(
        "c6", new SpannerColumnDefinition("synth_id", new SpannerColumnType("INT64", false)));
    spSchema.put(
        "t2",
        new SpannerTable(
            "new_people",
            new String[] {"c5", "c6"},
            t2SpColDefs,
            new ColumnPK[] {new ColumnPK("c6", 1)},
            ""));
    return spSchema;
  }

  private static Map<String, NameAndCols> getToSpanner() {
    Map<String, NameAndCols> toSpanner = new HashMap<String, NameAndCols>();
    Map<String, String> t1Cols = new HashMap<String, String>();
    t1Cols.put("product_id", "new_product_id");
    t1Cols.put("quantity", "new_quantity");
    t1Cols.put("user_id", "new_user_id");
    toSpanner.put("cart", new NameAndCols("new_cart", t1Cols));
    Map<String, String> t2Cols = new HashMap<String, String>();
    t2Cols.put("name", "new_name");
    toSpanner.put("people", new NameAndCols("new_people", t2Cols));
    return toSpanner;
  }

  private static Map<String, NameAndCols> getSrcToId() {
    Map<String, NameAndCols> srcToId = new HashMap<String, NameAndCols>();
    Map<String, String> t1ColIds = new HashMap<String, String>();
    t1ColIds.put("product_id", "c1");
    t1ColIds.put("quantity", "c2");
    t1ColIds.put("user_id", "c3");
    srcToId.put("cart", new NameAndCols("t1", t1ColIds));
    Map<String, String> t2ColIds = new HashMap<String, String>();
    t2ColIds.put("name", "c5");
    srcToId.put("people", new NameAndCols("t2", t2ColIds));
    return srcToId;
  }

  public static TransformationContext getTransformationContext() {
    Map<String, String> shardingConfig = new HashMap<>();
    shardingConfig.put("db_01", "1");
    shardingConfig.put("db_02", "2");

    TransformationContext transformationContext = new TransformationContext(shardingConfig);
    return transformationContext;
  }

  @Test
  public void shardedConfigDataTest() throws Exception {
    TransformationContext transformationContext = getTransformationContext();
    SpannerTransactionWriterDoFn spannerTransactionWriterDoFn =
        new SpannerTransactionWriterDoFn(
            SpannerConfig.create(), null, null, transformationContext, "", "", true, true);
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
    Schema schema = getShardedSchemaObject();
    TransformationContext transformationContext = getTransformationContext();
    SpannerTransactionWriterDoFn spannerTransactionWriterDoFn =
        new SpannerTransactionWriterDoFn(
            SpannerConfig.create(), null, schema, transformationContext, "", "mysql", false, true);
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

  public static Schema getShardedSchemaObject() {
    // Add SrcSchema.
    Map<String, SourceTable> srcSchema = getSampleSrcSchema();
    // Add SpSchema.
    Map<String, SpannerTable> spSchema = getSampleShardedSpSchema();
    // Add ToSpanner.
    Map<String, NameAndCols> toSpanner = getToSpanner();
    // Add SrcToID.
    Map<String, NameAndCols> srcToId = getSrcToId();
    Schema expectedSchema = new Schema(spSchema, new HashMap<>(), srcSchema);
    expectedSchema.setToSpanner(toSpanner);
    expectedSchema.setToSource(new HashMap<String, NameAndCols>());
    expectedSchema.setSrcToID(srcToId);
    expectedSchema.setSpannerToID(new HashMap<String, NameAndCols>());
    return expectedSchema;
  }

  public static Map<String, SpannerTable> getSampleShardedSpSchema() {
    Map<String, SpannerTable> spSchema = new HashMap<String, SpannerTable>();
    Map<String, SpannerColumnDefinition> t1SpColDefs =
        new HashMap<String, SpannerColumnDefinition>();
    t1SpColDefs.put(
        "c1",
        new SpannerColumnDefinition("new_product_id", new SpannerColumnType("STRING", false)));
    t1SpColDefs.put(
        "c2", new SpannerColumnDefinition("new_quantity", new SpannerColumnType("INT64", false)));
    t1SpColDefs.put(
        "c3", new SpannerColumnDefinition("new_user_id", new SpannerColumnType("STRING", false)));
    spSchema.put(
        "t1",
        new SpannerTable(
            "new_cart",
            new String[] {"c1", "c2", "c3"},
            t1SpColDefs,
            new ColumnPK[] {new ColumnPK("c3", 1), new ColumnPK("c1", 2)},
            null));
    Map<String, SpannerColumnDefinition> t2SpColDefs =
        new HashMap<String, SpannerColumnDefinition>();
    t2SpColDefs.put(
        "c5", new SpannerColumnDefinition("new_name", new SpannerColumnType("STRING", false)));
    t2SpColDefs.put(
        "c6",
        new SpannerColumnDefinition("migration_shard_id", new SpannerColumnType("STRING", false)));
    spSchema.put(
        "t2",
        new SpannerTable(
            "new_people",
            new String[] {"c5", "c6"},
            t2SpColDefs,
            new ColumnPK[] {new ColumnPK("c6", 1), new ColumnPK("c5", 2)},
            "c6"));
    return spSchema;
  }
}
