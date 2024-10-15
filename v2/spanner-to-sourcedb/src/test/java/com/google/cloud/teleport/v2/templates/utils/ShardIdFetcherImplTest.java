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
package com.google.cloud.teleport.v2.templates.utils;

import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.migrations.schema.ColumnPK;
import com.google.cloud.teleport.v2.spanner.migrations.schema.NameAndCols;
import com.google.cloud.teleport.v2.spanner.migrations.schema.Schema;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SourceTable;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SpannerColumnDefinition;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SpannerColumnType;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SpannerTable;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SyntheticPKey;
import com.google.cloud.teleport.v2.spanner.utils.ShardIdRequest;
import com.google.cloud.teleport.v2.spanner.utils.ShardIdResponse;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public final class ShardIdFetcherImplTest {

  @Test
  public void testSucessShardIdentification() {
    ShardIdFetcherImpl shardIdFetcher = new ShardIdFetcherImpl(getSchemaObject(), "skip");
    Map<String, Object> spannerRecord = new HashMap<String, Object>();
    spannerRecord.put("accountId", 12345);
    spannerRecord.put("accountName", "test");
    spannerRecord.put("migration_shard_id", "shard1");
    spannerRecord.put("accountNumber", 42);
    ShardIdRequest shardIdRequest = new ShardIdRequest("tableName", spannerRecord);
    ShardIdResponse shardIdResponse = shardIdFetcher.getShardId(shardIdRequest);
    assertThat(shardIdResponse.getLogicalShardId()).isEqualTo("shard1");
  }

  @Test(expected = RuntimeException.class)
  public void testFailedShardIdentificationWrongTableName() {
    ShardIdFetcherImpl shardIdFetcher = new ShardIdFetcherImpl(getSchemaObject(), "skip");
    Map<String, Object> spannerRecord = new HashMap<String, Object>();
    spannerRecord.put("accountId", 12345);
    spannerRecord.put("accountName", "test");
    spannerRecord.put("migration_shard_id", "shard1");
    spannerRecord.put("accountNumber", 42);
    ShardIdRequest shardIdRequest = new ShardIdRequest("junk", spannerRecord);
    shardIdFetcher.getShardId(shardIdRequest);
  }

  @Test(expected = RuntimeException.class)
  public void testFailedShardIdentificationMissingShardColumn() {
    ShardIdFetcherImpl shardIdFetcher = new ShardIdFetcherImpl(getSchemaObject(), "skip");
    Map<String, Object> spannerRecord = new HashMap<String, Object>();
    spannerRecord.put("accountId", 12345);
    spannerRecord.put("accountName", "test");
    spannerRecord.put("accountNumber", 42);
    ShardIdRequest shardIdRequest = new ShardIdRequest("tableName", spannerRecord);
    shardIdFetcher.getShardId(shardIdRequest);
  }

  @Test(expected = RuntimeException.class)
  public void testFailedShardIdentificationSpannerToOidMismatch() {
    ShardIdFetcherImpl shardIdFetcher =
        new ShardIdFetcherImpl(getBotchedSchemaObjectForInvalidSpannerToOid(), "skip");
    Map<String, Object> spannerRecord = new HashMap<String, Object>();
    spannerRecord.put("accountId", 12345);
    spannerRecord.put("accountName", "test");
    spannerRecord.put("accountNumber", 42);
    spannerRecord.put("migration_shard_id", "shard1");

    ShardIdRequest shardIdRequest = new ShardIdRequest("tableName", spannerRecord);
    shardIdFetcher.getShardId(shardIdRequest);
  }

  @Test(expected = RuntimeException.class)
  public void testFailedShardIdentificationSpannerInvalidSpSchema() {
    ShardIdFetcherImpl shardIdFetcher =
        new ShardIdFetcherImpl(getBotchedSchemaObjectForInvalidSpSchema(), "skip");
    Map<String, Object> spannerRecord = new HashMap<String, Object>();
    spannerRecord.put("accountId", 12345);
    spannerRecord.put("accountName", "test");
    spannerRecord.put("accountNumber", 42);
    spannerRecord.put("migration_shard_id", "shard1");

    ShardIdRequest shardIdRequest = new ShardIdRequest("tableName", spannerRecord);
    shardIdFetcher.getShardId(shardIdRequest);
  }

  @Test(expected = RuntimeException.class)
  public void testFailedShardIdentificationMissingShardColumnFromSchema() {
    ShardIdFetcherImpl shardIdFetcher =
        new ShardIdFetcherImpl(getBotchedSchemaObjectForMissingShardColumn(), "skip");
    Map<String, Object> spannerRecord = new HashMap<String, Object>();
    spannerRecord.put("accountId", 12345);
    spannerRecord.put("accountName", "test");
    spannerRecord.put("accountNumber", 42);
    spannerRecord.put("migration_shard_id", "shard1");

    ShardIdRequest shardIdRequest = new ShardIdRequest("tableName", spannerRecord);
    shardIdFetcher.getShardId(shardIdRequest);
  }

  public static Schema getSchemaObject() {
    Map<String, SyntheticPKey> syntheticPKeys = new HashMap<String, SyntheticPKey>();
    Map<String, SourceTable> srcSchema = new HashMap<String, SourceTable>();
    Map<String, SpannerTable> spSchema = getSampleSpSchema();
    Map<String, NameAndCols> spannerToID = getSampleSpannerToId();
    Schema expectedSchema = new Schema(spSchema, syntheticPKeys, srcSchema);
    expectedSchema.setSpannerToID(spannerToID);
    return expectedSchema;
  }

  public static Map<String, SpannerTable> getSampleSpSchema() {
    Map<String, SpannerTable> spSchema = new HashMap<String, SpannerTable>();
    Map<String, SpannerColumnDefinition> t1SpColDefs =
        new HashMap<String, SpannerColumnDefinition>();
    t1SpColDefs.put(
        "c1", new SpannerColumnDefinition("accountId", new SpannerColumnType("STRING", false)));
    t1SpColDefs.put(
        "c2", new SpannerColumnDefinition("accountName", new SpannerColumnType("STRING", false)));
    t1SpColDefs.put(
        "c3",
        new SpannerColumnDefinition("migration_shard_id", new SpannerColumnType("STRING", false)));
    t1SpColDefs.put(
        "c4", new SpannerColumnDefinition("accountNumber", new SpannerColumnType("INT", false)));
    spSchema.put(
        "t1",
        new SpannerTable(
            "tableName",
            new String[] {"c1", "c2", "c3", "c4"},
            t1SpColDefs,
            new ColumnPK[] {new ColumnPK("c1", 1)},
            "c3"));
    return spSchema;
  }

  public static Map<String, NameAndCols> getSampleSpannerToId() {
    Map<String, NameAndCols> spannerToId = new HashMap<String, NameAndCols>();
    Map<String, String> t1ColIds = new HashMap<String, String>();
    t1ColIds.put("accountId", "c1");
    t1ColIds.put("accountName", "c2");
    t1ColIds.put("migration_shard_id", "c3");
    t1ColIds.put("accountNumber", "c4");
    spannerToId.put("tableName", new NameAndCols("t1", t1ColIds));
    return spannerToId;
  }

  static Ddl getTestDdl() {
    Ddl ddl =
        Ddl.builder()
            .createTable("tableName")
            .column("accountId")
            .string()
            .max()
            .endColumn()
            .column("accountName")
            .string()
            .max()
            .endColumn()
            .column("migration_shard_id")
            .string()
            .max()
            .endColumn()
            .column("accountNumber")
            .int64()
            .endColumn()
            .endTable()
            .build();
    return ddl;
  }

  public static Schema getBotchedSchemaObjectForInvalidSpannerToOid() {
    Map<String, SyntheticPKey> syntheticPKeys = new HashMap<String, SyntheticPKey>();
    Map<String, SourceTable> srcSchema = new HashMap<String, SourceTable>();
    Map<String, SpannerTable> spSchema = getSampleSpSchema();
    Map<String, NameAndCols> spannerToID = getBotchedSampleSpannerToId();
    Schema expectedSchema = new Schema(spSchema, syntheticPKeys, srcSchema);
    expectedSchema.setSpannerToID(spannerToID);
    return expectedSchema;
  }

  public static Schema getBotchedSchemaObjectForInvalidSpSchema() {
    Map<String, SyntheticPKey> syntheticPKeys = new HashMap<String, SyntheticPKey>();
    Map<String, SourceTable> srcSchema = new HashMap<String, SourceTable>();
    Map<String, SpannerTable> spSchema = getBotchedSampleSpSchema();
    Map<String, NameAndCols> spannerToID = getSampleSpannerToId();
    Schema expectedSchema = new Schema(spSchema, syntheticPKeys, srcSchema);
    expectedSchema.setSpannerToID(spannerToID);
    return expectedSchema;
  }

  public static Schema getBotchedSchemaObjectForMissingShardColumn() {
    Map<String, SyntheticPKey> syntheticPKeys = new HashMap<String, SyntheticPKey>();
    Map<String, SourceTable> srcSchema = new HashMap<String, SourceTable>();
    Map<String, SpannerTable> spSchema = getBotchedSampleSpColmSchema();
    Map<String, NameAndCols> spannerToID = getSampleSpannerToId();
    Schema expectedSchema = new Schema(spSchema, syntheticPKeys, srcSchema);
    expectedSchema.setSpannerToID(spannerToID);
    return expectedSchema;
  }

  public static Map<String, SpannerTable> getBotchedSampleSpSchema() {
    Map<String, SpannerTable> spSchema = new HashMap<String, SpannerTable>();
    Map<String, SpannerColumnDefinition> t1SpColDefs =
        new HashMap<String, SpannerColumnDefinition>();
    t1SpColDefs.put(
        "c1", new SpannerColumnDefinition("accountId", new SpannerColumnType("STRING", false)));
    t1SpColDefs.put(
        "c2", new SpannerColumnDefinition("accountName", new SpannerColumnType("STRING", false)));
    t1SpColDefs.put(
        "c3",
        new SpannerColumnDefinition("migration_shard_id", new SpannerColumnType("STRING", false)));
    t1SpColDefs.put(
        "c4", new SpannerColumnDefinition("accountNumber", new SpannerColumnType("INT", false)));
    spSchema.put(
        "junk",
        new SpannerTable(
            "junk",
            new String[] {"c1", "c2", "c3", "c4"},
            t1SpColDefs,
            new ColumnPK[] {new ColumnPK("c1", 1)},
            "c3"));
    return spSchema;
  }

  public static Map<String, NameAndCols> getBotchedSampleSpannerToId() {
    Map<String, NameAndCols> spannerToId = new HashMap<String, NameAndCols>();
    Map<String, String> t1ColIds = new HashMap<String, String>();
    t1ColIds.put("accountId", "c1");
    t1ColIds.put("accountName", "c2");
    t1ColIds.put("migration_shard_id", "c3");
    t1ColIds.put("accountNumber", "c4");
    spannerToId.put("junk", new NameAndCols("t1", t1ColIds));
    return spannerToId;
  }

  public static Map<String, SpannerTable> getBotchedSampleSpColmSchema() {
    Map<String, SpannerTable> spSchema = new HashMap<String, SpannerTable>();
    Map<String, SpannerColumnDefinition> t1SpColDefs =
        new HashMap<String, SpannerColumnDefinition>();
    t1SpColDefs.put(
        "c1", new SpannerColumnDefinition("accountId", new SpannerColumnType("STRING", false)));
    t1SpColDefs.put(
        "c2", new SpannerColumnDefinition("accountName", new SpannerColumnType("STRING", false)));

    t1SpColDefs.put(
        "c4", new SpannerColumnDefinition("accountNumber", new SpannerColumnType("INT", false)));
    spSchema.put(
        "t1",
        new SpannerTable(
            "tableName",
            new String[] {"c1", "c2", "c3", "c4"},
            t1SpColDefs,
            new ColumnPK[] {new ColumnPK("c1", 1)},
            "c3"));
    return spSchema;
  }
}
