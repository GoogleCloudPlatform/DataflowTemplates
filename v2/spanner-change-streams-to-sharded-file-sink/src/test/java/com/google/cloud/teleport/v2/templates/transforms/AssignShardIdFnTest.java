/*
 * Copyright (C) 2023 Google LLC
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
package com.google.cloud.teleport.v2.templates.transforms;

import com.google.cloud.Timestamp;
import com.google.cloud.teleport.v2.spanner.migrations.schema.ColumnPK;
import com.google.cloud.teleport.v2.spanner.migrations.schema.NameAndCols;
import com.google.cloud.teleport.v2.spanner.migrations.schema.Schema;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SourceTable;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SpannerColumnDefinition;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SpannerColumnType;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SpannerTable;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SyntheticPKey;
import com.google.cloud.teleport.v2.templates.common.TrimmedShardedDataChangeRecord;
import com.google.cloud.teleport.v2.templates.constants.Constants;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.Mod;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.ModType;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;

/** Tests for AssignShardIdFnTest class. */
public class AssignShardIdFnTest {
  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void basicTestMultiShard() {
    TrimmedShardedDataChangeRecord record1 = getTrimmedDataChangeRecord("shard1");
    TrimmedShardedDataChangeRecord record2 = getTrimmedDataChangeRecord("shard2");
    List<TrimmedShardedDataChangeRecord> records = Arrays.asList(record1, record2);
    PCollection<TrimmedShardedDataChangeRecord> output =
        pipeline
            .apply(
                Create.of(records)
                    .withCoder(SerializableCoder.of(TrimmedShardedDataChangeRecord.class)))
            .apply(
                ParDo.of(
                    new AssignShardIdFn(
                        null,
                        getSchemaObject(),
                        null,
                        Constants.SHARDING_MODE_MULTI_SHARD,
                        "test")));

    record1.setShard("shard1");
    record2.setShard("shard2");

    PAssert.that(output).containsInAnyOrder(record1, record2);
    pipeline.run();
  }

  @Test
  public void basicTestSingleShard() {
    TrimmedShardedDataChangeRecord record1 = getTrimmedDataChangeRecord("shard1");
    TrimmedShardedDataChangeRecord record2 = getTrimmedDataChangeRecord("shard2");
    List<TrimmedShardedDataChangeRecord> records = Arrays.asList(record1, record2);
    PCollection<TrimmedShardedDataChangeRecord> output =
        pipeline
            .apply(
                Create.of(records)
                    .withCoder(SerializableCoder.of(TrimmedShardedDataChangeRecord.class)))
            .apply(
                ParDo.of(
                    new AssignShardIdFn(
                        null, null, null, Constants.SHARDING_MODE_SINGLE_SHARD, "test")));

    record1.setShard("test");
    record2.setShard("test");

    PAssert.that(output).containsInAnyOrder(record1, record2);
    pipeline.run();
  }

  public TrimmedShardedDataChangeRecord getTrimmedDataChangeRecord(String shardId) {
    return new TrimmedShardedDataChangeRecord(
        Timestamp.parseTimestamp("2020-12-01T10:15:30.000Z"),
        "serverTxnId",
        "recordSeq",
        "tableName",
        Collections.singletonList(
            new Mod(
                "{\"accountId\": \"Id1\"}",
                "{}",
                "{\"accountName\": \"abc\", \"migration_shard_id\": \"" + shardId + "\"}")),
        ModType.valueOf("INSERT"),
        1,
        "");
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
    spSchema.put(
        "t1",
        new SpannerTable(
            "tableName",
            new String[] {"c1", "c2", "c3"},
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
    spannerToId.put("tableName", new NameAndCols("t1", t1ColIds));
    return spannerToId;
  }
}
