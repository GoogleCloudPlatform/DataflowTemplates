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

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.ReadOnlyTransaction;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.TimestampBound;
import com.google.cloud.spanner.Value;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.io.gcp.spanner.SpannerAccessor;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.Mod;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.ModType;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.DoFn;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

/** Tests for AssignShardIdFnTest class. */
public class AssignShardIdFnTest {
  @Rule public final transient TestPipeline pipeline = TestPipeline.create();
  @Rule public final MockitoRule mocktio = MockitoJUnit.rule();

  @Mock private SpannerAccessor spannerAccessor;

  @Mock private DatabaseClient mockDatabaseClient;

  @Mock private ReadOnlyTransaction mockReadOnlyTransaction;

  @Mock private DoFn.ProcessContext processContext;

  Struct mockRow = mock(Struct.class);

  @Before
  public void setUp() {
    mockSpannerReadRow();
  }

  private void mockSpannerReadRow() {
    when(spannerAccessor.getDatabaseClient()).thenReturn(mockDatabaseClient);

    when(mockDatabaseClient.singleUse(any(TimestampBound.class)))
        .thenReturn(mockReadOnlyTransaction);

    when(mockRow.getValue("accountId")).thenReturn(Value.string("Id1"));
    when(mockRow.getValue("accountName")).thenReturn(Value.string("xyz"));
    when(mockRow.getValue("migration_shard_id")).thenReturn(Value.string("shard1"));
    when(mockRow.getValue("accountNumber")).thenReturn(Value.int64(1));

    // Mock readRow
    when(mockReadOnlyTransaction.readRow(eq("tableName"), any(Key.class), any(Iterable.class)))
        .thenReturn(mockRow);
  }

  @Test
  public void testGetRowAsMap() throws Exception {
    AssignShardIdFn assignShardIdFn =
        new AssignShardIdFn(
            SpannerConfig.create(),
            getSchemaObject(),
            getTestDdl(),
            Constants.SHARDING_MODE_MULTI_SHARD,
            "test",
            "skip",
            "",
            "",
            "");
    List<String> columns =
        List.of("accountId", "accountName", "migration_shard_id", "accountNumber");
    Map<String, Object> actual = assignShardIdFn.getRowAsMap(mockRow, columns, "tableName");
    Map<String, Object> expected = new HashMap<>();
    expected.put("accountId", "Id1");
    expected.put("accountName", "xyz");
    expected.put("migration_shard_id", "shard1");
    expected.put("accountNumber", 1L);
    assertEquals(actual, expected);
  }

  @Test(expected = Exception.class)
  public void cannotGetRowAsMap() throws Exception {
    AssignShardIdFn assignShardIdFn =
        new AssignShardIdFn(
            SpannerConfig.create(),
            getSchemaObject(),
            getTestDdl(),
            Constants.SHARDING_MODE_MULTI_SHARD,
            "test",
            "skip",
            "",
            "",
            "");
    List<String> columns =
        List.of("accountId", "accountName", "migration_shard_id", "accountNumber", "missingColumn");
    assignShardIdFn.getRowAsMap(mockRow, columns, "tableName");
  }

  @Test
  public void testProcessElementInsertModForMultiShard() throws Exception {
    TrimmedShardedDataChangeRecord record = getDeleteTrimmedDataChangeRecord("shard1");
    when(processContext.element()).thenReturn(record);
    AssignShardIdFn assignShardIdFn =
        new AssignShardIdFn(
            SpannerConfig.create(),
            getSchemaObject(),
            getTestDdl(),
            Constants.SHARDING_MODE_MULTI_SHARD,
            "test",
            "skip",
            "",
            "",
            "");

    record.setShard("shard1");
    assignShardIdFn.setSpannerAccessor(spannerAccessor);
    ObjectMapper mapper = new ObjectMapper();
    mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    assignShardIdFn.setMapper(mapper);
    assignShardIdFn.setShardIdFetcher(assignShardIdFn.getShardIdFetcherImpl("", ""));

    assignShardIdFn.processElement(processContext);
    verify(processContext).output(eq(record));
  }

  @Test
  public void testProcessElementDeleteModForMultiShard() throws Exception {
    TrimmedShardedDataChangeRecord record = getDeleteTrimmedDataChangeRecord("shard1");
    when(processContext.element()).thenReturn(record);
    AssignShardIdFn assignShardIdFn =
        new AssignShardIdFn(
            SpannerConfig.create(),
            getSchemaObject(),
            getTestDdl(),
            Constants.SHARDING_MODE_MULTI_SHARD,
            "test",
            "skip",
            "",
            "",
            "");

    record.setShard("shard1");
    assignShardIdFn.setSpannerAccessor(spannerAccessor);
    ObjectMapper mapper = new ObjectMapper();
    mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    assignShardIdFn.setMapper(mapper);
    assignShardIdFn.setShardIdFetcher(assignShardIdFn.getShardIdFetcherImpl("", ""));

    assignShardIdFn.processElement(processContext);
    verify(processContext).output(eq(record));
  }

  @Test
  public void testProcessElementForSingleShard() throws Exception {
    TrimmedShardedDataChangeRecord record = getInsertTrimmedDataChangeRecord("shard1");
    when(processContext.element()).thenReturn(record);
    AssignShardIdFn assignShardIdFn =
        new AssignShardIdFn(
            SpannerConfig.create(),
            getSchemaObject(),
            getTestDdl(),
            Constants.SHARDING_MODE_SINGLE_SHARD,
            "test",
            "skip",
            "",
            "",
            "");

    record.setShard("test");

    assignShardIdFn.processElement(processContext);
    verify(processContext).output(eq(record));
  }

  @Test(expected = RuntimeException.class)
  public void testGetShardIdFetcherImplWithIncorrectCustomJarPath() throws Exception {
    TrimmedShardedDataChangeRecord record = getInsertTrimmedDataChangeRecord("shard1");
    when(processContext.element()).thenReturn(record);
    String customJarPath = "src/test/resources/custom-shard-fetcher.jar";
    String shardingCustomClassName = "com.custom.CustomShardIdFetcher";
    AssignShardIdFn assignShardIdFn =
        new AssignShardIdFn(
            SpannerConfig.create(),
            getSchemaObject(),
            getTestDdl(),
            Constants.SHARDING_MODE_MULTI_SHARD,
            "test",
            "skip",
            customJarPath,
            shardingCustomClassName,
            "");
    assignShardIdFn.getShardIdFetcherImpl(customJarPath, shardingCustomClassName);
  }

  public TrimmedShardedDataChangeRecord getInsertTrimmedDataChangeRecord(String shardId) {
    return new TrimmedShardedDataChangeRecord(
        Timestamp.parseTimestamp("2020-12-01T10:15:30.000Z"),
        "serverTxnId",
        "recordSeq",
        "tableName",
        Collections.singletonList(
            new Mod(
                "{\"accountId\": \"Id1\"}",
                "{}",
                "{\"accountName\": \"abc\", \"migration_shard_id\": \""
                    + shardId
                    + "\", \"accountNumber\": 1}")),
        ModType.valueOf("INSERT"),
        1,
        "");
  }

  public TrimmedShardedDataChangeRecord getDeleteTrimmedDataChangeRecord(String shardId) {
    return new TrimmedShardedDataChangeRecord(
        Timestamp.parseTimestamp("2020-12-01T10:15:30.000Z"),
        "serverTxnId",
        "recordSeq",
        "tableName",
        Collections.singletonList(new Mod("{\"accountId\": \"Id1\"}", "{}", "{}")),
        ModType.valueOf("DELETE"),
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
}
