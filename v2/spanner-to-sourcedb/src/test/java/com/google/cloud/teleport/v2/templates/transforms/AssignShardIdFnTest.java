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
package com.google.cloud.teleport.v2.templates.transforms;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.ByteArray;
import com.google.cloud.Date;
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
import com.google.cloud.teleport.v2.templates.changestream.TrimmedShardedDataChangeRecord;
import com.google.cloud.teleport.v2.templates.constants.Constants;
import com.google.cloud.teleport.v2.templates.utils.ShardIdFetcherImpl;
import com.google.cloud.teleport.v2.templates.utils.ShardingLogicImplFetcher;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.io.gcp.spanner.SpannerAccessor;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.Mod;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.ModType;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

/** Tests for AssignShardIdFnTest class. */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
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

    doNothing().when(spannerAccessor).close();
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
            "",
            10000L,
            "mysql",
            1000L);
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
            "",
            10000L,
            "mysql",
            1000L);
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
            "",
            10000L,
            "mysql",
            1000L);

    record.setShard("shard1");
    assignShardIdFn.setSpannerAccessor(spannerAccessor);
    ObjectMapper mapper = new ObjectMapper();
    mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    assignShardIdFn.setMapper(mapper);
    assignShardIdFn.setShardIdFetcher(
        ShardingLogicImplFetcher.getShardingLogicImpl("", "", "", getSchemaObject(), "skip"));

    assignShardIdFn.processElement(processContext);
    String keyStr = "tableName" + "_" + record.getMod().getKeysJson() + "_" + "shard1";
    Long key = keyStr.hashCode() % 10000L;
    assignShardIdFn.processElement(processContext);
    verify(processContext, atLeast(1)).output(eq(KV.of(key, record)));
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
            "",
            10000L,
            "mysql",
            1000L);

    record.setShard("shard1");
    assignShardIdFn.setSpannerAccessor(spannerAccessor);
    ObjectMapper mapper = new ObjectMapper();
    mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    assignShardIdFn.setMapper(mapper);
    assignShardIdFn.setShardIdFetcher(
        ShardingLogicImplFetcher.getShardingLogicImpl("", "", "", getSchemaObject(), "skip"));

    assignShardIdFn.processElement(processContext);
    String keyStr = "tableName" + "_" + record.getMod().getKeysJson() + "_" + "shard1";
    Long key = keyStr.hashCode() % 10000L;

    verify(processContext, atLeast(1)).output(eq(KV.of(key, record)));
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
            "",
            10000L,
            "mysql",
            1000L);

    record.setShard("test");
    String keyStr = "tableName" + "_" + record.getMod().getKeysJson() + "_" + "test";
    Long key = keyStr.hashCode() % 10000L;
    assignShardIdFn.processElement(processContext);
    verify(processContext, atLeast(1)).output(eq(KV.of(key, record)));
  }

  @Test(expected = RuntimeException.class)
  public void testGetShardIdFetcherImplWithIncorrectCustomJarPath() throws Exception {
    TrimmedShardedDataChangeRecord record = getInsertTrimmedDataChangeRecord("shard1");
    when(processContext.element()).thenReturn(record);
    String customJarPath = "src/test/resources/custom-shard-fetcher.jar";
    String shardingCustomClassName = "com.test.CustomShardIdFetcher";
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
            "",
            10000L,
            "mysql",
            1000L);
    assignShardIdFn.setShardIdFetcher(
        ShardingLogicImplFetcher.getShardingLogicImpl(
            customJarPath, shardingCustomClassName, "", getSchemaObject(), "skip"));
  }

  @Test
  public void testProcessElementDeleteAllDatatypes() throws Exception {
    TrimmedShardedDataChangeRecord record = getDeleteTrimmedDataChangeRecordAllDatatypes("shard1");
    when(processContext.element()).thenReturn(record);
    // All datatypes row
    ByteArray bytesArray = ByteArray.copyFrom("abc");
    Struct allDatatypesRow =
        Struct.newBuilder()
            .set("first_name")
            .to("Id1")
            .set("migration_shard_id")
            .to("shard1")
            .set("age")
            .to(new BigDecimal(1))
            .set("bool_field")
            .to(true)
            .set("int64_field")
            .to(1)
            .set("float64_field")
            .to(4.2)
            .set("string_field")
            .to("abc")
            .set("bytes_field")
            .to(bytesArray)
            .set("timestamp_field")
            .to(Timestamp.parseTimestamp("2023-05-18T12:01:13.088397258Z"))
            .set("date_field")
            .to(Date.parseDate("2020-12-30"))
            .set("json_field")
            .to("{\"a\": \"b\"}")
            .set("timestamp_field2")
            .to(Timestamp.parseTimestamp("2023-05-18T12:01:13.088397258Z"))
            .set("date_field2")
            .to(Date.parseDate("2020-12-30"))
            .build();
    when(mockReadOnlyTransaction.readRow(eq("Users"), any(Key.class), any(Iterable.class)))
        .thenReturn(allDatatypesRow);
    AssignShardIdFn assignShardIdFn =
        new AssignShardIdFn(
            SpannerConfig.create(),
            getSchemaObjectAllDatatypes(),
            getTestDdlForPrimaryKeyTest(),
            Constants.SHARDING_MODE_MULTI_SHARD,
            "test",
            "skip",
            "",
            "",
            "",
            10000L,
            "mysql",
            1000L);

    record.setShard("shard1");
    assignShardIdFn.setSpannerAccessor(spannerAccessor);
    ObjectMapper mapper = new ObjectMapper();
    mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    assignShardIdFn.setMapper(mapper);
    ShardIdFetcherImpl shardIdFetcher =
        new ShardIdFetcherImpl(getSchemaObjectAllDatatypes(), "skip");
    shardIdFetcher.init("just to test this method is called argghhh!!");
    assignShardIdFn.setShardIdFetcher(shardIdFetcher);

    assignShardIdFn.processElement(processContext);
    String keyStr = record.getTableName() + "_" + record.getMod().getKeysJson() + "_" + "shard1";
    Long key = keyStr.hashCode() % 10000L;

    verify(processContext, atLeast(1)).output(eq(KV.of(key, record)));
  }

  @Test
  public void testProcessElementInsertAllDatatypes() throws Exception {
    TrimmedShardedDataChangeRecord record = getInsertTrimmedDataChangeRecordAllDatatypes("shard1");
    when(processContext.element()).thenReturn(record);
    AssignShardIdFn assignShardIdFn =
        new AssignShardIdFn(
            SpannerConfig.create(),
            getSchemaObjectAllDatatypes(),
            getTestDdlForPrimaryKeyTest(),
            Constants.SHARDING_MODE_MULTI_SHARD,
            "test",
            "skip",
            "",
            "",
            "",
            10000L,
            "mysql",
            1000L);

    record.setShard("shard1");
    assignShardIdFn.setSpannerAccessor(spannerAccessor);
    ObjectMapper mapper = new ObjectMapper();
    mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    assignShardIdFn.setMapper(mapper);
    ShardIdFetcherImpl shardIdFetcher =
        new ShardIdFetcherImpl(getSchemaObjectAllDatatypes(), "skip");
    assignShardIdFn.setShardIdFetcher(shardIdFetcher);

    assignShardIdFn.processElement(processContext);
    String keyStr = record.getTableName() + "_" + record.getMod().getKeysJson() + "_" + "shard1";
    Long key = keyStr.hashCode() % 10000L;
    assignShardIdFn.teardown();
    verify(processContext, atLeast(1)).output(eq(KV.of(key, record)));
  }

  @Test
  public void testSkippedShardForTableNotInSchema() throws Exception {
    TrimmedShardedDataChangeRecord record = getInsertTrimmedDataChangeRecord("shard1");
    when(processContext.element()).thenReturn(record);

    AssignShardIdFn assignShardIdFn =
        new AssignShardIdFn(
            SpannerConfig.create(),
            getSchemaObjectAllDatatypes(),
            getTestDdlForPrimaryKeyTest(),
            Constants.SHARDING_MODE_MULTI_SHARD,
            "test",
            "skip",
            "",
            "",
            "",
            10000L,
            "mysql",
            1000L);
    String keyStr = "tableName" + "_" + record.getMod().getKeysJson() + "_" + "skip";
    Long key = keyStr.hashCode() % 10000L;
    record.setShard("skip");
    ShardIdFetcherImpl shardIdFetcher =
        new ShardIdFetcherImpl(getSchemaObjectAllDatatypes(), "skip");
    assignShardIdFn.setShardIdFetcher(shardIdFetcher);
    assignShardIdFn.processElement(processContext);
    verify(processContext, atLeast(1)).output(eq(KV.of(key, record)));
  }

  @Test
  public void testNoShardForIncorrectShardColumn() throws Exception {
    TrimmedShardedDataChangeRecord record = getInsertTrimmedDataChangeRecord("shard1");
    when(processContext.element()).thenReturn(record);

    AssignShardIdFn assignShardIdFn =
        new AssignShardIdFn(
            SpannerConfig.create(),
            getSchemaObject(),
            getTestDdlForPrimaryKeyTest(),
            Constants.SHARDING_MODE_MULTI_SHARD,
            "test",
            "skip",
            "",
            "",
            "",
            10000L,
            "mysql",
            1000L);
    String keyStr = "tableName" + "_" + record.getMod().getKeysJson() + "_" + "skip";
    Long key = keyStr.hashCode() % 10000L;
    ShardIdFetcherImpl shardIdFetcher =
        new ShardIdFetcherImpl(getBotchedSchemaObjectForMissingShardColumn(), "skip");
    assignShardIdFn.setShardIdFetcher(shardIdFetcher);
    assignShardIdFn.processElement(processContext);
    verify(processContext, atLeast(1)).output(eq(KV.of(key, record)));
  }

  @Test
  public void testNoShardForIncorrectSpToOid() throws Exception {
    TrimmedShardedDataChangeRecord record = getInsertTrimmedDataChangeRecord("shard1");
    when(processContext.element()).thenReturn(record);

    AssignShardIdFn assignShardIdFn =
        new AssignShardIdFn(
            SpannerConfig.create(),
            getSchemaObject(),
            getTestDdlForPrimaryKeyTest(),
            Constants.SHARDING_MODE_MULTI_SHARD,
            "test",
            "skip",
            "",
            "",
            "",
            10000L,
            "mysql",
            1000L);
    String keyStr = "tableName" + "_" + record.getMod().getKeysJson() + "_" + "skip";
    Long key = keyStr.hashCode() % 10000L;
    ShardIdFetcherImpl shardIdFetcher =
        new ShardIdFetcherImpl(getBotchedSchemaObjectForInvalidSpannerToOid(), "skip");
    assignShardIdFn.setShardIdFetcher(shardIdFetcher);
    assignShardIdFn.processElement(processContext);
    verify(processContext, atLeast(1)).output(eq(KV.of(key, record)));
  }

  @Test
  public void testNoShardForIncorrectSpSchema() throws Exception {
    TrimmedShardedDataChangeRecord record = getInsertTrimmedDataChangeRecord("shard1");
    when(processContext.element()).thenReturn(record);

    AssignShardIdFn assignShardIdFn =
        new AssignShardIdFn(
            SpannerConfig.create(),
            getSchemaObject(),
            getTestDdlForPrimaryKeyTest(),
            Constants.SHARDING_MODE_MULTI_SHARD,
            "test",
            "skip",
            "",
            "",
            "",
            10000L,
            "mysql",
            1000L);
    String keyStr = "tableName" + "_" + record.getMod().getKeysJson() + "_" + "skip";
    Long key = keyStr.hashCode() % 10000L;
    ShardIdFetcherImpl shardIdFetcher =
        new ShardIdFetcherImpl(getBotchedSchemaObjectForInvalidSpSchema(), "skip");
    assignShardIdFn.setShardIdFetcher(shardIdFetcher);
    assignShardIdFn.processElement(processContext);
    verify(processContext, atLeast(1)).output(eq(KV.of(key, record)));
  }

  @Test()
  public void testInvalidShard() throws Exception {
    TrimmedShardedDataChangeRecord record = getInsertTrimmedDataChangeRecord("shard1/");
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
            "",
            10000L,
            "mysql",
            1000L);

    record.setShard("shard1");
    assignShardIdFn.setSpannerAccessor(spannerAccessor);
    ObjectMapper mapper = new ObjectMapper();
    mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    assignShardIdFn.setMapper(mapper);
    assignShardIdFn.setShardIdFetcher(
        ShardingLogicImplFetcher.getShardingLogicImpl("", "", "", getSchemaObject(), "skip"));

    assignShardIdFn.processElement(processContext);
    String keyStr = "tableName" + "_" + record.getMod().getKeysJson() + "_" + "skip";
    Long key = keyStr.hashCode() % 10000L;
    assignShardIdFn.processElement(processContext);
    verify(processContext, atLeast(1)).output(eq(KV.of(key, record)));
  }

  @Test
  public void testProcessElementDeleteNoSpannerRow() throws Exception {
    TrimmedShardedDataChangeRecord record = getDeleteTrimmedDataChangeRecordAllDatatypes("shard1");
    when(processContext.element()).thenReturn(record);
    // All datatypes row
    ByteArray bytesArray = ByteArray.copyFrom("abc");

    when(mockReadOnlyTransaction.readRow(eq("Users"), any(Key.class), any(Iterable.class)))
        .thenReturn(null);
    AssignShardIdFn assignShardIdFn =
        new AssignShardIdFn(
            SpannerConfig.create(),
            getSchemaObjectAllDatatypes(),
            getTestDdlForPrimaryKeyTest(),
            Constants.SHARDING_MODE_MULTI_SHARD,
            "test",
            "skip",
            "",
            "",
            "",
            10000L,
            "mysql",
            1000L);

    record.setShard("shard1");
    assignShardIdFn.setSpannerAccessor(spannerAccessor);
    ObjectMapper mapper = new ObjectMapper();
    mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    assignShardIdFn.setMapper(mapper);
    ShardIdFetcherImpl shardIdFetcher =
        new ShardIdFetcherImpl(getSchemaObjectAllDatatypes(), "skip");
    assignShardIdFn.setShardIdFetcher(shardIdFetcher);

    assignShardIdFn.processElement(processContext);
    String keyStr = record.getTableName() + "_" + record.getMod().getKeysJson() + "_" + "skip";
    Long key = keyStr.hashCode() % 10000L;

    verify(processContext, atLeast(1)).output(eq(KV.of(key, record)));
  }

  public TrimmedShardedDataChangeRecord getInsertTrimmedDataChangeRecord(String shardId) {
    return new TrimmedShardedDataChangeRecord(
        Timestamp.parseTimestamp("2020-12-01T10:15:30.000Z"),
        "serverTxnId",
        "recordSeq",
        "tableName",
        new Mod(
            "{\"accountId\": \"Id1\"}",
            "{}",
            "{\"accountName\": \"abc\", \"migration_shard_id\": \""
                + shardId
                + "\", \"accountNumber\": 1}"),
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
        new Mod("{\"accountId\": \"Id1\"}", "{}", "{}"),
        ModType.valueOf("DELETE"),
        1,
        "");
  }

  public TrimmedShardedDataChangeRecord getInsertTrimmedDataChangeRecordAllDatatypes(
      String shardId) {
    return new TrimmedShardedDataChangeRecord(
        Timestamp.parseTimestamp("2020-12-01T10:15:30.000Z"),
        "serverTxnId",
        "recordSeq",
        "Users",
        new Mod(
            "{\"first_name\": \"Id1\", \"migration_shard_id\": \""
                + shardId
                + "\", \"age\": \"1\", \"bool_field\":true, \"int64_field\": \"1\","
                + " \"float64_field\": 4.2, \"string_field\": \"abc\", \"bytes_field\": \"abc\","
                + " \"timestamp_field\": \"2023-05-18T12:01:13.088397258Z\", \"date_field\":"
                + " \"2023-05-18\"}",
            "{}",
            "{ \"timestamp_field2\": \"2023-05-18T12:01:13.088397258Z\", \"date_field2\":"
                + " \"2023-05-18\", \"json_field\": \"{\\\"a\\\": \\\"b\\\"}\"}"),
        ModType.valueOf("INSERT"),
        1,
        "");
  }

  public TrimmedShardedDataChangeRecord getDeleteTrimmedDataChangeRecordAllDatatypes(
      String shardId) {
    return new TrimmedShardedDataChangeRecord(
        Timestamp.parseTimestamp("2020-12-01T10:15:30.000Z"),
        "serverTxnId",
        "recordSeq",
        "Users",
        new Mod(
            "{\"first_name\": \"Id1\", \"migration_shard_id\": \""
                + shardId
                + "\", \"age\": \"1\", \"bool_field\":true, \"int64_field\": \"1\","
                + " \"float64_field\": 4.2, \"string_field\": \"abc\", \"bytes_field\": \"abc\","
                + " \"timestamp_field\": \"2023-05-18T12:01:13.088397258Z\", \"date_field\":"
                + " \"2023-05-18\"}",
            "{}",
            "{}"),
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

  static Ddl getTestDdlForPrimaryKeyTest() {

    Ddl ddl =
        Ddl.builder()
            .createTable("Users")
            .column("first_name")
            .string()
            .max()
            .endColumn()
            .column("migration_shard_id")
            .string()
            .size(50)
            .endColumn()
            .column("age")
            .numeric()
            .endColumn()
            .column("bool_field")
            .bool()
            .endColumn()
            .column("int64_field")
            .int64()
            .endColumn()
            .column("float64_field")
            .float64()
            .endColumn()
            .column("string_field")
            .string()
            .max()
            .endColumn()
            .column("json_field")
            .json()
            .endColumn()
            .column("bytes_field")
            .bytes()
            .max()
            .endColumn()
            .column("timestamp_field")
            .timestamp()
            .endColumn()
            .column("timestamp_field2")
            .timestamp()
            .endColumn()
            .column("date_field")
            .date()
            .endColumn()
            .column("date_field2")
            .date()
            .endColumn()
            .primaryKey()
            .asc("first_name")
            .desc("migration_shard_id")
            .asc("age")
            .asc("bool_field")
            .asc("int64_field")
            .asc("float64_field")
            .asc("string_field")
            .asc("bytes_field")
            .asc("timestamp_field")
            .asc("date_field")
            .end()
            .endTable()
            .build();
    return ddl;
  }

  public static Schema getSchemaObjectAllDatatypes() {
    Map<String, SyntheticPKey> syntheticPKeys = new HashMap<String, SyntheticPKey>();
    Map<String, SourceTable> srcSchema = new HashMap<String, SourceTable>();
    Map<String, SpannerTable> spSchema = getSampleSpSchemaAllDatatypes();
    Map<String, NameAndCols> spannerToID = getSampleSpannerToIdAllDatatypes();
    Schema expectedSchema = new Schema(spSchema, syntheticPKeys, srcSchema);
    expectedSchema.setSpannerToID(spannerToID);
    return expectedSchema;
  }

  public static Map<String, SpannerTable> getSampleSpSchemaAllDatatypes() {
    Map<String, SpannerTable> spSchema = new HashMap<String, SpannerTable>();
    Map<String, SpannerColumnDefinition> t1SpColDefs =
        new HashMap<String, SpannerColumnDefinition>();
    t1SpColDefs.put(
        "c1", new SpannerColumnDefinition("first_name", new SpannerColumnType("STRING", false)));
    t1SpColDefs.put(
        "c2", new SpannerColumnDefinition("age", new SpannerColumnType("NUMERIC", false)));
    t1SpColDefs.put(
        "c3", new SpannerColumnDefinition("bool_field", new SpannerColumnType("BOOL", false)));
    t1SpColDefs.put(
        "c4", new SpannerColumnDefinition("int64_field", new SpannerColumnType("INT64", false)));

    t1SpColDefs.put(
        "c5",
        new SpannerColumnDefinition("float64_field", new SpannerColumnType("FLOAT64", false)));
    t1SpColDefs.put(
        "c6", new SpannerColumnDefinition("string_field", new SpannerColumnType("STRING", false)));
    t1SpColDefs.put(
        "c7", new SpannerColumnDefinition("json_field", new SpannerColumnType("JSON", false)));
    t1SpColDefs.put(
        "c8", new SpannerColumnDefinition("bytes_field", new SpannerColumnType("BYTES", false)));
    t1SpColDefs.put(
        "c9",
        new SpannerColumnDefinition("timestamp_field", new SpannerColumnType("TIMESTAMP", false)));
    t1SpColDefs.put(
        "c10",
        new SpannerColumnDefinition("timestamp_field2", new SpannerColumnType("TIMESTAMP", false)));
    t1SpColDefs.put(
        "c11", new SpannerColumnDefinition("date_field", new SpannerColumnType("DATE", false)));
    t1SpColDefs.put(
        "c12", new SpannerColumnDefinition("date_field2", new SpannerColumnType("DATE", false)));
    t1SpColDefs.put(
        "c13",
        new SpannerColumnDefinition("migration_shard_id", new SpannerColumnType("STRING", false)));

    spSchema.put(
        "t1",
        new SpannerTable(
            "Users",
            new String[] {
              "c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "c10", "c11", "c12", "c13"
            },
            t1SpColDefs,
            new ColumnPK[] {
              new ColumnPK("c1", 1),
              new ColumnPK("c13", 2),
              new ColumnPK("c2", 3),
              new ColumnPK("c3", 4),
              new ColumnPK("c4", 5),
              new ColumnPK("c5", 6),
              new ColumnPK("c6", 7),
              new ColumnPK("c8", 8),
              new ColumnPK("c9", 9),
              new ColumnPK("c11", 10),
            },
            "c13"));
    return spSchema;
  }

  public static Map<String, NameAndCols> getSampleSpannerToIdAllDatatypes() {
    Map<String, NameAndCols> spannerToId = new HashMap<String, NameAndCols>();
    Map<String, String> t1ColIds = new HashMap<String, String>();
    t1ColIds.put("first_name", "c1");
    t1ColIds.put("age", "c2");
    t1ColIds.put("bool_field", "c3");
    t1ColIds.put("int64_field", "c4");
    t1ColIds.put("float64_field", "c5");
    t1ColIds.put("string_field", "c6");
    t1ColIds.put("json_field", "c7");
    t1ColIds.put("bytes_field", "c8");
    t1ColIds.put("timestamp_field", "c9");
    t1ColIds.put("timestamp_field2", "c10");
    t1ColIds.put("date_field", "c11");
    t1ColIds.put("date_field2", "c12");
    t1ColIds.put("migration_shard_id", "c13");

    spannerToId.put("Users", new NameAndCols("t1", t1ColIds));
    return spannerToId;
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
