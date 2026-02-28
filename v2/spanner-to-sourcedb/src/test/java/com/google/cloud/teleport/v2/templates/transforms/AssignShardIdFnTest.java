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
import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.Options.ReadOption;
import com.google.cloud.spanner.ReadOnlyTransaction;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.TimestampBound;
import com.google.cloud.spanner.Value;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.migrations.schema.ISchemaMapper;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SessionBasedMapper;
import com.google.cloud.teleport.v2.spanner.sourceddl.SourceDatabaseType;
import com.google.cloud.teleport.v2.spanner.sourceddl.SourceSchema;
import com.google.cloud.teleport.v2.spanner.sourceddl.SourceTable;
import com.google.cloud.teleport.v2.templates.SpannerToSourceDb.Options;
import com.google.cloud.teleport.v2.templates.changestream.TrimmedShardedDataChangeRecord;
import com.google.cloud.teleport.v2.templates.constants.Constants;
import com.google.cloud.teleport.v2.templates.utils.SchemaUtils;
import com.google.cloud.teleport.v2.templates.utils.ShardingLogicImplFetcher;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
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
import org.apache.beam.sdk.values.PCollectionView;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.mockito.ArgumentCaptor;
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

  @Mock private Options mockOptions;

  @Mock private PCollectionView<Ddl> mockDdlView;

  Struct mockRow = mock(Struct.class);

  private static final String SESSION_FILE_PATH =
      "src/test/resources/AssignShardIdTestSession.json";
  private static final String ALL_TYPES_SESSION_FILE_PATH =
      "src/test/resources/AssignShardIdTestAllDataTypesSession.json";

  @Before
  public void setUp() {
    mockSpannerReadRow();
    when(processContext.getPipelineOptions()).thenReturn(mockOptions);
    ShardingLogicImplFetcher.reset();
  }

  private void mockSpannerReadRow() {
    when(spannerAccessor.getDatabaseClient()).thenReturn(mockDatabaseClient);

    when(mockDatabaseClient.singleUse(any(TimestampBound.class)))
        .thenReturn(mockReadOnlyTransaction);

    when(mockRow.getValue("accountId")).thenReturn(Value.string("Id1"));
    when(mockRow.getValue("accountName")).thenReturn(Value.string("xyz"));
    when(mockRow.getValue("migration_shard_id")).thenReturn(Value.string("shard1"));
    when(mockRow.isNull("string_col_null")).thenReturn(true);
    when(mockRow.getValue("accountNumber")).thenReturn(Value.int64(1));
    when(mockRow.isNull("int_64_col_null")).thenReturn(true);
    when(mockRow.getValue("bytesCol"))
        .thenReturn(Value.bytes(ByteArray.copyFrom("GOOGLE".getBytes())));
    when(mockRow.isNull("bytes_col_null")).thenReturn(true);
    when(mockRow.getDouble("float_64_col")).thenReturn(0.5);
    when(mockRow.getValue("float_64_col")).thenReturn(Value.float64(0.5));
    when(mockRow.getDouble("float_64_col_nan")).thenReturn(Double.NaN);
    when(mockRow.getValue("float_64_col_nan")).thenReturn(Value.float64(Double.NaN));
    when(mockRow.getDouble("float_64_col_infinity")).thenReturn(Double.POSITIVE_INFINITY);
    when(mockRow.getValue("float_64_col_infinity"))
        .thenReturn(Value.float64(Double.POSITIVE_INFINITY));
    when(mockRow.getDouble("float_64_col_neg_infinity")).thenReturn(Double.NEGATIVE_INFINITY);
    when(mockRow.getValue("float_64_col_neg_infinity"))
        .thenReturn(Value.float64(Double.NEGATIVE_INFINITY));
    when(mockRow.isNull("float_64_col_null")).thenReturn(true);
    when(mockRow.getValue("float_32_col")).thenReturn(Value.float32(0.5f));
    when(mockRow.getFloat("float_32_col")).thenReturn(0.5f);
    when(mockRow.getFloat("float_32_col_nan")).thenReturn(Float.NaN);
    when(mockRow.getValue("float_32_col_nan")).thenReturn(Value.float32(Float.NaN));
    when(mockRow.getFloat("float_32_col_infinity")).thenReturn(Float.POSITIVE_INFINITY);
    when(mockRow.getValue("float_32_col_infinity"))
        .thenReturn(Value.float32(Float.POSITIVE_INFINITY));
    when(mockRow.getFloat("float_32_col_neg_infinity")).thenReturn(Float.NEGATIVE_INFINITY);
    when(mockRow.getValue("float_32_col_neg_infinity"))
        .thenReturn(Value.float32(Float.NEGATIVE_INFINITY));
    when(mockRow.isNull("float_32_col_null")).thenReturn(true);
    when(mockRow.getBoolean("bool_col")).thenReturn(true);
    when(mockRow.getValue("bool_col")).thenReturn(Value.bool(true));
    when(mockRow.isNull("bool_col_null")).thenReturn(true);
    when(mockRow.isNull("timestamp_col_null")).thenReturn(true);

    // Mock readRow
    when(mockReadOnlyTransaction.readRow(eq("tableName"), any(Key.class), any(Iterable.class)))
        .thenReturn(mockRow);

    doNothing().when(spannerAccessor).close();
  }

  @Test
  public void testGetRowAsMap() throws Exception {
    Ddl ddl = SchemaUtils.buildSpannerDdlFromSessionFile(SESSION_FILE_PATH);
    SourceSchema sourceSchema = SchemaUtils.buildSourceSchemaFromSessionFile(SESSION_FILE_PATH);

    AssignShardIdFn assignShardIdFn =
        new AssignShardIdFn(
            SpannerConfig.create(),
            mockDdlView,
            sourceSchema,
            Constants.SHARDING_MODE_MULTI_SHARD,
            "test",
            "skip",
            "",
            "",
            "",
            10000L,
            Constants.SOURCE_MYSQL,
            SESSION_FILE_PATH,
            "",
            "",
            "");
    List<String> columns =
        List.of("accountId", "accountName", "migration_shard_id", "accountNumber");
    Map<String, Object> actual = assignShardIdFn.getRowAsMap(mockRow, columns, "tableName", ddl);
    Map<String, Object> expected = new HashMap<>();
    expected.put("accountId", "Id1");
    expected.put("accountName", "xyz");
    expected.put("migration_shard_id", "shard1");
    expected.put("accountNumber", 1L);
    assertEquals(actual, expected);
  }

  @Test(expected = Exception.class)
  public void cannotGetRowAsMap() throws Exception {
    Ddl ddl = SchemaUtils.buildSpannerDdlFromSessionFile(SESSION_FILE_PATH);
    SourceSchema sourceSchema = SchemaUtils.buildSourceSchemaFromSessionFile(SESSION_FILE_PATH);

    AssignShardIdFn assignShardIdFn =
        new AssignShardIdFn(
            SpannerConfig.create(),
            mockDdlView,
            sourceSchema,
            Constants.SHARDING_MODE_MULTI_SHARD,
            "test",
            "skip",
            "",
            "",
            "",
            10000L,
            Constants.SOURCE_MYSQL,
            SESSION_FILE_PATH,
            "",
            "",
            "");
    List<String> columns =
        List.of("accountId", "accountName", "migration_shard_id", "accountNumber", "missingColumn");

    assignShardIdFn.getRowAsMap(mockRow, columns, "tableName", ddl);
  }

  @Test
  public void testProcessElementInsertModForMultiShard() throws Exception {
    TrimmedShardedDataChangeRecord record = getDeleteTrimmedDataChangeRecord("shard1");
    when(processContext.element()).thenReturn(record);

    Ddl ddl = SchemaUtils.buildSpannerDdlFromSessionFile(SESSION_FILE_PATH);
    SourceSchema sourceSchema = SchemaUtils.buildSourceSchemaFromSessionFile(SESSION_FILE_PATH);

    // Prepare mock for c.sideInput(ddlView)
    when(processContext.sideInput(mockDdlView)).thenReturn(ddl);
    when(mockOptions.getSessionFilePath()).thenReturn(SESSION_FILE_PATH);
    when(mockOptions.getTableOverrides()).thenReturn("");
    when(mockOptions.getColumnOverrides()).thenReturn("");
    when(mockOptions.getSchemaOverridesFilePath()).thenReturn("");

    com.google.cloud.spanner.ResultSet resultSet = mock(ResultSet.class);
    when(mockReadOnlyTransaction.read(
            eq("tableName"), any(KeySet.class), any(Iterable.class), any(ReadOption.class)))
        .thenReturn(resultSet);
    when(resultSet.next()).thenReturn(true);
    when(resultSet.getCurrentRowAsStruct()).thenReturn(mockRow);

    AssignShardIdFn assignShardIdFn =
        new AssignShardIdFn(
            SpannerConfig.create(),
            mockDdlView,
            sourceSchema,
            Constants.SHARDING_MODE_MULTI_SHARD,
            "test",
            "skip",
            "",
            "",
            "",
            10000L,
            Constants.SOURCE_MYSQL,
            SESSION_FILE_PATH,
            "",
            "",
            "");

    record.setShard("shard1");
    assignShardIdFn.setSpannerAccessor(spannerAccessor);
    ObjectMapper mapper = new ObjectMapper();
    mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    assignShardIdFn.setMapper(mapper);

    assignShardIdFn.processElement(processContext);
    String keyStr = "tableName" + "_" + record.getMod().getKeysJson() + "_" + "shard1";
    Long key = keyStr.hashCode() % 10000L;
    assignShardIdFn.processElement(processContext);

    String newValuesJson =
        "{\"bool_col_null\":null,\"float_32_col_nan\":\"NaN\",\"bool_col\":true,\"timestamp_col_null\":null,\"accountName\":\"xyz\",\"float_64_col_neg_infinity\":\"-Infinity\",\"float_32_col_neg_infinity\":\"-Infinity\",\"bytes_col_null\":null,\"string_col_null\":null,\"accountNumber\":\"1\",\"bytesCol\":\"R09PR0xF\",\"accountId\":\"Id1\",\"migration_shard_id\":\"shard1\",\"int_64_col_null\":null,\"float_64_col\":0.5,\"float_32_col_infinity\":\"Infinity\",\"float_32_col\":0.5,\"float_64_col_infinity\":\"Infinity\",\"float_64_col_null\":null,\"float_64_col_nan\":\"NaN\",\"float_32_col_null\":null}";

    record.setMod(
        new Mod(record.getMod().getKeysJson(), record.getMod().getOldValuesJson(), newValuesJson));
    verify(processContext, atLeast(1)).output(eq(KV.of(key, record)));
  }

  @Test
  public void testProcessElementDeleteModForMultiShard() throws Exception {
    TrimmedShardedDataChangeRecord record = getDeleteTrimmedDataChangeRecord("shard1");
    when(processContext.element()).thenReturn(record);

    Ddl ddl = SchemaUtils.buildSpannerDdlFromSessionFile(SESSION_FILE_PATH);
    SourceSchema sourceSchema = SchemaUtils.buildSourceSchemaFromSessionFile(SESSION_FILE_PATH);

    // Prepare mock for c.sideInput(ddlView)
    when(processContext.sideInput(mockDdlView)).thenReturn(ddl);

    com.google.cloud.spanner.ResultSet resultSet = mock(ResultSet.class);
    when(mockReadOnlyTransaction.read(
            eq("tableName"), any(KeySet.class), any(Iterable.class), any(ReadOption.class)))
        .thenReturn(resultSet);
    when(resultSet.next()).thenReturn(true);
    when(resultSet.getCurrentRowAsStruct()).thenReturn(mockRow);

    AssignShardIdFn assignShardIdFn =
        new AssignShardIdFn(
            SpannerConfig.create(),
            mockDdlView,
            sourceSchema,
            Constants.SHARDING_MODE_MULTI_SHARD,
            "test",
            "skip",
            "",
            "",
            "",
            10000L,
            Constants.SOURCE_MYSQL,
            SESSION_FILE_PATH,
            "",
            "",
            "");

    record.setShard("shard1");
    assignShardIdFn.setSpannerAccessor(spannerAccessor);
    ObjectMapper mapper = new ObjectMapper();
    mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    assignShardIdFn.setMapper(mapper);

    assignShardIdFn.processElement(processContext);
    String keyStr = "tableName" + "_" + record.getMod().getKeysJson() + "_" + "shard1";
    Long key = keyStr.hashCode() % 10000L;

    String newValuesJson =
        "{\"bool_col_null\":null,\"float_32_col_nan\":\"NaN\",\"bool_col\":true,\"timestamp_col_null\":null,\"accountName\":\"xyz\",\"float_64_col_neg_infinity\":\"-Infinity\",\"float_32_col_neg_infinity\":\"-Infinity\",\"bytes_col_null\":null,\"string_col_null\":null,\"accountNumber\":\"1\",\"bytesCol\":\"R09PR0xF\",\"accountId\":\"Id1\",\"migration_shard_id\":\"shard1\",\"int_64_col_null\":null,\"float_64_col\":0.5,\"float_32_col_infinity\":\"Infinity\",\"float_32_col\":0.5,\"float_64_col_infinity\":\"Infinity\",\"float_64_col_null\":null,\"float_64_col_nan\":\"NaN\",\"float_32_col_null\":null}";

    record.setMod(
        new Mod(record.getMod().getKeysJson(), record.getMod().getOldValuesJson(), newValuesJson));
    verify(processContext, atLeast(1)).output(eq(KV.of(key, record)));
  }

  @Test
  public void testProcessElementForSingleShard() throws Exception {
    TrimmedShardedDataChangeRecord record = getInsertTrimmedDataChangeRecord("shard1");
    when(processContext.element()).thenReturn(record);

    Ddl ddl = SchemaUtils.buildSpannerDdlFromSessionFile(SESSION_FILE_PATH);
    SourceSchema sourceSchema = SchemaUtils.buildSourceSchemaFromSessionFile(SESSION_FILE_PATH);

    // Prepare mock for c.sideInput(ddlView) - not strictly needed for single shard, but safe to set
    when(processContext.sideInput(mockDdlView)).thenReturn(ddl);

    AssignShardIdFn assignShardIdFn =
        new AssignShardIdFn(
            SpannerConfig.create(),
            mockDdlView,
            sourceSchema,
            Constants.SHARDING_MODE_SINGLE_SHARD,
            "shard1",
            "skip",
            "",
            "",
            "",
            10000L,
            Constants.SOURCE_MYSQL,
            SESSION_FILE_PATH,
            "",
            "",
            "");

    record.setShard("shard1");
    assignShardIdFn.setMapper(new ObjectMapper());
    String newValuesJson =
        "{\"accountId\":\"Id1\",\"migration_shard_id\":\"shard1\",\"float_64_col\":0,\"bool_col\":false,\"accountName\":\"xyz\",\"float_64_col_infinity\":0,\"float_64_col_neg_infinity\":0,\"accountNumber\":\"1\",\"float_64_col_nan\":0,\"bytesCol\":\"R09PR0xF\"}";
    record.setMod(
        new Mod(record.getMod().getKeysJson(), record.getMod().getOldValuesJson(), newValuesJson));
    String keyStr = "tableName" + "_" + record.getMod().getKeysJson() + "_" + "shard1";
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

    Ddl ddl = SchemaUtils.buildSpannerDdlFromSessionFile(SESSION_FILE_PATH);
    SourceSchema sourceSchema = SchemaUtils.buildSourceSchemaFromSessionFile(SESSION_FILE_PATH);
    ISchemaMapper schemaMapper = new SessionBasedMapper(SESSION_FILE_PATH, ddl);

    // We only need a valid constructor call for the runtime exception to be expected
    AssignShardIdFn assignShardIdFn =
        new AssignShardIdFn(
            SpannerConfig.create(),
            mockDdlView,
            sourceSchema,
            Constants.SHARDING_MODE_MULTI_SHARD,
            "test",
            "skip",
            customJarPath,
            shardingCustomClassName,
            "",
            10000L,
            Constants.SOURCE_MYSQL,
            SESSION_FILE_PATH,
            "",
            "",
            "");
    // The line below actually triggers the loading logic that throws the exception
    ShardingLogicImplFetcher.getShardingLogicImpl(
        customJarPath, shardingCustomClassName, "", schemaMapper, "skip");
  }

  @Test
  public void testProcessElementDeleteAllDatatypes() throws Exception {
    TrimmedShardedDataChangeRecord record = getDeleteTrimmedDataChangeRecordAllDatatypes("shard1");
    when(processContext.element()).thenReturn(record);
    // All datatypes row
    ByteArray bytesArray = ByteArray.copyFrom("abc");
    com.google.cloud.spanner.ResultSet resultSet = mock(ResultSet.class);

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

    when(mockReadOnlyTransaction.read(
            eq("Users"), any(KeySet.class), any(Iterable.class), any(ReadOption.class)))
        .thenReturn(resultSet);

    when(resultSet.next()).thenReturn(true);
    when(resultSet.getCurrentRowAsStruct()).thenReturn(allDatatypesRow);

    Ddl ddl = SchemaUtils.buildSpannerDdlFromSessionFile(ALL_TYPES_SESSION_FILE_PATH);
    SourceSchema sourceSchema =
        SchemaUtils.buildSourceSchemaFromSessionFile(ALL_TYPES_SESSION_FILE_PATH);

    // Prepare mock for c.sideInput(ddlView)
    when(processContext.sideInput(mockDdlView)).thenReturn(ddl);

    AssignShardIdFn assignShardIdFn =
        new AssignShardIdFn(
            SpannerConfig.create(),
            mockDdlView,
            sourceSchema,
            Constants.SHARDING_MODE_MULTI_SHARD,
            "test",
            "skip",
            "",
            "",
            "",
            10000L,
            Constants.SOURCE_MYSQL,
            ALL_TYPES_SESSION_FILE_PATH,
            "",
            "",
            "");

    record.setShard("shard1");
    assignShardIdFn.setSpannerAccessor(spannerAccessor);
    ObjectMapper mapper = new ObjectMapper();
    mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    assignShardIdFn.setMapper(mapper);

    assignShardIdFn.processElement(processContext);
    String keyStr = record.getTableName() + "_" + record.getMod().getKeysJson() + "_" + "shard1";
    Long key = keyStr.hashCode() % 10000L;
    String newValuesJson =
        "{\"json_field\":\"{\\\"a\\\": \\\"b\\\"}\",\"date_field2\":\"2020-12-30\",\"timestamp_field2\":\"2023-05-18T12:01:13.088397258Z\"}";
    record.setMod(
        new Mod(record.getMod().getKeysJson(), record.getMod().getOldValuesJson(), newValuesJson));
    verify(processContext, atLeast(1)).output(eq(KV.of(key, record)));
  }

  @Test
  public void testProcessElementInsertAllDatatypes() throws Exception {
    TrimmedShardedDataChangeRecord record = getInsertTrimmedDataChangeRecordAllDatatypes("shard1");
    when(processContext.element()).thenReturn(record);

    Ddl ddl = SchemaUtils.buildSpannerDdlFromSessionFile(ALL_TYPES_SESSION_FILE_PATH);
    SourceSchema sourceSchema =
        SchemaUtils.buildSourceSchemaFromSessionFile(ALL_TYPES_SESSION_FILE_PATH);

    // Prepare mock for c.sideInput(ddlView)
    when(processContext.sideInput(mockDdlView)).thenReturn(ddl);

    AssignShardIdFn assignShardIdFn =
        new AssignShardIdFn(
            SpannerConfig.create(),
            mockDdlView,
            sourceSchema,
            Constants.SHARDING_MODE_MULTI_SHARD,
            "test",
            "skip",
            "",
            "",
            "",
            10000L,
            Constants.SOURCE_MYSQL,
            ALL_TYPES_SESSION_FILE_PATH,
            "",
            "",
            "");

    record.setShard("shard1");
    assignShardIdFn.setSpannerAccessor(spannerAccessor);
    ObjectMapper mapper = new ObjectMapper();
    mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    assignShardIdFn.setMapper(mapper);

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

    Ddl ddl = SchemaUtils.buildSpannerDdlFromSessionFile(ALL_TYPES_SESSION_FILE_PATH);

    // Use a SourceSchema that does NOT contain the table
    SourceTable presentTable =
        SourceTable.builder(SourceDatabaseType.MYSQL)
            .name("someothertable")
            .schema(null)
            .columns(ImmutableList.of())
            .primaryKeyColumns(ImmutableList.of())
            .build();
    SourceSchema sourceSchema =
        SourceSchema.builder(SourceDatabaseType.MYSQL)
            .databaseName("testdb")
            .tables(ImmutableMap.of("someothertable", presentTable))
            .build();

    // Prepare mock for c.sideInput(ddlView)
    when(processContext.sideInput(mockDdlView)).thenReturn(ddl);

    AssignShardIdFn assignShardIdFn =
        new AssignShardIdFn(
            SpannerConfig.create(),
            mockDdlView,
            sourceSchema,
            Constants.SHARDING_MODE_MULTI_SHARD,
            "test",
            "skip",
            "",
            "",
            "",
            10000L,
            Constants.SOURCE_MYSQL,
            ALL_TYPES_SESSION_FILE_PATH,
            "",
            "",
            "");
    String keyStr = "tableName" + "_" + record.getMod().getKeysJson() + "_" + "skip";
    Long key = keyStr.hashCode() % 10000L;
    record.setShard(Constants.SEVERE_ERROR_SHARD_ID);

    ObjectMapper mapper = new ObjectMapper();
    mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    assignShardIdFn.setMapper(mapper);
    assignShardIdFn.processElement(processContext);
    verify(processContext, atLeast(1)).output(eq(KV.of(key, record)));
    assertEquals(Constants.SEVERE_ERROR_SHARD_ID, record.getShard());
  }

  @Test
  public void testInvalidShard() throws Exception {
    TrimmedShardedDataChangeRecord record = getInsertTrimmedDataChangeRecord("shard1/");
    when(processContext.element()).thenReturn(record);

    Ddl ddl = SchemaUtils.buildSpannerDdlFromSessionFile(SESSION_FILE_PATH);
    SourceSchema sourceSchema = SchemaUtils.buildSourceSchemaFromSessionFile(SESSION_FILE_PATH);

    // Prepare mock for c.sideInput(ddlView)
    when(processContext.sideInput(mockDdlView)).thenReturn(ddl);

    AssignShardIdFn assignShardIdFn =
        new AssignShardIdFn(
            SpannerConfig.create(),
            mockDdlView,
            sourceSchema,
            Constants.SHARDING_MODE_MULTI_SHARD,
            "test",
            "skip",
            "",
            "",
            "",
            10000L,
            Constants.SOURCE_MYSQL,
            SESSION_FILE_PATH,
            "",
            "",
            "");

    record.setShard(Constants.SEVERE_ERROR_SHARD_ID);
    assignShardIdFn.setSpannerAccessor(spannerAccessor);
    ObjectMapper mapper = new ObjectMapper();
    mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    assignShardIdFn.setMapper(mapper);

    assignShardIdFn.processElement(processContext);
    String keyStr = "tableName" + "_" + record.getMod().getKeysJson() + "_" + "skip";
    Long key = keyStr.hashCode() % 10000L;
    verify(processContext, atLeast(1)).output(eq(KV.of(key, record)));
    assertEquals(Constants.SEVERE_ERROR_SHARD_ID, record.getShard());
  }

  @Test
  public void testProcessElementDeleteNoSpannerRow() throws Exception {
    TrimmedShardedDataChangeRecord record = getDeleteTrimmedDataChangeRecordAllDatatypes("shard1");
    when(processContext.element()).thenReturn(record);

    when(mockReadOnlyTransaction.readRow(eq("Users"), any(Key.class), any(Iterable.class)))
        .thenReturn(null);

    Ddl ddl = SchemaUtils.buildSpannerDdlFromSessionFile(ALL_TYPES_SESSION_FILE_PATH);
    SourceSchema sourceSchema =
        SchemaUtils.buildSourceSchemaFromSessionFile(ALL_TYPES_SESSION_FILE_PATH);

    // Prepare mock for c.sideInput(ddlView)
    when(processContext.sideInput(mockDdlView)).thenReturn(ddl);

    AssignShardIdFn assignShardIdFn =
        new AssignShardIdFn(
            SpannerConfig.create(),
            mockDdlView,
            sourceSchema,
            Constants.SHARDING_MODE_MULTI_SHARD,
            "test",
            "skip",
            "",
            "",
            "",
            10000L,
            Constants.SOURCE_MYSQL,
            ALL_TYPES_SESSION_FILE_PATH,
            "",
            "",
            "");

    record.setShard(Constants.SEVERE_ERROR_SHARD_ID);
    assignShardIdFn.setSpannerAccessor(spannerAccessor);
    ObjectMapper mapper = new ObjectMapper();
    mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    assignShardIdFn.setMapper(mapper);

    assignShardIdFn.processElement(processContext);
    String keyStr = record.getTableName() + "_" + record.getMod().getKeysJson() + "_" + "skip";
    Long key = keyStr.hashCode() % 10000L;

    verify(processContext, atLeast(1)).output(eq(KV.of(key, record)));
    assertEquals(Constants.SEVERE_ERROR_SHARD_ID, record.getShard());
  }

  @Test
  public void testProcessElementDeleteModUsesCorrectStaleReadTimestamp() throws Exception {
    // Define a precise commit timestamp for the test record.
    com.google.cloud.Timestamp commitTimestamp =
        com.google.cloud.Timestamp.ofTimeSecondsAndNanos(1678886400, 5000);
    TrimmedShardedDataChangeRecord record =
        new TrimmedShardedDataChangeRecord(
            commitTimestamp,
            "serverTxnId",
            "recordSeq",
            "tableName",
            new Mod("{\"accountId\": \"Id1\"}", "{}", "{}"),
            ModType.valueOf("DELETE"),
            1,
            "");
    when(processContext.element()).thenReturn(record);

    Ddl ddl = SchemaUtils.buildSpannerDdlFromSessionFile(SESSION_FILE_PATH);
    SourceSchema sourceSchema = SchemaUtils.buildSourceSchemaFromSessionFile(SESSION_FILE_PATH);

    // Prepare mock for c.sideInput(mockDdlView)
    when(processContext.sideInput(mockDdlView)).thenReturn(ddl);

    AssignShardIdFn assignShardIdFn =
        new AssignShardIdFn(
            SpannerConfig.create(),
            mockDdlView,
            sourceSchema,
            Constants.SHARDING_MODE_MULTI_SHARD,
            "test",
            "skip",
            "",
            "",
            "",
            10000L,
            Constants.SOURCE_MYSQL,
            SESSION_FILE_PATH,
            "",
            "",
            "");

    assignShardIdFn.setSpannerAccessor(spannerAccessor);
    ObjectMapper mapper = new ObjectMapper();
    mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    assignShardIdFn.setMapper(mapper);

    // Triggers the stale read.
    assignShardIdFn.processElement(processContext);

    ArgumentCaptor<TimestampBound> timestampBoundCaptor =
        ArgumentCaptor.forClass(TimestampBound.class);
    verify(mockDatabaseClient).singleUse(timestampBoundCaptor.capture());

    com.google.cloud.Timestamp expectedStaleTimestamp =
        com.google.cloud.Timestamp.ofTimeSecondsAndNanos(
            commitTimestamp.getSeconds(), commitTimestamp.getNanos() - 1000);

    // Extract the actual timestamp from the captured bound and verify it is a microsecond older
    TimestampBound capturedBound = timestampBoundCaptor.getValue();
    com.google.cloud.Timestamp actualStaleTimestamp = capturedBound.getReadTimestamp();

    assertEquals(expectedStaleTimestamp, actualStaleTimestamp);
  }

  @Test
  public void testProcessElementSpannerRetryableException() throws Exception {
    TrimmedShardedDataChangeRecord record = getDeleteTrimmedDataChangeRecord("shard1");
    when(processContext.element()).thenReturn(record);

    Ddl ddl = SchemaUtils.buildSpannerDdlFromSessionFile(SESSION_FILE_PATH);
    SourceSchema sourceSchema = SchemaUtils.buildSourceSchemaFromSessionFile(SESSION_FILE_PATH);

    when(processContext.sideInput(mockDdlView)).thenReturn(ddl);

    // Mock ReadOnlyTransaction to throw a Retryable SpannerException
    when(mockReadOnlyTransaction.read(
            eq("tableName"), any(KeySet.class), any(Iterable.class), any(ReadOption.class)))
        .thenThrow(
            com.google.cloud.spanner.SpannerExceptionFactory.newSpannerException(
                com.google.cloud.spanner.ErrorCode.UNAVAILABLE, "Detailed retryable error"));

    AssignShardIdFn assignShardIdFn =
        new AssignShardIdFn(
            SpannerConfig.create(),
            mockDdlView,
            sourceSchema,
            Constants.SHARDING_MODE_MULTI_SHARD,
            "test",
            "skip",
            "",
            "",
            "",
            10000L,
            Constants.SOURCE_MYSQL,
            SESSION_FILE_PATH,
            "",
            "",
            "");
    record.setShard(Constants.RETRYABLE_ERROR_SHARD_ID);
    assignShardIdFn.setSpannerAccessor(spannerAccessor);
    ObjectMapper mapper = new ObjectMapper();
    mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    assignShardIdFn.setMapper(mapper);

    assignShardIdFn.processElement(processContext);

    String keyStr = record.getTableName() + "_" + record.getMod().getKeysJson() + "_" + "skip";
    Long key = keyStr.hashCode() % 10000L;

    verify(processContext).output(eq(KV.of(key, record)));
    assertEquals(Constants.RETRYABLE_ERROR_SHARD_ID, record.getShard());
  }

  @Test
  public void testProcessElementWithGeneratedColumnsInsert() throws Exception {
    TrimmedShardedDataChangeRecord record = getInsertTrimmedDataChangeRecord("shard1");
    // Change table name to "generated_table"
    record =
        new TrimmedShardedDataChangeRecord(
            record.getCommitTimestamp(),
            record.getServerTransactionId(),
            record.getRecordSequence(),
            "generated_table",
            record.getMod(),
            record.getModType(),
            record.getNumberOfRecordsInTransaction(),
            "");
    when(processContext.element()).thenReturn(record);

    Ddl ddl = SchemaUtils.buildSpannerDdlFromSessionFile(SESSION_FILE_PATH);
    // Add "generated_table" to DDL so `AssignShardIdFn` can find primary keys
    ddl =
        ddl.toBuilder()
            .createTable("generated_table")
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
            .column("generated_col")
            .string()
            .max()
            .generatedAs("ACCOUNT_NAME")
            .endColumn()
            .primaryKey()
            .asc("accountId")
            .end()
            .endTable()
            .build();

    // Setup source schema
    SourceTable sourceTable =
        SourceTable.builder(SourceDatabaseType.MYSQL)
            .name("generated_table")
            .schema(null)
            .primaryKeyColumns(ImmutableList.of("accountId"))
            .columns(
                ImmutableList.of(
                    com.google.cloud.teleport.v2.spanner.sourceddl.SourceColumn.builder(
                            SourceDatabaseType.MYSQL)
                        .name("accountId")
                        .type("varchar")
                        .isPrimaryKey(true)
                        .build(),
                    com.google.cloud.teleport.v2.spanner.sourceddl.SourceColumn.builder(
                            SourceDatabaseType.MYSQL)
                        .name("accountName")
                        .type("varchar")
                        .build(),
                    com.google.cloud.teleport.v2.spanner.sourceddl.SourceColumn.builder(
                            SourceDatabaseType.MYSQL)
                        .name("migration_shard_id")
                        .type("varchar")
                        .build(),
                    com.google.cloud.teleport.v2.spanner.sourceddl.SourceColumn.builder(
                            SourceDatabaseType.MYSQL)
                        .name("accountNumber")
                        .type("bigint")
                        .build(),
                    // Source column is NOT generated!
                    com.google.cloud.teleport.v2.spanner.sourceddl.SourceColumn.builder(
                            SourceDatabaseType.MYSQL)
                        .name("generated_col")
                        .type("varchar")
                        .isGenerated(false)
                        .build()))
            .build();
    SourceSchema sourceSchema =
        SourceSchema.builder(SourceDatabaseType.MYSQL)
            .databaseName("testdb")
            .tables(ImmutableMap.of("generated_table", sourceTable))
            .build();

    when(processContext.sideInput(mockDdlView)).thenReturn(ddl);

    Struct mockFetchedRow = mock(Struct.class);
    when(mockFetchedRow.getString("generated_col")).thenReturn("COMPUTED_VAL");
    when(mockFetchedRow.getValue("generated_col")).thenReturn(Value.string("COMPUTED_VAL"));
    ResultSet mockResultSet = mock(ResultSet.class);
    when(mockResultSet.next()).thenReturn(true);
    when(mockResultSet.getCurrentRowAsStruct()).thenReturn(mockFetchedRow);
    when(mockReadOnlyTransaction.read(
            eq("generated_table"), any(KeySet.class), any(Iterable.class), any(ReadOption.class)))
        .thenReturn(mockResultSet);

    AssignShardIdFn assignShardIdFn =
        new AssignShardIdFn(
            SpannerConfig.create(),
            mockDdlView,
            sourceSchema,
            Constants.SHARDING_MODE_SINGLE_SHARD,
            "shard1",
            "skip",
            "",
            "",
            "",
            10000L,
            Constants.SOURCE_MYSQL,
            null,
            "",
            "",
            "");

    record.setShard("shard1");
    assignShardIdFn.setSpannerAccessor(spannerAccessor);
    ObjectMapper mapper = new ObjectMapper();
    mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    assignShardIdFn.setMapper(mapper);

    assignShardIdFn.processElement(processContext);

    // Verify the output has the generated column appended
    ArgumentCaptor<KV<Long, TrimmedShardedDataChangeRecord>> captor =
        ArgumentCaptor.forClass(KV.class);
    verify(processContext, atLeast(1)).output(captor.capture());

    TrimmedShardedDataChangeRecord outputRecord = captor.getValue().getValue();
    assertEquals("shard1", outputRecord.getShard());
    org.junit.Assert.assertTrue(
        outputRecord.getMod().getNewValuesJson().contains("\"generated_col\":\"COMPUTED_VAL\""));
  }

  @Test
  public void testProcessElementWithGeneratedColumnsUpdate() throws Exception {
    TrimmedShardedDataChangeRecord record = getInsertTrimmedDataChangeRecord("shard1");
    record =
        new TrimmedShardedDataChangeRecord(
            record.getCommitTimestamp(),
            record.getServerTransactionId(),
            record.getRecordSequence(),
            "generated_table",
            record.getMod(),
            ModType.valueOf("UPDATE"),
            record.getNumberOfRecordsInTransaction(),
            "");
    when(processContext.element()).thenReturn(record);

    Ddl ddl = SchemaUtils.buildSpannerDdlFromSessionFile(SESSION_FILE_PATH);
    ddl =
        ddl.toBuilder()
            .createTable("generated_table")
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
            .column("generated_col")
            .string()
            .max()
            .generatedAs("ACCOUNT_NAME")
            .endColumn()
            .primaryKey()
            .asc("accountId")
            .end()
            .endTable()
            .build();

    SourceTable sourceTable =
        SourceTable.builder(SourceDatabaseType.MYSQL)
            .name("generated_table")
            .schema(null)
            .primaryKeyColumns(ImmutableList.of("accountId"))
            .columns(
                ImmutableList.of(
                    com.google.cloud.teleport.v2.spanner.sourceddl.SourceColumn.builder(
                            SourceDatabaseType.MYSQL)
                        .name("accountId")
                        .type("varchar")
                        .isPrimaryKey(true)
                        .build(),
                    com.google.cloud.teleport.v2.spanner.sourceddl.SourceColumn.builder(
                            SourceDatabaseType.MYSQL)
                        .name("accountName")
                        .type("varchar")
                        .build(),
                    com.google.cloud.teleport.v2.spanner.sourceddl.SourceColumn.builder(
                            SourceDatabaseType.MYSQL)
                        .name("migration_shard_id")
                        .type("varchar")
                        .build(),
                    com.google.cloud.teleport.v2.spanner.sourceddl.SourceColumn.builder(
                            SourceDatabaseType.MYSQL)
                        .name("accountNumber")
                        .type("bigint")
                        .build(),
                    com.google.cloud.teleport.v2.spanner.sourceddl.SourceColumn.builder(
                            SourceDatabaseType.MYSQL)
                        .name("generated_col")
                        .type("varchar")
                        .isGenerated(false)
                        .build()))
            .build();
    SourceSchema sourceSchema =
        SourceSchema.builder(SourceDatabaseType.MYSQL)
            .databaseName("testdb")
            .tables(ImmutableMap.of("generated_table", sourceTable))
            .build();

    when(processContext.sideInput(mockDdlView)).thenReturn(ddl);

    Struct mockFetchedRow = mock(Struct.class);
    when(mockFetchedRow.getString("generated_col")).thenReturn("COMPUTED_VAL_UPDATE");
    when(mockFetchedRow.getValue("generated_col")).thenReturn(Value.string("COMPUTED_VAL_UPDATE"));
    ResultSet mockResultSet = mock(ResultSet.class);
    when(mockResultSet.next()).thenReturn(true);
    when(mockResultSet.getCurrentRowAsStruct()).thenReturn(mockFetchedRow);
    when(mockReadOnlyTransaction.read(
            eq("generated_table"), any(KeySet.class), any(Iterable.class), any(ReadOption.class)))
        .thenReturn(mockResultSet);

    AssignShardIdFn assignShardIdFn =
        new AssignShardIdFn(
            SpannerConfig.create(),
            mockDdlView,
            sourceSchema,
            Constants.SHARDING_MODE_SINGLE_SHARD,
            "shard1",
            "skip",
            "",
            "",
            "",
            10000L,
            Constants.SOURCE_MYSQL,
            null,
            "",
            "",
            "");

    record.setShard("shard1");
    assignShardIdFn.setSpannerAccessor(spannerAccessor);
    ObjectMapper mapper = new ObjectMapper();
    mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    assignShardIdFn.setMapper(mapper);

    assignShardIdFn.processElement(processContext);

    ArgumentCaptor<KV<Long, TrimmedShardedDataChangeRecord>> captor =
        ArgumentCaptor.forClass(KV.class);
    verify(processContext, atLeast(1)).output(captor.capture());

    TrimmedShardedDataChangeRecord outputRecord = captor.getValue().getValue();
    assertEquals("shard1", outputRecord.getShard());
    org.junit.Assert.assertTrue(
        outputRecord
            .getMod()
            .getNewValuesJson()
            .contains("\"generated_col\":\"COMPUTED_VAL_UPDATE\""));
  }

  @Test
  public void testProcessElementSpannerSevereException() throws Exception {
    TrimmedShardedDataChangeRecord record = getDeleteTrimmedDataChangeRecord("shard1");
    when(processContext.element()).thenReturn(record);

    Ddl ddl = SchemaUtils.buildSpannerDdlFromSessionFile(SESSION_FILE_PATH);
    SourceSchema sourceSchema = SchemaUtils.buildSourceSchemaFromSessionFile(SESSION_FILE_PATH);

    when(processContext.sideInput(mockDdlView)).thenReturn(ddl);

    // Mock ReadOnlyTransaction to throw a Permanent SpannerException (e.g. Table
    // not found)
    when(mockReadOnlyTransaction.read(
            eq("tableName"), any(KeySet.class), any(Iterable.class), any(ReadOption.class)))
        .thenThrow(
            com.google.cloud.spanner.SpannerExceptionFactory.newSpannerException(
                com.google.cloud.spanner.ErrorCode.NOT_FOUND, "Detailed permanent error"));

    AssignShardIdFn assignShardIdFn =
        new AssignShardIdFn(
            SpannerConfig.create(),
            mockDdlView,
            sourceSchema,
            Constants.SHARDING_MODE_MULTI_SHARD,
            "test",
            "skip",
            "",
            "",
            "",
            10000L,
            Constants.SOURCE_MYSQL,
            SESSION_FILE_PATH,
            "",
            "",
            "");
    record.setShard(Constants.SEVERE_ERROR_SHARD_ID);
    assignShardIdFn.setSpannerAccessor(spannerAccessor);
    ObjectMapper mapper = new ObjectMapper();
    mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    assignShardIdFn.setMapper(mapper);

    assignShardIdFn.processElement(processContext);

    String keyStr = record.getTableName() + "_" + record.getMod().getKeysJson() + "_" + "skip";
    Long key = keyStr.hashCode() % 10000L;

    verify(processContext).output(eq(KV.of(key, record)));
    assertEquals(Constants.SEVERE_ERROR_SHARD_ID, record.getShard());
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
}
