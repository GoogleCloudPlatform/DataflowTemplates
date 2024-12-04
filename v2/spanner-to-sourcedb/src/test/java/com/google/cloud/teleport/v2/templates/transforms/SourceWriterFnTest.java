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

import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.Timestamp;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.exceptions.InvalidTransformationException;
import com.google.cloud.teleport.v2.spanner.migrations.schema.ColumnPK;
import com.google.cloud.teleport.v2.spanner.migrations.schema.NameAndCols;
import com.google.cloud.teleport.v2.spanner.migrations.schema.Schema;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SourceTable;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SpannerColumnDefinition;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SpannerColumnType;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SpannerTable;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SyntheticPKey;
import com.google.cloud.teleport.v2.spanner.migrations.shard.Shard;
import com.google.cloud.teleport.v2.spanner.migrations.transformation.CustomTransformation;
import com.google.cloud.teleport.v2.spanner.migrations.utils.SessionFileReader;
import com.google.cloud.teleport.v2.spanner.utils.ISpannerMigrationTransformer;
import com.google.cloud.teleport.v2.spanner.utils.MigrationTransformationResponse;
import com.google.cloud.teleport.v2.templates.changestream.ChangeStreamErrorRecord;
import com.google.cloud.teleport.v2.templates.changestream.TrimmedShardedDataChangeRecord;
import com.google.cloud.teleport.v2.templates.constants.Constants;
import com.google.cloud.teleport.v2.templates.dbutils.dao.source.IDao;
import com.google.cloud.teleport.v2.templates.dbutils.dao.source.JdbcDao;
import com.google.cloud.teleport.v2.templates.dbutils.dao.spanner.SpannerDao;
import com.google.cloud.teleport.v2.templates.dbutils.dml.MySQLDMLGenerator;
import com.google.cloud.teleport.v2.templates.dbutils.processor.SourceProcessor;
import com.google.cloud.teleport.v2.templates.utils.ShadowTableRecord;
import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import java.util.HashMap;
import java.util.Map;
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
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class SourceWriterFnTest {
  @Rule public final transient TestPipeline pipeline = TestPipeline.create();
  @Rule public final MockitoRule mocktio = MockitoJUnit.rule();
  @Mock private JdbcDao mockSqlDao;
  @Mock private SpannerDao mockSpannerDao;
  @Mock HashMap<String, IDao> mockDaoMap;
  @Mock private SpannerConfig mockSpannerConfig;
  @Mock private DoFn.ProcessContext processContext;
  @Mock private ISpannerMigrationTransformer mockSpannerMigrationTransformer;
  private static Gson gson = new Gson();

  private Shard testShard;
  private Schema testSchema;
  private Ddl testDdl;
  private String testSourceDbTimezoneOffset;

  private SourceProcessor sourceProcessor;

  @Before
  public void doBeforeEachTest() throws Exception {
    when(mockDaoMap.get(any())).thenReturn(mockSqlDao);
    when(mockSpannerDao.getShadowTableRecord(eq("shadow_parent1"), any())).thenReturn(null);
    when(mockSpannerDao.getShadowTableRecord(eq("shadow_tableName"), any())).thenReturn(null);
    when(mockSpannerDao.getShadowTableRecord(eq("shadow_parent2"), any()))
        .thenThrow(new IllegalStateException("Test exception"));
    when(mockSpannerDao.getShadowTableRecord(eq("shadow_child11"), any()))
        .thenReturn(new ShadowTableRecord(Timestamp.parseTimestamp("2025-02-02T00:00:00Z"), 1));
    when(mockSpannerDao.getShadowTableRecord(eq("shadow_child21"), any())).thenReturn(null);
    doNothing().when(mockSpannerDao).updateShadowTable(any());
    doThrow(new java.sql.SQLIntegrityConstraintViolationException("a foreign key constraint fails"))
        .when(mockSqlDao)
        .write(contains("2300")); // This is the child_id for which we want to test the foreign key
    // constraint failure.
    doThrow(
            new java.sql.SQLNonTransientConnectionException(
                "transient connection error", "HY000", 1161))
        .when(mockSqlDao)
        .write(contains("1161")); // This is the child_id for which we want to retryable
    // connection error
    doThrow(
            new java.sql.SQLNonTransientConnectionException(
                "permanent connection error", "HY000", 4242))
        .when(mockSqlDao)
        .write(contains("4242")); // no retryable error
    doThrow(new RuntimeException("generic exception"))
        .when(mockSqlDao)
        .write(contains("12345")); // to test code path of generic exception
    doNothing().when(mockSqlDao).write(contains("parent1"));
    testShard = new Shard();
    testShard.setLogicalShardId("shardA");
    testShard.setUser("test");
    testShard.setHost("test");
    testShard.setPassword("test");
    testShard.setPort("1234");
    testShard.setDbName("test");

    testSchema = SessionFileReader.read("src/test/resources/sourceWriterUTSession.json");
    testSourceDbTimezoneOffset = "+00:00";
    testDdl = getTestDdl();
    sourceProcessor =
        SourceProcessor.builder()
            .dmlGenerator(new MySQLDMLGenerator())
            .sourceDaoMap(mockDaoMap)
            .build();
  }

  @Test
  public void testSourceIsAhead() throws Exception {
    TrimmedShardedDataChangeRecord record = getChild11TrimmedDataChangeRecord("shardA");
    record.setShard("shardA");
    when(processContext.element()).thenReturn(KV.of(1L, record));
    SourceWriterFn sourceWriterFn =
        new SourceWriterFn(
            ImmutableList.of(testShard),
            testSchema,
            mockSpannerConfig,
            testSourceDbTimezoneOffset,
            testDdl,
            "shadow_",
            "skip",
            500,
            "mysql",
            null);
    ObjectMapper mapper = new ObjectMapper();
    mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    sourceWriterFn.setObjectMapper(mapper);
    sourceWriterFn.setSpannerDao(mockSpannerDao);
    sourceWriterFn.processElement(processContext);
    verify(mockSpannerDao, atLeast(1)).getShadowTableRecord(any(), any());
    verify(mockSqlDao, never()).write(any());
    verify(mockSpannerDao, never()).updateShadowTable(any());
  }

  @Test
  public void testSourceIsAheadWithSameCommitTimestamp() throws Exception {
    TrimmedShardedDataChangeRecord record =
        getChild11TrimmedDataChangeRecordWithSameCommitTimestamp("shardA");
    record.setShard("shardA");
    when(processContext.element()).thenReturn(KV.of(1L, record));
    SourceWriterFn sourceWriterFn =
        new SourceWriterFn(
            ImmutableList.of(testShard),
            testSchema,
            mockSpannerConfig,
            testSourceDbTimezoneOffset,
            testDdl,
            "shadow_",
            "skip",
            500,
            "mysql",
            null);
    ObjectMapper mapper = new ObjectMapper();
    mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    sourceWriterFn.setObjectMapper(mapper);
    sourceWriterFn.setSpannerDao(mockSpannerDao);
    sourceWriterFn.processElement(processContext);
    verify(mockSpannerDao, atLeast(1)).getShadowTableRecord(any(), any());
    verify(mockSqlDao, never()).write(any());
    verify(mockSpannerDao, never()).updateShadowTable(any());
  }

  @Test
  public void testSourceIsBehind() throws Exception {
    TrimmedShardedDataChangeRecord record = getParent1TrimmedDataChangeRecord("shardA");
    record.setShard("shardA");
    when(processContext.element()).thenReturn(KV.of(1L, record));
    SourceWriterFn sourceWriterFn =
        new SourceWriterFn(
            ImmutableList.of(testShard),
            testSchema,
            mockSpannerConfig,
            testSourceDbTimezoneOffset,
            testDdl,
            "shadow_",
            "skip",
            500,
            "mysql",
            null);
    ObjectMapper mapper = new ObjectMapper();
    mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    sourceWriterFn.setObjectMapper(mapper);
    sourceWriterFn.setSourceProcessor(sourceProcessor);
    sourceWriterFn.setSpannerDao(mockSpannerDao);
    sourceWriterFn.processElement(processContext);
    verify(mockSpannerDao, atLeast(1)).getShadowTableRecord(any(), any());
    verify(mockSqlDao, atLeast(1)).write(any());
    verify(mockSpannerDao, atLeast(1)).updateShadowTable(any());
  }

  @Test
  public void testCustomTransformationException() throws Exception {
    TrimmedShardedDataChangeRecord record = getParent1TrimmedDataChangeRecord("shardA");
    record.setShard("shardA");
    when(processContext.element()).thenReturn(KV.of(1L, record));
    when(mockSpannerMigrationTransformer.toSourceRow(any()))
        .thenThrow(new InvalidTransformationException("some exception"));
    CustomTransformation customTransformation =
        CustomTransformation.builder("jarPath", "classPath").build();
    SourceWriterFn sourceWriterFn =
        new SourceWriterFn(
            ImmutableList.of(testShard),
            testSchema,
            mockSpannerConfig,
            testSourceDbTimezoneOffset,
            testDdl,
            "shadow_",
            "skip",
            500,
            "mysql",
            customTransformation);
    ObjectMapper mapper = new ObjectMapper();
    mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    sourceWriterFn.setObjectMapper(mapper);
    sourceWriterFn.setSourceProcessor(sourceProcessor);
    sourceWriterFn.setSpannerDao(mockSpannerDao);
    sourceWriterFn.setSpannerToSourceTransformer(mockSpannerMigrationTransformer);
    sourceWriterFn.processElement(processContext);
    verify(mockSpannerDao, atLeast(1)).getShadowTableRecord(any(), any());
    String jsonRec = gson.toJson(record, TrimmedShardedDataChangeRecord.class);
    ChangeStreamErrorRecord errorRecord =
        new ChangeStreamErrorRecord(
            jsonRec,
            "com.google.cloud.teleport.v2.spanner.exceptions.InvalidTransformationException: some exception");
    verify(processContext, atLeast(1))
        .output(
            Constants.PERMANENT_ERROR_TAG, gson.toJson(errorRecord, ChangeStreamErrorRecord.class));
  }

  @Test
  public void testCustomTransformationApplied() throws Exception {
    TrimmedShardedDataChangeRecord record = getParent1TrimmedDataChangeRecord("shardA");
    record.setShard("shardA");
    when(processContext.element()).thenReturn(KV.of(1L, record));
    when(mockSpannerMigrationTransformer.toSourceRow(any()))
        .thenReturn(new MigrationTransformationResponse(Map.of("id", "45"), false));
    CustomTransformation customTransformation =
        CustomTransformation.builder("jarPath", "classPath").build();
    SourceWriterFn sourceWriterFn =
        new SourceWriterFn(
            ImmutableList.of(testShard),
            testSchema,
            mockSpannerConfig,
            testSourceDbTimezoneOffset,
            testDdl,
            "shadow_",
            "skip",
            500,
            "mysql",
            customTransformation);
    ObjectMapper mapper = new ObjectMapper();
    mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    sourceWriterFn.setObjectMapper(mapper);
    sourceWriterFn.setSourceProcessor(sourceProcessor);
    sourceWriterFn.setSpannerDao(mockSpannerDao);
    sourceWriterFn.setSpannerToSourceTransformer(mockSpannerMigrationTransformer);
    sourceWriterFn.processElement(processContext);
    ArgumentCaptor<String> argumentCaptor = ArgumentCaptor.forClass(String.class);
    verify(mockSpannerDao, atLeast(1)).getShadowTableRecord(any(), any());
    verify(mockSqlDao, atLeast(1)).write(argumentCaptor.capture());
    assertTrue(argumentCaptor.getValue().contains("INSERT INTO `parent1`(`id`) VALUES (45)"));
    verify(mockSpannerDao, atLeast(1)).updateShadowTable(any());
  }

  @Test
  public void testCustomTransformationFiltered() throws Exception {
    TrimmedShardedDataChangeRecord record = getParent1TrimmedDataChangeRecord("shardA");
    record.setShard("shardA");
    when(processContext.element()).thenReturn(KV.of(1L, record));
    when(mockSpannerMigrationTransformer.toSourceRow(any()))
        .thenReturn(new MigrationTransformationResponse(null, true));
    CustomTransformation customTransformation =
        CustomTransformation.builder("jarPath", "classPath").build();
    SourceWriterFn sourceWriterFn =
        new SourceWriterFn(
            ImmutableList.of(testShard),
            testSchema,
            mockSpannerConfig,
            testSourceDbTimezoneOffset,
            testDdl,
            "shadow_",
            "skip",
            500,
            "mysql",
            customTransformation);
    ObjectMapper mapper = new ObjectMapper();
    mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    sourceWriterFn.setObjectMapper(mapper);
    sourceWriterFn.setSourceProcessor(sourceProcessor);
    sourceWriterFn.setSpannerDao(mockSpannerDao);
    sourceWriterFn.setSpannerToSourceTransformer(mockSpannerMigrationTransformer);
    sourceWriterFn.processElement(processContext);
    verify(mockSpannerDao, atLeast(1)).getShadowTableRecord(any(), any());
    verify(mockSqlDao, atLeast(0)).write(any());
    verify(mockSpannerDao, atLeast(0)).updateShadowTable(any());
    String jsonRec = gson.toJson(record, TrimmedShardedDataChangeRecord.class);
    ChangeStreamErrorRecord errorRecord =
        new ChangeStreamErrorRecord(jsonRec, Constants.FILTERED_TAG_MESSAGE);
    verify(processContext, atLeast(1))
        .output(Constants.FILTERED_TAG, gson.toJson(errorRecord, ChangeStreamErrorRecord.class));
  }

  @Test
  public void testNoShard() throws Exception {
    TrimmedShardedDataChangeRecord record = getParent1TrimmedDataChangeRecord("shardA");
    when(processContext.element()).thenReturn(KV.of(1L, record));
    SourceWriterFn sourceWriterFn =
        new SourceWriterFn(
            ImmutableList.of(testShard),
            testSchema,
            mockSpannerConfig,
            testSourceDbTimezoneOffset,
            testDdl,
            "shadow_",
            "skip",
            500,
            "mysql",
            null);
    ObjectMapper mapper = new ObjectMapper();
    mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    sourceWriterFn.setObjectMapper(mapper);
    sourceWriterFn.setSpannerDao(mockSpannerDao);
    sourceWriterFn.processElement(processContext);
    String jsonRec = gson.toJson(record, TrimmedShardedDataChangeRecord.class);
    ChangeStreamErrorRecord errorRecord =
        new ChangeStreamErrorRecord(jsonRec, Constants.SHARD_NOT_PRESENT_ERROR_MESSAGE);
    verify(processContext, atLeast(1))
        .output(
            Constants.PERMANENT_ERROR_TAG, gson.toJson(errorRecord, ChangeStreamErrorRecord.class));
  }

  @Test
  public void testSkipShard() throws Exception {
    TrimmedShardedDataChangeRecord record = getParent1TrimmedDataChangeRecord("shardA");
    record.setShard("skip");

    when(processContext.element()).thenReturn(KV.of(1L, record));
    SourceWriterFn sourceWriterFn =
        new SourceWriterFn(
            ImmutableList.of(testShard),
            testSchema,
            mockSpannerConfig,
            testSourceDbTimezoneOffset,
            testDdl,
            "shadow_",
            "skip",
            500,
            "mysql",
            null);
    ObjectMapper mapper = new ObjectMapper();
    mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    sourceWriterFn.setObjectMapper(mapper);
    sourceWriterFn.setSpannerDao(mockSpannerDao);
    sourceWriterFn.processElement(processContext);
    String jsonRec = gson.toJson(record, TrimmedShardedDataChangeRecord.class);
    ChangeStreamErrorRecord errorRecord =
        new ChangeStreamErrorRecord(jsonRec, Constants.SKIPPED_TAG_MESSAGE);
    verify(processContext, atLeast(1))
        .output(Constants.SKIPPED_TAG, gson.toJson(errorRecord, ChangeStreamErrorRecord.class));
  }

  @Test
  public void testPermanentError() throws Exception {
    TrimmedShardedDataChangeRecord record = getParent1IncorrectTrimmedDataChangeRecord("shardA");
    record.setShard("shardA");
    when(processContext.element()).thenReturn(KV.of(1L, record));
    SourceWriterFn sourceWriterFn =
        new SourceWriterFn(
            ImmutableList.of(testShard),
            testSchema,
            mockSpannerConfig,
            testSourceDbTimezoneOffset,
            testDdl,
            "shadow_",
            "skip",
            500,
            "mysql",
            null);
    ObjectMapper mapper = new ObjectMapper();
    mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    sourceWriterFn.setObjectMapper(mapper);
    sourceWriterFn.setSpannerDao(mockSpannerDao);
    sourceWriterFn.processElement(processContext);
    String jsonRec = gson.toJson(record, TrimmedShardedDataChangeRecord.class);
    ChangeStreamErrorRecord errorRecord =
        new ChangeStreamErrorRecord(
            jsonRec,
            "com.google.cloud.teleport.v2.spanner.migrations.exceptions.ChangeEventConvertorException:"
                + " Required key id not found in change event");
    verify(processContext, atLeast(1))
        .output(
            Constants.PERMANENT_ERROR_TAG, gson.toJson(errorRecord, ChangeStreamErrorRecord.class));
  }

  @Test
  public void testRetryableError() throws Exception {
    TrimmedShardedDataChangeRecord record = getParent2TrimmedDataChangeRecord("shardA");
    record.setShard("shardA");
    when(processContext.element()).thenReturn(KV.of(1L, record));
    SourceWriterFn sourceWriterFn =
        new SourceWriterFn(
            ImmutableList.of(testShard),
            testSchema,
            mockSpannerConfig,
            testSourceDbTimezoneOffset,
            testDdl,
            "shadow_",
            "skip",
            500,
            "mysql",
            null);
    ObjectMapper mapper = new ObjectMapper();
    mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    sourceWriterFn.setObjectMapper(mapper);
    sourceWriterFn.setSpannerDao(mockSpannerDao);
    sourceWriterFn.processElement(processContext);
    String jsonRec = gson.toJson(record, TrimmedShardedDataChangeRecord.class);
    ChangeStreamErrorRecord errorRecord = new ChangeStreamErrorRecord(jsonRec, "Test exception");
    verify(processContext, atLeast(1))
        .output(
            Constants.RETRYABLE_ERROR_TAG, gson.toJson(errorRecord, ChangeStreamErrorRecord.class));
  }

  @Test
  public void testRetryableErrorForForeignKey() throws Exception {
    TrimmedShardedDataChangeRecord record = getChild21TrimmedDataChangeRecord("shardA", 2300);
    record.setShard("shardA");
    when(processContext.element()).thenReturn(KV.of(1L, record));
    SourceWriterFn sourceWriterFn =
        new SourceWriterFn(
            ImmutableList.of(testShard),
            testSchema,
            mockSpannerConfig,
            testSourceDbTimezoneOffset,
            testDdl,
            "shadow_",
            "skip",
            500,
            "mysql",
            null);
    ObjectMapper mapper = new ObjectMapper();
    mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    sourceWriterFn.setObjectMapper(mapper);
    sourceWriterFn.setSourceProcessor(sourceProcessor);
    sourceWriterFn.setSpannerDao(mockSpannerDao);
    sourceWriterFn.processElement(processContext);
    String jsonRec = gson.toJson(record, TrimmedShardedDataChangeRecord.class);
    ChangeStreamErrorRecord errorRecord =
        new ChangeStreamErrorRecord(jsonRec, "a foreign key constraint fails");
    verify(processContext, atLeast(1))
        .output(
            Constants.RETRYABLE_ERROR_TAG, gson.toJson(errorRecord, ChangeStreamErrorRecord.class));
  }

  @Test
  public void testRetryableErrorConnectionFailure() throws Exception {
    TrimmedShardedDataChangeRecord record = getChild21TrimmedDataChangeRecord("shardA", 1161);
    record.setShard("shardA");
    when(processContext.element()).thenReturn(KV.of(1L, record));
    SourceWriterFn sourceWriterFn =
        new SourceWriterFn(
            ImmutableList.of(testShard),
            testSchema,
            mockSpannerConfig,
            testSourceDbTimezoneOffset,
            testDdl,
            "shadow_",
            "skip",
            500,
            "mysql",
            null);
    ObjectMapper mapper = new ObjectMapper();
    mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    sourceWriterFn.setObjectMapper(mapper);
    sourceWriterFn.setSourceProcessor(sourceProcessor);
    sourceWriterFn.setSpannerDao(mockSpannerDao);
    sourceWriterFn.processElement(processContext);
    String jsonRec = gson.toJson(record, TrimmedShardedDataChangeRecord.class);
    ChangeStreamErrorRecord errorRecord =
        new ChangeStreamErrorRecord(jsonRec, "transient connection error");
    verify(processContext, atLeast(1))
        .output(
            Constants.RETRYABLE_ERROR_TAG, gson.toJson(errorRecord, ChangeStreamErrorRecord.class));
  }

  @Test
  public void testPermanentConnectionFailure() throws Exception {
    TrimmedShardedDataChangeRecord record = getChild21TrimmedDataChangeRecord("shardA", 4242);
    record.setShard("shardA");
    when(processContext.element()).thenReturn(KV.of(1L, record));
    SourceWriterFn sourceWriterFn =
        new SourceWriterFn(
            ImmutableList.of(testShard),
            testSchema,
            mockSpannerConfig,
            testSourceDbTimezoneOffset,
            testDdl,
            "shadow_",
            "skip",
            500,
            "mysql",
            null);
    ObjectMapper mapper = new ObjectMapper();
    mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    sourceWriterFn.setObjectMapper(mapper);
    sourceWriterFn.setSourceProcessor(sourceProcessor);
    sourceWriterFn.setSpannerDao(mockSpannerDao);
    sourceWriterFn.processElement(processContext);
    String jsonRec = gson.toJson(record, TrimmedShardedDataChangeRecord.class);
    ChangeStreamErrorRecord errorRecord =
        new ChangeStreamErrorRecord(jsonRec, "permanent connection error");
    verify(processContext, atLeast(1))
        .output(
            Constants.PERMANENT_ERROR_TAG, gson.toJson(errorRecord, ChangeStreamErrorRecord.class));
  }

  @Test
  public void testPermanentGenericException() throws Exception {
    TrimmedShardedDataChangeRecord record = getChild21TrimmedDataChangeRecord("shardA", 12345);
    record.setShard("shardA");
    when(processContext.element()).thenReturn(KV.of(1L, record));
    SourceWriterFn sourceWriterFn =
        new SourceWriterFn(
            ImmutableList.of(testShard),
            testSchema,
            mockSpannerConfig,
            testSourceDbTimezoneOffset,
            testDdl,
            "shadow_",
            "skip",
            500,
            "mysql",
            null);
    ObjectMapper mapper = new ObjectMapper();
    mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    sourceWriterFn.setObjectMapper(mapper);
    sourceWriterFn.setSourceProcessor(sourceProcessor);
    sourceWriterFn.setSpannerDao(mockSpannerDao);
    sourceWriterFn.processElement(processContext);
    String jsonRec = gson.toJson(record, TrimmedShardedDataChangeRecord.class);
    ChangeStreamErrorRecord errorRecord = new ChangeStreamErrorRecord(jsonRec, "generic exception");
    verify(processContext, atLeast(1))
        .output(
            Constants.PERMANENT_ERROR_TAG, gson.toJson(errorRecord, ChangeStreamErrorRecord.class));
  }

  @Test
  public void testDMLEmpty() throws Exception {
    TrimmedShardedDataChangeRecord record = getTrimmedDataChangeRecordToSimulateNullDML("shardA");
    record.setShard("shardA");
    when(processContext.element()).thenReturn(KV.of(1L, record));
    SourceWriterFn sourceWriterFn =
        new SourceWriterFn(
            ImmutableList.of(testShard),
            getSchemaObject(),
            mockSpannerConfig,
            testSourceDbTimezoneOffset,
            testDdlForNullDML(),
            "shadow_",
            "skip",
            500,
            "mysql",
            null);
    ObjectMapper mapper = new ObjectMapper();
    mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    sourceWriterFn.setObjectMapper(mapper);
    sourceWriterFn.setSpannerDao(mockSpannerDao);
    sourceWriterFn.processElement(processContext);
    verify(mockSqlDao, never()).write(contains("567890"));
  }

  static Ddl getTestDdl() {
    Ddl ddl =
        Ddl.builder()
            .createTable("parent1")
            .column("id")
            .int64()
            .endColumn()
            .column("update_ts")
            .timestamp()
            .endColumn()
            .column("in_ts")
            .timestamp()
            .endColumn()
            .column("migration_shard_id")
            .string()
            .max()
            .endColumn()
            .primaryKey()
            .asc("id")
            .end()
            .endTable()
            .createTable("child11")
            .column("child_id")
            .int64()
            .endColumn()
            .column("parent_id")
            .int64()
            .endColumn()
            .column("update_ts")
            .timestamp()
            .endColumn()
            .column("in_ts")
            .timestamp()
            .endColumn()
            .column("migration_shard_id")
            .string()
            .max()
            .endColumn()
            .endTable()
            .createTable("parent2")
            .column("id")
            .int64()
            .endColumn()
            .column("update_ts")
            .timestamp()
            .endColumn()
            .column("in_ts")
            .timestamp()
            .endColumn()
            .column("migration_shard_id")
            .string()
            .max()
            .endColumn()
            .primaryKey()
            .asc("id")
            .end()
            .endTable()
            .createTable("child21")
            .column("child_id")
            .int64()
            .endColumn()
            .column("parent_id")
            .int64()
            .endColumn()
            .column("update_ts")
            .timestamp()
            .endColumn()
            .column("in_ts")
            .timestamp()
            .endColumn()
            .column("migration_shard_id")
            .string()
            .max()
            .endColumn()
            .primaryKey()
            .asc("child_id")
            .asc("parent_id")
            .end()
            .endTable()
            .build();
    return ddl;
  }

  private TrimmedShardedDataChangeRecord getChild11TrimmedDataChangeRecord(String shardId) {
    return new TrimmedShardedDataChangeRecord(
        Timestamp.parseTimestamp("2024-12-01T10:15:30.000Z"),
        "serverTxnId",
        "0",
        "child11",
        new Mod(
            "{\"child_id\": \"42\" , \"parent_id\": \"42\"}",
            "{}",
            "{ \"migration_shard_id\": \"" + shardId + "\"}"),
        ModType.valueOf("INSERT"),
        1,
        "");
  }

  private TrimmedShardedDataChangeRecord getChild11TrimmedDataChangeRecordWithSameCommitTimestamp(
      String shardId) {
    return new TrimmedShardedDataChangeRecord(
        Timestamp.parseTimestamp("2025-02-02T00:00:00Z"),
        "serverTxnId",
        "0",
        "child11",
        new Mod(
            "{\"child_id\": \"42\" , \"parent_id\": \"42\"}",
            "{}",
            "{ \"migration_shard_id\": \"" + shardId + "\"}"),
        ModType.valueOf("INSERT"),
        1,
        "");
  }

  private TrimmedShardedDataChangeRecord getParent1TrimmedDataChangeRecord(String shardId) {
    return new TrimmedShardedDataChangeRecord(
        Timestamp.parseTimestamp("2020-12-01T10:15:30.000Z"),
        "serverTxnId",
        "0",
        "parent1",
        new Mod("{\"id\": \"42\"}", "{}", "{ \"migration_shard_id\": \"" + shardId + "\"}"),
        ModType.valueOf("INSERT"),
        1,
        "");
  }

  private TrimmedShardedDataChangeRecord getTrimmedDataChangeRecordToSimulateNullDML(
      String shardId) {
    return new TrimmedShardedDataChangeRecord(
        Timestamp.parseTimestamp("2020-12-01T10:15:30.000Z"),
        "serverTxnId",
        "recordSeq",
        "tableName",
        new Mod(
            "{\"accountId\": \"567890\"}",
            "{}",
            "{\"accountName\": \"abc\", \"migration_shard_id\": \""
                + shardId
                + "\", \"accountNumber\": 1}"),
        ModType.valueOf("INSERT"),
        1,
        "");
  }

  private TrimmedShardedDataChangeRecord getParent1IncorrectTrimmedDataChangeRecord(
      String shardId) {
    return new TrimmedShardedDataChangeRecord(
        Timestamp.parseTimestamp("2020-12-01T10:15:30.000Z"),
        "serverTxnId",
        "0",
        "parent1",
        new Mod(
            "{\"junk_colm\": \"hello\"}", "{}", "{ \"migration_shard_id\": \"" + shardId + "\"}"),
        ModType.valueOf("INSERT"),
        1,
        "");
  }

  private TrimmedShardedDataChangeRecord getParent2TrimmedDataChangeRecord(String shardId) {
    return new TrimmedShardedDataChangeRecord(
        Timestamp.parseTimestamp("2020-12-01T10:15:30.000Z"),
        "serverTxnId",
        "0",
        "parent2",
        new Mod("{\"id\": \"42\"}", "{}", "{ \"migration_shard_id\": \"" + shardId + "\"}"),
        ModType.valueOf("INSERT"),
        1,
        "");
  }

  private TrimmedShardedDataChangeRecord getChild21TrimmedDataChangeRecord(
      String shardId, int childIdForMockResponse) {
    return new TrimmedShardedDataChangeRecord(
        Timestamp.parseTimestamp("2024-12-01T10:15:30.000Z"),
        "serverTxnId",
        "0",
        "child21",
        new Mod(
            "{\"child_id\": \"" + childIdForMockResponse + "\" , \"parent_id\": \"42\"}",
            "{}",
            "{ \"migration_shard_id\": \"" + shardId + "\"}"),
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

  static Ddl testDdlForNullDML() {
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
