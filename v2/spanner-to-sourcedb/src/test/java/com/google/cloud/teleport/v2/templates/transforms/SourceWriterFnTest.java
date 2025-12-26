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
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.Options.RpcPriority;
import com.google.cloud.spanner.SpannerExceptionFactory;
import com.google.cloud.spanner.TransactionRunner;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.exceptions.InvalidTransformationException;
import com.google.cloud.teleport.v2.spanner.migrations.exceptions.ChangeEventConvertorException;
import com.google.cloud.teleport.v2.spanner.migrations.schema.ISchemaMapper;
import com.google.cloud.teleport.v2.spanner.migrations.schema.Schema;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SessionBasedMapper;
import com.google.cloud.teleport.v2.spanner.migrations.shard.Shard;
import com.google.cloud.teleport.v2.spanner.migrations.transformation.CustomTransformation;
import com.google.cloud.teleport.v2.spanner.migrations.utils.SessionFileReader;
import com.google.cloud.teleport.v2.spanner.sourceddl.SourceSchema;
import com.google.cloud.teleport.v2.spanner.utils.ISpannerMigrationTransformer;
import com.google.cloud.teleport.v2.spanner.utils.MigrationTransformationResponse;
import com.google.cloud.teleport.v2.templates.SpannerToSourceDb.Options;
import com.google.cloud.teleport.v2.templates.changestream.ChangeStreamErrorRecord;
import com.google.cloud.teleport.v2.templates.changestream.TrimmedShardedDataChangeRecord;
import com.google.cloud.teleport.v2.templates.constants.Constants;
import com.google.cloud.teleport.v2.templates.dbutils.dao.source.IDao;
import com.google.cloud.teleport.v2.templates.dbutils.dao.source.JdbcDao;
import com.google.cloud.teleport.v2.templates.dbutils.dao.spanner.SpannerDao;
import com.google.cloud.teleport.v2.templates.dbutils.dml.MySQLDMLGenerator;
import com.google.cloud.teleport.v2.templates.dbutils.processor.SourceProcessor;
import com.google.cloud.teleport.v2.templates.utils.SchemaUtils;
import com.google.cloud.teleport.v2.templates.utils.ShadowTableRecord;
import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import java.sql.SQLDataException;
import java.sql.SQLSyntaxErrorException;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.Mod;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.ModType;
import org.apache.beam.sdk.options.ValueProvider;
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
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.mockito.stubbing.Answer;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class SourceWriterFnTest {
  @Rule public final transient TestPipeline pipeline = TestPipeline.create();
  @Rule public final MockitoRule mocktio = MockitoJUnit.rule();
  @Mock private JdbcDao mockSqlDao;
  @Mock private SpannerDao mockSpannerDao;
  @Mock private DatabaseClient mockDatabaseClient;
  @Mock private TransactionRunner mockTransactionRunner;
  @Mock HashMap<String, IDao> mockDaoMap;
  @Mock private SpannerConfig mockSpannerConfig;
  @Mock private DoFn.ProcessContext processContext;
  @Mock private ISpannerMigrationTransformer mockSpannerMigrationTransformer;
  @Mock private SourceProcessor mockSourceProcessor;
  @Mock private Options mockOptions;
  @Mock private PCollectionView<Ddl> mockDdlView;
  @Mock private PCollectionView<Ddl> mockShadowTableDdlView;
  private static Gson gson = new Gson();

  private Shard testShard;
  private Schema testSchema;
  private Ddl testDdl;
  private Ddl shadowTableDdl;
  private SourceSchema testSourceSchema;

  private ISchemaMapper schemaMapper;

  private String testSourceDbTimezoneOffset;

  private SourceProcessor sourceProcessor;

  @Before
  public void doBeforeEachTest() throws Exception {
    when(mockDaoMap.get(any())).thenReturn(mockSqlDao);
    when(mockSpannerDao.getDatabaseClient()).thenReturn(mockDatabaseClient);
    when(mockDatabaseClient.readWriteTransaction(any())).thenReturn(mockTransactionRunner);
    when(mockTransactionRunner.run(any(TransactionRunner.TransactionCallable.class)))
        .thenAnswer(
            new Answer<Void>() {
              @Override
              public Void answer(InvocationOnMock invocation) throws Throwable {
                TransactionRunner.TransactionCallable<Void> callable = invocation.getArgument(0);
                try {
                  callable.run(null);
                } catch (Exception e) {
                  throw SpannerExceptionFactory.newSpannerException(
                      ErrorCode.UNKNOWN, e.getMessage(), e);
                }
                return null;
              }
            });
    when(processContext.getPipelineOptions()).thenReturn(mockOptions);

    when(mockSpannerDao.readShadowTableRecordWithExclusiveLock(
            eq("shadow_parent1"), any(), any(), any()))
        .thenReturn(null);
    when(mockSpannerDao.readShadowTableRecordWithExclusiveLock(
            eq("shadow_tableName"), any(), any(), any()))
        .thenReturn(null);
    when(mockSpannerDao.readShadowTableRecordWithExclusiveLock(
            eq("shadow_parent2"), any(), any(), any()))
        .thenThrow(new IllegalStateException("Test exception"));
    when(mockSpannerDao.readShadowTableRecordWithExclusiveLock(
            eq("shadow_child11"), any(), any(), any()))
        .thenReturn(new ShadowTableRecord(Timestamp.parseTimestamp("2025-02-02T00:00:00Z"), 1));
    when(mockSpannerDao.readShadowTableRecordWithExclusiveLock(
            eq("shadow_child21"), any(), any(), any()))
        .thenReturn(null);
    when(mockSpannerConfig.getRpcPriority())
        .thenReturn(ValueProvider.StaticValueProvider.of(RpcPriority.HIGH));
    doNothing().when(mockSpannerDao).updateShadowTable(any(), any());
    doThrow(new java.sql.SQLIntegrityConstraintViolationException("a foreign key constraint fails"))
        .when(mockSqlDao)
        .write(
            contains("2300"),
            any()); // This is the child_id for which we want to test the foreign key
    // constraint failure.
    doThrow(
            new java.sql.SQLNonTransientConnectionException(
                "transient connection error", "HY000", 1161))
        .when(mockSqlDao)
        .write(contains("1161"), any()); // This is the child_id for which we want to retryable
    // connection error
    doThrow(
            new java.sql.SQLNonTransientConnectionException(
                "permanent connection error", "HY000", 4242))
        .when(mockSqlDao)
        .write(contains("4242"), any()); // no retryable error
    doThrow(new RuntimeException("generic exception"))
        .when(mockSqlDao)
        .write(contains("12345"), any()); // to test code path of generic exception
    doThrow(new SQLSyntaxErrorException("sql syntax error"))
        .when(mockSqlDao)
        .write(contains("6666"), any());
    doThrow(new SQLDataException("sql data error")).when(mockSqlDao).write(contains("7777"), any());
    doThrow(new InvalidTransformationException("invalid transformation"))
        .when(mockSqlDao)
        .write(contains("8888"), any());
    doThrow(new ChangeEventConvertorException("change event convertor error"))
        .when(mockSqlDao)
        .write(contains("9999"), any());
    doThrow(SpannerExceptionFactory.newSpannerException(ErrorCode.ALREADY_EXISTS, "test spanner"))
        .when(mockSqlDao)
        .write(contains("1111"), any());
    doThrow(new IllegalArgumentException("illegal argument"))
        .when(mockSqlDao)
        .write(contains("2222"), any());
    doNothing().when(mockSqlDao).write(contains("parent1"), any());
    testShard = new Shard();
    testShard.setLogicalShardId("shardA");
    testShard.setUser("test");
    testShard.setHost("test");
    testShard.setPassword("test");
    testShard.setPort("1234");
    testShard.setDbName("test");

    testSchema = SessionFileReader.read("src/test/resources/sourceWriterUTSession.json");
    testDdl =
        SchemaUtils.buildSpannerDdlFromSessionFile("src/test/resources/sourceWriterUTSession.json");
    schemaMapper = new SessionBasedMapper(testSchema, testDdl);
    shadowTableDdl =
        SchemaUtils.buildSpannerShadowTableDdlFromSessionFile(
            "src/test/resources/sourceWriterUTSession.json");
    testSourceSchema =
        SchemaUtils.buildSourceSchemaFromSessionFile(
            "src/test/resources/sourceWriterUTSession.json");
    testSourceDbTimezoneOffset = "+00:00";
    sourceProcessor =
        SourceProcessor.builder()
            .dmlGenerator(new MySQLDMLGenerator())
            .sourceDaoMap(mockDaoMap)
            .build();

    // Mock the options object for use in tests
    when(mockOptions.getSessionFilePath())
        .thenReturn("src/test/resources/sourceWriterUTSession.json");
    when(mockOptions.getTableOverrides()).thenReturn("");
    when(mockOptions.getColumnOverrides()).thenReturn("");
    when(mockOptions.getSchemaOverridesFilePath()).thenReturn("");
    when(mockOptions.getSourceDbTimezoneOffset()).thenReturn(testSourceDbTimezoneOffset);

    // Mock side input access in ProcessContext
    when(processContext.sideInput(mockDdlView)).thenReturn(testDdl);
    when(processContext.sideInput(mockShadowTableDdlView)).thenReturn(shadowTableDdl);
    when(mockOptions.as(Options.class)).thenReturn(mockOptions);
  }

  @Test
  public void testSourceIsAhead() throws Exception {
    TrimmedShardedDataChangeRecord record = getChild11TrimmedDataChangeRecord("shardA");
    record.setShard("shardA");
    when(processContext.element()).thenReturn(KV.of(1L, record));
    SourceWriterFn sourceWriterFn =
        new SourceWriterFn(
            ImmutableList.of(testShard),
            mockSpannerConfig,
            testSourceDbTimezoneOffset,
            testSourceSchema,
            "shadow_",
            "skip",
            500,
            "mysql",
            null,
            mockDdlView,
            mockShadowTableDdlView,
            "src/test/resources/sourceWriterUTSession.json",
            "",
            "",
            "");
    ObjectMapper mapper = new ObjectMapper();
    mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    sourceWriterFn.setObjectMapper(mapper);
    sourceWriterFn.setSpannerDao(mockSpannerDao);
    sourceWriterFn.processElement(processContext);
    verify(mockSpannerDao, atLeast(1))
        .readShadowTableRecordWithExclusiveLock(any(), any(), any(), any());
    verify(mockSqlDao, never()).write(any(), any());
    verify(mockSpannerDao, never()).updateShadowTable(any(), any());
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
            mockSpannerConfig,
            testSourceDbTimezoneOffset,
            testSourceSchema,
            "shadow_",
            "skip",
            500,
            "mysql",
            null,
            mockDdlView,
            mockShadowTableDdlView,
            "src/test/resources/sourceWriterUTSession.json",
            "",
            "",
            "");
    ObjectMapper mapper = new ObjectMapper();
    mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    sourceWriterFn.setObjectMapper(mapper);
    sourceWriterFn.setSpannerDao(mockSpannerDao);
    sourceWriterFn.processElement(processContext);
    verify(mockSpannerDao, atLeast(1))
        .readShadowTableRecordWithExclusiveLock(any(), any(), any(), any());
    verify(mockSqlDao, never()).write(any(), any());
    verify(mockSpannerDao, never()).updateShadowTable(any(), any());
  }

  @Test
  public void testSourceIsBehind() throws Exception {
    TrimmedShardedDataChangeRecord record = getParent1TrimmedDataChangeRecord("shardA");
    record.setShard("shardA");
    when(processContext.element()).thenReturn(KV.of(1L, record));
    SourceWriterFn sourceWriterFn =
        new SourceWriterFn(
            ImmutableList.of(testShard),
            mockSpannerConfig,
            testSourceDbTimezoneOffset,
            testSourceSchema,
            "shadow_",
            "skip",
            500,
            "mysql",
            null,
            mockDdlView,
            mockShadowTableDdlView,
            "src/test/resources/sourceWriterUTSession.json",
            "",
            "",
            "");
    ObjectMapper mapper = new ObjectMapper();
    mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    sourceWriterFn.setObjectMapper(mapper);
    sourceWriterFn.setSourceProcessor(sourceProcessor);
    sourceWriterFn.setSpannerDao(mockSpannerDao);
    sourceWriterFn.processElement(processContext);
    verify(mockSpannerDao, atLeast(1))
        .readShadowTableRecordWithExclusiveLock(any(), any(), any(), any());
    verify(mockSqlDao, atLeast(1)).write(any(), any());
    verify(mockSpannerDao, atLeast(1)).updateShadowTable(any(), any());
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
            mockSpannerConfig,
            testSourceDbTimezoneOffset,
            testSourceSchema,
            "shadow_",
            "skip",
            500,
            "mysql",
            customTransformation,
            mockDdlView,
            mockShadowTableDdlView,
            "src/test/resources/sourceWriterUTSession.json",
            "",
            "",
            "");
    ObjectMapper mapper = new ObjectMapper();
    mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    sourceWriterFn.setObjectMapper(mapper);
    sourceWriterFn.setSourceProcessor(sourceProcessor);
    sourceWriterFn.setSpannerDao(mockSpannerDao);
    sourceWriterFn.setSpannerToSourceTransformer(mockSpannerMigrationTransformer);
    sourceWriterFn.processElement(processContext);
    verify(mockSpannerDao, atLeast(1))
        .readShadowTableRecordWithExclusiveLock(any(), any(), any(), any());
    ArgumentCaptor<String> argumentCaptor = ArgumentCaptor.forClass(String.class);
    verify(processContext, atLeast(1))
        .output(eq(Constants.PERMANENT_ERROR_TAG), argumentCaptor.capture());
    ChangeStreamErrorRecord actualError =
        gson.fromJson(argumentCaptor.getValue(), ChangeStreamErrorRecord.class);
    assertTrue(actualError.getErrorMessage().contains("some exception"));
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
            mockSpannerConfig,
            testSourceDbTimezoneOffset,
            testSourceSchema,
            "shadow_",
            "skip",
            500,
            "mysql",
            customTransformation,
            mockDdlView,
            mockShadowTableDdlView,
            "src/test/resources/sourceWriterUTSession.json",
            "",
            "",
            "");
    ObjectMapper mapper = new ObjectMapper();
    mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    sourceWriterFn.setObjectMapper(mapper);
    sourceWriterFn.setSourceProcessor(sourceProcessor);
    sourceWriterFn.setSpannerDao(mockSpannerDao);
    sourceWriterFn.setSpannerToSourceTransformer(mockSpannerMigrationTransformer);
    sourceWriterFn.processElement(processContext);
    ArgumentCaptor<String> argumentCaptor = ArgumentCaptor.forClass(String.class);
    verify(mockSpannerDao, atLeast(1))
        .readShadowTableRecordWithExclusiveLock(any(), any(), any(), any());
    verify(mockSqlDao, atLeast(1)).write(argumentCaptor.capture(), any());
    assertTrue(argumentCaptor.getValue().contains("INSERT INTO `parent1`(`id`) VALUES (45)"));
    verify(mockSpannerDao, atLeast(1)).updateShadowTable(any(), any());
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
            mockSpannerConfig,
            testSourceDbTimezoneOffset,
            testSourceSchema,
            "shadow_",
            "skip",
            500,
            "mysql",
            customTransformation,
            mockDdlView,
            mockShadowTableDdlView,
            "src/test/resources/sourceWriterUTSession.json",
            "",
            "",
            "");
    ObjectMapper mapper = new ObjectMapper();
    mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    sourceWriterFn.setObjectMapper(mapper);
    sourceWriterFn.setSourceProcessor(sourceProcessor);
    sourceWriterFn.setSpannerDao(mockSpannerDao);
    sourceWriterFn.setSpannerToSourceTransformer(mockSpannerMigrationTransformer);
    sourceWriterFn.processElement(processContext);
    verify(mockSpannerDao, atLeast(1))
        .readShadowTableRecordWithExclusiveLock(any(), any(), any(), any());
    verify(mockSqlDao, atLeast(0)).write(any(), any());
    verify(mockSpannerDao, atLeast(0)).updateShadowTable(any(), any());
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
            mockSpannerConfig,
            testSourceDbTimezoneOffset,
            testSourceSchema,
            "shadow_",
            "skip",
            500,
            "mysql",
            null,
            mockDdlView,
            mockShadowTableDdlView,
            "src/test/resources/sourceWriterUTSession.json",
            "",
            "",
            "");
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
            mockSpannerConfig,
            testSourceDbTimezoneOffset,
            testSourceSchema,
            "shadow_",
            "skip",
            500,
            "mysql",
            null,
            mockDdlView,
            mockShadowTableDdlView,
            "src/test/resources/sourceWriterUTSession.json",
            "",
            "",
            "");
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
            mockSpannerConfig,
            testSourceDbTimezoneOffset,
            testSourceSchema,
            "shadow_",
            "skip",
            500,
            "mysql",
            null,
            mockDdlView,
            mockShadowTableDdlView,
            "src/test/resources/sourceWriterUTSession.json",
            "",
            "",
            "");
    ObjectMapper mapper = new ObjectMapper();
    mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    sourceWriterFn.setObjectMapper(mapper);
    sourceWriterFn.setSpannerDao(mockSpannerDao);
    sourceWriterFn.processElement(processContext);
    ArgumentCaptor<String> argumentCaptor = ArgumentCaptor.forClass(String.class);
    verify(processContext, atLeast(1))
        .output(eq(Constants.PERMANENT_ERROR_TAG), argumentCaptor.capture());
    ChangeStreamErrorRecord actualError =
        gson.fromJson(argumentCaptor.getValue(), ChangeStreamErrorRecord.class);
    assertTrue(actualError.getErrorMessage().contains("Required key id not found in change event"));
  }

  @Test
  public void testRetryableError() throws Exception {
    TrimmedShardedDataChangeRecord record = getParent2TrimmedDataChangeRecord("shardA");
    record.setShard("shardA");
    when(processContext.element()).thenReturn(KV.of(1L, record));
    SourceWriterFn sourceWriterFn =
        new SourceWriterFn(
            ImmutableList.of(testShard),
            mockSpannerConfig,
            testSourceDbTimezoneOffset,
            testSourceSchema,
            "shadow_",
            "skip",
            500,
            "mysql",
            null,
            mockDdlView,
            mockShadowTableDdlView,
            "src/test/resources/sourceWriterUTSession.json",
            "",
            "",
            "");
    ObjectMapper mapper = new ObjectMapper();
    mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    sourceWriterFn.setObjectMapper(mapper);
    sourceWriterFn.setSpannerDao(mockSpannerDao);
    sourceWriterFn.processElement(processContext);
    ArgumentCaptor<String> argumentCaptor = ArgumentCaptor.forClass(String.class);
    verify(processContext, atLeast(1))
        .output(eq(Constants.RETRYABLE_ERROR_TAG), argumentCaptor.capture());
    ChangeStreamErrorRecord actualError =
        gson.fromJson(argumentCaptor.getValue(), ChangeStreamErrorRecord.class);
    assertTrue(actualError.getErrorMessage().contains("Test exception"));
  }

  @Test
  public void testRetryableErrorForForeignKey() throws Exception {
    TrimmedShardedDataChangeRecord record = getChild21TrimmedDataChangeRecord("shardA", 2300);
    record.setShard("shardA");
    when(processContext.element()).thenReturn(KV.of(1L, record));
    SourceWriterFn sourceWriterFn =
        new SourceWriterFn(
            ImmutableList.of(testShard),
            mockSpannerConfig,
            testSourceDbTimezoneOffset,
            testSourceSchema,
            "shadow_",
            "skip",
            500,
            "mysql",
            null,
            mockDdlView,
            mockShadowTableDdlView,
            "src/test/resources/sourceWriterUTSession.json",
            "",
            "",
            "");
    ObjectMapper mapper = new ObjectMapper();
    mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    sourceWriterFn.setObjectMapper(mapper);
    sourceWriterFn.setSourceProcessor(sourceProcessor);
    sourceWriterFn.setSpannerDao(mockSpannerDao);
    sourceWriterFn.processElement(processContext);
    ArgumentCaptor<String> argumentCaptor = ArgumentCaptor.forClass(String.class);
    verify(processContext, atLeast(1))
        .output(eq(Constants.RETRYABLE_ERROR_TAG), argumentCaptor.capture());
    ChangeStreamErrorRecord actualError =
        gson.fromJson(argumentCaptor.getValue(), ChangeStreamErrorRecord.class);
    assertTrue(actualError.getErrorMessage().contains("a foreign key constraint fails"));
  }

  @Test
  public void testRetryableErrorConnectionFailure() throws Exception {
    TrimmedShardedDataChangeRecord record = getChild21TrimmedDataChangeRecord("shardA", 1161);
    record.setShard("shardA");
    when(processContext.element()).thenReturn(KV.of(1L, record));
    SourceWriterFn sourceWriterFn =
        new SourceWriterFn(
            ImmutableList.of(testShard),
            mockSpannerConfig,
            testSourceDbTimezoneOffset,
            testSourceSchema,
            "shadow_",
            "skip",
            500,
            "mysql",
            null,
            mockDdlView,
            mockShadowTableDdlView,
            "src/test/resources/sourceWriterUTSession.json",
            "",
            "",
            "");
    ObjectMapper mapper = new ObjectMapper();
    mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    sourceWriterFn.setObjectMapper(mapper);
    sourceWriterFn.setSourceProcessor(sourceProcessor);
    sourceWriterFn.setSpannerDao(mockSpannerDao);
    sourceWriterFn.processElement(processContext);
    ArgumentCaptor<String> argumentCaptor = ArgumentCaptor.forClass(String.class);
    verify(processContext, atLeast(1))
        .output(eq(Constants.RETRYABLE_ERROR_TAG), argumentCaptor.capture());
    ChangeStreamErrorRecord actualError =
        gson.fromJson(argumentCaptor.getValue(), ChangeStreamErrorRecord.class);
    assertTrue(actualError.getErrorMessage().contains("transient connection error"));
  }

  @Test
  public void testPermanentConnectionFailure() throws Exception {
    TrimmedShardedDataChangeRecord record = getChild21TrimmedDataChangeRecord("shardA", 4242);
    record.setShard("shardA");
    when(processContext.element()).thenReturn(KV.of(1L, record));
    SourceWriterFn sourceWriterFn =
        new SourceWriterFn(
            ImmutableList.of(testShard),
            mockSpannerConfig,
            testSourceDbTimezoneOffset,
            testSourceSchema,
            "shadow_",
            "skip",
            500,
            "mysql",
            null,
            mockDdlView,
            mockShadowTableDdlView,
            "src/test/resources/sourceWriterUTSession.json",
            "",
            "",
            "");
    ObjectMapper mapper = new ObjectMapper();
    mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    sourceWriterFn.setObjectMapper(mapper);
    sourceWriterFn.setSourceProcessor(sourceProcessor);
    sourceWriterFn.setSpannerDao(mockSpannerDao);
    sourceWriterFn.processElement(processContext);
    ArgumentCaptor<String> argumentCaptor = ArgumentCaptor.forClass(String.class);
    verify(processContext, atLeast(1))
        .output(eq(Constants.PERMANENT_ERROR_TAG), argumentCaptor.capture());
    ChangeStreamErrorRecord actualError =
        gson.fromJson(argumentCaptor.getValue(), ChangeStreamErrorRecord.class);
    assertTrue(actualError.getErrorMessage().contains("permanent connection error"));
  }

  @Test
  public void testGenericExceptionIsRetriable() throws Exception {
    TrimmedShardedDataChangeRecord record = getChild21TrimmedDataChangeRecord("shardA", 12345);
    record.setShard("shardA");
    when(processContext.element()).thenReturn(KV.of(1L, record));
    SourceWriterFn sourceWriterFn =
        new SourceWriterFn(
            ImmutableList.of(testShard),
            mockSpannerConfig,
            testSourceDbTimezoneOffset,
            testSourceSchema,
            "shadow_",
            "skip",
            500,
            "mysql",
            null,
            mockDdlView,
            mockShadowTableDdlView,
            "src/test/resources/sourceWriterUTSession.json",
            "",
            "",
            "");
    ObjectMapper mapper = new ObjectMapper();
    mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    sourceWriterFn.setObjectMapper(mapper);
    sourceWriterFn.setSourceProcessor(sourceProcessor);
    sourceWriterFn.setSpannerDao(mockSpannerDao);
    sourceWriterFn.processElement(processContext);
    ArgumentCaptor<String> argumentCaptor = ArgumentCaptor.forClass(String.class);
    verify(processContext, atLeast(1))
        .output(eq(Constants.RETRYABLE_ERROR_TAG), argumentCaptor.capture());
    ChangeStreamErrorRecord actualError =
        gson.fromJson(argumentCaptor.getValue(), ChangeStreamErrorRecord.class);
    assertTrue(actualError.getErrorMessage().contains("generic exception"));
  }

  @Test
  public void testPermanentErrorWithSQLSyntaxErrorException() throws Exception {
    TrimmedShardedDataChangeRecord record = getChild21TrimmedDataChangeRecord("shardA", 6666);
    record.setShard("shardA");
    when(processContext.element()).thenReturn(KV.of(1L, record));
    SourceWriterFn sourceWriterFn =
        new SourceWriterFn(
            ImmutableList.of(testShard),
            mockSpannerConfig,
            testSourceDbTimezoneOffset,
            testSourceSchema,
            "shadow_",
            "skip",
            500,
            "mysql",
            null,
            mockDdlView,
            mockShadowTableDdlView,
            "src/test/resources/sourceWriterUTSession.json",
            "",
            "",
            "");
    ObjectMapper mapper = new ObjectMapper();
    mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    sourceWriterFn.setObjectMapper(mapper);
    sourceWriterFn.setSourceProcessor(sourceProcessor);
    sourceWriterFn.setSpannerDao(mockSpannerDao);
    sourceWriterFn.processElement(processContext);
    ArgumentCaptor<String> argumentCaptor = ArgumentCaptor.forClass(String.class);
    verify(processContext, atLeast(1))
        .output(eq(Constants.PERMANENT_ERROR_TAG), argumentCaptor.capture());
    ChangeStreamErrorRecord actualError =
        gson.fromJson(argumentCaptor.getValue(), ChangeStreamErrorRecord.class);
    assertTrue(actualError.getErrorMessage().contains("sql syntax error"));
  }

  @Test
  public void testPermanentErrorWithSQLDataException() throws Exception {
    TrimmedShardedDataChangeRecord record = getChild21TrimmedDataChangeRecord("shardA", 7777);
    record.setShard("shardA");
    when(processContext.element()).thenReturn(KV.of(1L, record));
    SourceWriterFn sourceWriterFn =
        new SourceWriterFn(
            ImmutableList.of(testShard),
            mockSpannerConfig,
            testSourceDbTimezoneOffset,
            testSourceSchema,
            "shadow_",
            "skip",
            500,
            "mysql",
            null,
            mockDdlView,
            mockShadowTableDdlView,
            "src/test/resources/sourceWriterUTSession.json",
            "",
            "",
            "");
    ObjectMapper mapper = new ObjectMapper();
    mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    sourceWriterFn.setObjectMapper(mapper);
    sourceWriterFn.setSourceProcessor(sourceProcessor);
    sourceWriterFn.setSpannerDao(mockSpannerDao);
    sourceWriterFn.processElement(processContext);
    ArgumentCaptor<String> argumentCaptor = ArgumentCaptor.forClass(String.class);
    verify(processContext, atLeast(1))
        .output(eq(Constants.PERMANENT_ERROR_TAG), argumentCaptor.capture());
    ChangeStreamErrorRecord actualError =
        gson.fromJson(argumentCaptor.getValue(), ChangeStreamErrorRecord.class);
    assertTrue(actualError.getErrorMessage().contains("sql data error"));
  }

  @Test
  public void testRetryableSentinelShardId() throws Exception {
    TrimmedShardedDataChangeRecord record =
        getParent1TrimmedDataChangeRecord(Constants.RETRYABLE_SENTINEL_SHARD_ID);
    record.setShard(Constants.RETRYABLE_SENTINEL_SHARD_ID);
    when(processContext.element()).thenReturn(KV.of(1L, record));

    SourceWriterFn sourceWriterFn =
        new SourceWriterFn(
            ImmutableList.of(testShard),
            mockSpannerConfig,
            testSourceDbTimezoneOffset,
            testSourceSchema,
            "shadow_",
            "skip",
            500,
            "mysql",
            null,
            mockDdlView,
            mockShadowTableDdlView,
            "src/test/resources/sourceWriterUTSession.json",
            "",
            "",
            "");
    ObjectMapper mapper = new ObjectMapper();
    mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    sourceWriterFn.setObjectMapper(mapper);
    sourceWriterFn.processElement(processContext);

    verify(processContext, atLeast(1)).output(eq(Constants.RETRYABLE_ERROR_TAG), any());
  }

  @Test
  public void testSevereSentinelShardId() throws Exception {
    TrimmedShardedDataChangeRecord record =
        getParent1TrimmedDataChangeRecord(Constants.SEVERE_SENTINEL_SHARD_ID);
    record.setShard(Constants.SEVERE_SENTINEL_SHARD_ID);
    when(processContext.element()).thenReturn(KV.of(1L, record));

    SourceWriterFn sourceWriterFn =
        new SourceWriterFn(
            ImmutableList.of(testShard),
            mockSpannerConfig,
            testSourceDbTimezoneOffset,
            testSourceSchema,
            "shadow_",
            "skip",
            500,
            "mysql",
            null,
            mockDdlView,
            mockShadowTableDdlView,
            "src/test/resources/sourceWriterUTSession.json",
            "",
            "",
            "");
    ObjectMapper mapper = new ObjectMapper();
    mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    sourceWriterFn.setObjectMapper(mapper);
    sourceWriterFn.processElement(processContext);

    verify(processContext, atLeast(1)).output(eq(Constants.PERMANENT_ERROR_TAG), any());
  }

  @Test
  public void testPermanentErrorWithInvalidTransformationException() throws Exception {
    TrimmedShardedDataChangeRecord record = getChild21TrimmedDataChangeRecord("shardA", 8888);
    record.setShard("shardA");
    when(processContext.element()).thenReturn(KV.of(1L, record));
    SourceWriterFn sourceWriterFn =
        new SourceWriterFn(
            ImmutableList.of(testShard),
            mockSpannerConfig,
            testSourceDbTimezoneOffset,
            testSourceSchema,
            "shadow_",
            "skip",
            500,
            "mysql",
            null,
            mockDdlView,
            mockShadowTableDdlView,
            "src/test/resources/sourceWriterUTSession.json",
            "",
            "",
            "");
    ObjectMapper mapper = new ObjectMapper();
    mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    sourceWriterFn.setObjectMapper(mapper);
    sourceWriterFn.setSourceProcessor(sourceProcessor);
    sourceWriterFn.setSpannerDao(mockSpannerDao);
    sourceWriterFn.processElement(processContext);
    ArgumentCaptor<String> argumentCaptor = ArgumentCaptor.forClass(String.class);
    verify(processContext, atLeast(1))
        .output(eq(Constants.PERMANENT_ERROR_TAG), argumentCaptor.capture());
    ChangeStreamErrorRecord actualError =
        gson.fromJson(argumentCaptor.getValue(), ChangeStreamErrorRecord.class);
    assertTrue(actualError.getErrorMessage().contains("invalid transformation"));
  }

  @Test
  public void testPermanentErrorWithChangeEventConvertorException() throws Exception {
    TrimmedShardedDataChangeRecord record = getChild21TrimmedDataChangeRecord("shardA", 9999);
    record.setShard("shardA");
    when(processContext.element()).thenReturn(KV.of(1L, record));
    SourceWriterFn sourceWriterFn =
        new SourceWriterFn(
            ImmutableList.of(testShard),
            mockSpannerConfig,
            testSourceDbTimezoneOffset,
            testSourceSchema,
            "shadow_",
            "skip",
            500,
            "mysql",
            null,
            mockDdlView,
            mockShadowTableDdlView,
            "src/test/resources/sourceWriterUTSession.json",
            "",
            "",
            "");
    ObjectMapper mapper = new ObjectMapper();
    mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    sourceWriterFn.setObjectMapper(mapper);
    sourceWriterFn.setSourceProcessor(sourceProcessor);
    sourceWriterFn.setSpannerDao(mockSpannerDao);
    sourceWriterFn.processElement(processContext);
    ArgumentCaptor<String> argumentCaptor = ArgumentCaptor.forClass(String.class);
    verify(processContext, atLeast(1))
        .output(eq(Constants.PERMANENT_ERROR_TAG), argumentCaptor.capture());
    ChangeStreamErrorRecord actualError =
        gson.fromJson(argumentCaptor.getValue(), ChangeStreamErrorRecord.class);
    assertTrue(actualError.getErrorMessage().contains("change event convertor error"));
  }

  @Test
  public void testRetryableErrorWithSpannerException() throws Exception {
    TrimmedShardedDataChangeRecord record = getChild21TrimmedDataChangeRecord("shardA", 1111);
    record.setShard("shardA");
    when(processContext.element()).thenReturn(KV.of(1L, record));
    SourceWriterFn sourceWriterFn =
        new SourceWriterFn(
            ImmutableList.of(testShard),
            mockSpannerConfig,
            testSourceDbTimezoneOffset,
            testSourceSchema,
            "shadow_",
            "skip",
            500,
            "mysql",
            null,
            mockDdlView,
            mockShadowTableDdlView,
            "src/test/resources/sourceWriterUTSession.json",
            "",
            "",
            "");
    ObjectMapper mapper = new ObjectMapper();
    mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    sourceWriterFn.setObjectMapper(mapper);
    sourceWriterFn.setSourceProcessor(sourceProcessor);
    sourceWriterFn.setSpannerDao(mockSpannerDao);
    sourceWriterFn.processElement(processContext);
    ArgumentCaptor<String> argumentCaptor = ArgumentCaptor.forClass(String.class);
    verify(processContext, atLeast(1))
        .output(eq(Constants.RETRYABLE_ERROR_TAG), argumentCaptor.capture());
    ChangeStreamErrorRecord actualError =
        gson.fromJson(argumentCaptor.getValue(), ChangeStreamErrorRecord.class);
    assertTrue(actualError.getErrorMessage().contains("test spanner"));
  }

  @Test
  public void testRetryableErrorWithIllegalArgumentException() throws Exception {
    TrimmedShardedDataChangeRecord record = getChild21TrimmedDataChangeRecord("shardA", 2222);
    record.setShard("shardA");
    when(processContext.element()).thenReturn(KV.of(1L, record));
    SourceWriterFn sourceWriterFn =
        new SourceWriterFn(
            ImmutableList.of(testShard),
            mockSpannerConfig,
            testSourceDbTimezoneOffset,
            testSourceSchema,
            "shadow_",
            "skip",
            500,
            "mysql",
            null,
            mockDdlView,
            mockShadowTableDdlView,
            "src/test/resources/sourceWriterUTSession.json",
            "",
            "",
            "");
    ObjectMapper mapper = new ObjectMapper();
    mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    sourceWriterFn.setObjectMapper(mapper);
    sourceWriterFn.setSourceProcessor(sourceProcessor);
    sourceWriterFn.setSpannerDao(mockSpannerDao);
    sourceWriterFn.processElement(processContext);
    ArgumentCaptor<String> argumentCaptor = ArgumentCaptor.forClass(String.class);
    verify(processContext, atLeast(1))
        .output(eq(Constants.RETRYABLE_ERROR_TAG), argumentCaptor.capture());
    ChangeStreamErrorRecord actualError =
        gson.fromJson(argumentCaptor.getValue(), ChangeStreamErrorRecord.class);
    assertTrue(actualError.getErrorMessage().contains("illegal argument"));
  }

  @Test
  public void testPlainSpannerExceptionIsRetryable() throws Exception {
    TrimmedShardedDataChangeRecord record = getParent1TrimmedDataChangeRecord("shardA");
    record.setShard("shardA");
    when(processContext.element()).thenReturn(KV.of(1L, record));
    // Override the default behavior of the transaction runner for this test
    when(mockTransactionRunner.run(any(TransactionRunner.TransactionCallable.class)))
        .thenThrow(
            SpannerExceptionFactory.newSpannerException(
                ErrorCode.UNKNOWN, "plain spanner exception"));

    SourceWriterFn sourceWriterFn =
        new SourceWriterFn(
            ImmutableList.of(testShard),
            mockSpannerConfig,
            testSourceDbTimezoneOffset,
            testSourceSchema,
            "shadow_",
            "skip",
            500,
            "mysql",
            null,
            mockDdlView,
            mockShadowTableDdlView,
            "src/test/resources/sourceWriterUTSession.json",
            "",
            "",
            "");
    ObjectMapper mapper = new ObjectMapper();
    mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    sourceWriterFn.setObjectMapper(mapper);
    sourceWriterFn.setSourceProcessor(sourceProcessor);
    sourceWriterFn.setSpannerDao(mockSpannerDao);
    sourceWriterFn.processElement(processContext);

    ArgumentCaptor<String> argumentCaptor = ArgumentCaptor.forClass(String.class);
    verify(processContext, atLeast(1))
        .output(eq(Constants.RETRYABLE_ERROR_TAG), argumentCaptor.capture());
    ChangeStreamErrorRecord actualError =
        gson.fromJson(argumentCaptor.getValue(), ChangeStreamErrorRecord.class);
    assertTrue(actualError.getErrorMessage().contains("plain spanner exception"));
  }

  @Test
  public void testDMLEmpty() throws Exception {
    TrimmedShardedDataChangeRecord record = getTrimmedDataChangeRecordToSimulateNullDML("shardA");
    record.setShard("shardA");
    when(processContext.element()).thenReturn(KV.of(1L, record));
    Ddl ddlForNullDML = testDdlForNullDML();
    when(processContext.sideInput(mockDdlView)).thenReturn(ddlForNullDML);
    when(processContext.sideInput(mockShadowTableDdlView)).thenReturn(ddlForNullDML);

    SourceWriterFn sourceWriterFn =
        new SourceWriterFn(
            ImmutableList.of(testShard),
            mockSpannerConfig,
            testSourceDbTimezoneOffset,
            testSourceSchema,
            "shadow_",
            "skip",
            500,
            "mysql",
            null,
            mockDdlView,
            mockShadowTableDdlView,
            "src/test/resources/sourceWriterUTSession.json",
            "",
            "",
            "");
    ObjectMapper mapper = new ObjectMapper();
    mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    sourceWriterFn.setObjectMapper(mapper);
    sourceWriterFn.setSpannerDao(mockSpannerDao);
    sourceWriterFn.setSourceProcessor(sourceProcessor);
    sourceWriterFn.processElement(processContext);
    verify(mockSqlDao, never()).write(contains("567890"), any());
  }

  @Test
  public void testTeardown() throws Exception {
    SourceWriterFn sourceWriterFn =
        new SourceWriterFn(
            ImmutableList.of(testShard),
            mockSpannerConfig,
            testSourceDbTimezoneOffset,
            testSourceSchema,
            "shadow_",
            "skip",
            500,
            "mysql",
            null,
            mockDdlView,
            mockShadowTableDdlView,
            "src/test/resources/sourceWriterUTSession.json",
            "",
            "",
            "");
    sourceWriterFn.setSpannerDao(mockSpannerDao);
    sourceWriterFn.setSourceProcessor(mockSourceProcessor);
    sourceWriterFn.teardown();
    verify(mockSpannerDao).close();
    verify(mockSourceProcessor).close();
  }

  @Test
  public void testTeardownWithNulls() throws Exception {
    SourceWriterFn sourceWriterFn =
        new SourceWriterFn(
            ImmutableList.of(testShard),
            mockSpannerConfig,
            testSourceDbTimezoneOffset,
            testSourceSchema,
            "shadow_",
            "skip",
            500,
            "mysql",
            null,
            mockDdlView,
            mockShadowTableDdlView,
            "src/test/resources/sourceWriterUTSession.json",
            "",
            "",
            "");
    sourceWriterFn.teardown();
    // No exception thrown is success.
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
