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
import com.google.cloud.teleport.v2.spanner.migrations.schema.Schema;
import com.google.cloud.teleport.v2.spanner.migrations.shard.Shard;
import com.google.cloud.teleport.v2.spanner.migrations.utils.SessionFileReader;
import com.google.cloud.teleport.v2.templates.changestream.ChangeStreamErrorRecord;
import com.google.cloud.teleport.v2.templates.changestream.TrimmedShardedDataChangeRecord;
import com.google.cloud.teleport.v2.templates.constants.Constants;
import com.google.cloud.teleport.v2.templates.utils.MySqlDao;
import com.google.cloud.teleport.v2.templates.utils.SpannerDao;
import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import java.sql.SQLException;
import java.util.HashMap;
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

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class SourceWriterFnTest {
  @Rule public final transient TestPipeline pipeline = TestPipeline.create();
  @Rule public final MockitoRule mocktio = MockitoJUnit.rule();
  @Mock private MySqlDao mockMySqlDao;
  @Mock private SpannerDao mockSpannerDao;
  @Mock HashMap<String, MySqlDao> mockMySqlDaoMap;
  @Mock private SpannerConfig mockSpannerConfig;
  @Mock private DoFn.ProcessContext processContext;
  private static Gson gson = new Gson();

  private Shard testShard;
  private Schema testSchema;
  private Ddl testDdl;
  private String testSourceDbTimezoneOffset;

  @Before
  public void doBeforeEachTest() throws Exception {
    when(mockMySqlDaoMap.get(any())).thenReturn(mockMySqlDao);
    when(mockSpannerDao.getProcessedCommitTimestamp(eq("shadow_parent1"), any())).thenReturn(null);
    when(mockSpannerDao.getProcessedCommitTimestamp(eq("shadow_parent2"), any()))
        .thenThrow(new IllegalStateException("Test exception"));
    when(mockSpannerDao.getProcessedCommitTimestamp(eq("shadow_child11"), any()))
        .thenReturn(Timestamp.parseTimestamp("2025-02-02T00:00:00Z"));
    when(mockSpannerDao.getProcessedCommitTimestamp(eq("shadow_child21"), any())).thenReturn(null);
    doNothing().when(mockSpannerDao).updateProcessedCommitTimestamp(any());
    doThrow(new SQLException("a foreign key constraint fails"))
        .when(mockMySqlDao)
        .write(contains("child21"));
    doNothing().when(mockMySqlDao).write(contains("parent1"));
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
            "skip");
    ObjectMapper mapper = new ObjectMapper();
    mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    sourceWriterFn.setObjectMapper(mapper);
    sourceWriterFn.setSpannerDao(mockSpannerDao);
    sourceWriterFn.setMySqlDaoMap(mockMySqlDaoMap);
    sourceWriterFn.processElement(processContext);
    verify(mockSpannerDao, atLeast(1)).getProcessedCommitTimestamp(any(), any());
    verify(mockMySqlDao, never()).write(any());
    verify(mockSpannerDao, never()).updateProcessedCommitTimestamp(any());
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
            "skip");
    ObjectMapper mapper = new ObjectMapper();
    mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    sourceWriterFn.setObjectMapper(mapper);
    sourceWriterFn.setSpannerDao(mockSpannerDao);
    sourceWriterFn.setMySqlDaoMap(mockMySqlDaoMap);
    sourceWriterFn.processElement(processContext);
    verify(mockSpannerDao, atLeast(1)).getProcessedCommitTimestamp(any(), any());
    verify(mockMySqlDao, atLeast(1)).write(any());
    verify(mockSpannerDao, atLeast(1)).updateProcessedCommitTimestamp(any());
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
            "skip");
    ObjectMapper mapper = new ObjectMapper();
    mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    sourceWriterFn.setObjectMapper(mapper);
    sourceWriterFn.setSpannerDao(mockSpannerDao);
    sourceWriterFn.setMySqlDaoMap(mockMySqlDaoMap);
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
            "skip");
    ObjectMapper mapper = new ObjectMapper();
    mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    sourceWriterFn.setObjectMapper(mapper);
    sourceWriterFn.setSpannerDao(mockSpannerDao);
    sourceWriterFn.setMySqlDaoMap(mockMySqlDaoMap);
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
            "skip");
    ObjectMapper mapper = new ObjectMapper();
    mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    sourceWriterFn.setObjectMapper(mapper);
    sourceWriterFn.setSpannerDao(mockSpannerDao);
    sourceWriterFn.setMySqlDaoMap(mockMySqlDaoMap);
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
            "skip");
    ObjectMapper mapper = new ObjectMapper();
    mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    sourceWriterFn.setObjectMapper(mapper);
    sourceWriterFn.setSpannerDao(mockSpannerDao);
    sourceWriterFn.setMySqlDaoMap(mockMySqlDaoMap);
    sourceWriterFn.processElement(processContext);
    String jsonRec = gson.toJson(record, TrimmedShardedDataChangeRecord.class);
    ChangeStreamErrorRecord errorRecord = new ChangeStreamErrorRecord(jsonRec, "Test exception");
    verify(processContext, atLeast(1))
        .output(
            Constants.RETRYABLE_ERROR_TAG, gson.toJson(errorRecord, ChangeStreamErrorRecord.class));
  }

  @Test
  public void testRetryableErrorForForeignKey() throws Exception {
    TrimmedShardedDataChangeRecord record = getChild21TrimmedDataChangeRecord("shardA");
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
            "skip");
    ObjectMapper mapper = new ObjectMapper();
    mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    sourceWriterFn.setObjectMapper(mapper);
    sourceWriterFn.setSpannerDao(mockSpannerDao);
    sourceWriterFn.setMySqlDaoMap(mockMySqlDaoMap);
    sourceWriterFn.processElement(processContext);
    String jsonRec = gson.toJson(record, TrimmedShardedDataChangeRecord.class);
    ChangeStreamErrorRecord errorRecord =
        new ChangeStreamErrorRecord(jsonRec, "a foreign key constraint fails");
    verify(processContext, atLeast(1))
        .output(
            Constants.RETRYABLE_ERROR_TAG, gson.toJson(errorRecord, ChangeStreamErrorRecord.class));
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
        "recordSeq",
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
        "recordSeq",
        "parent1",
        new Mod("{\"id\": \"42\"}", "{}", "{ \"migration_shard_id\": \"" + shardId + "\"}"),
        ModType.valueOf("INSERT"),
        1,
        "");
  }

  private TrimmedShardedDataChangeRecord getParent1IncorrectTrimmedDataChangeRecord(
      String shardId) {
    return new TrimmedShardedDataChangeRecord(
        Timestamp.parseTimestamp("2020-12-01T10:15:30.000Z"),
        "serverTxnId",
        "recordSeq",
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
        "recordSeq",
        "parent2",
        new Mod("{\"id\": \"42\"}", "{}", "{ \"migration_shard_id\": \"" + shardId + "\"}"),
        ModType.valueOf("INSERT"),
        1,
        "");
  }

  private TrimmedShardedDataChangeRecord getChild21TrimmedDataChangeRecord(String shardId) {
    return new TrimmedShardedDataChangeRecord(
        Timestamp.parseTimestamp("2024-12-01T10:15:30.000Z"),
        "serverTxnId",
        "recordSeq",
        "child21",
        new Mod(
            "{\"child_id\": \"42\" , \"parent_id\": \"42\"}",
            "{}",
            "{ \"migration_shard_id\": \"" + shardId + "\"}"),
        ModType.valueOf("INSERT"),
        1,
        "");
  }
}
