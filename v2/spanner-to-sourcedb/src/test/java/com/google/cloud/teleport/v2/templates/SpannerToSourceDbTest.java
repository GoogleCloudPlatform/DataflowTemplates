/*
 * Copyright (C) 2026 Google LLC
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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.migrations.shard.Shard;
import com.google.cloud.teleport.v2.spanner.sourceddl.SourceSchema;
import com.google.cloud.teleport.v2.templates.SpannerToSourceDb.Options;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Collections;
import java.util.List;
import org.apache.beam.runners.dataflow.options.DataflowPipelineDebugOptions;
import org.apache.beam.runners.dataflow.options.DataflowPipelineWorkerPoolOptions;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.DataChangeRecord;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class SpannerToSourceDbTest {

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  private Options options;
  @Mock private SourceSchema mockSourceSchema;
  @Mock private PCollectionView<Ddl> mockDdlView;
  @Mock private PCollectionView<Ddl> mockShadowTableDdlView;
  @Mock private SpannerConfig mockSpannerConfig;
  @Mock private SpannerConfig mockSpannerMetadataConfig;

  private List<Shard> shards;
  private Ddl dummyDdl;

  @Before
  public void setUp() {
    Shard shard = new Shard();
    shard.setLogicalShardId("shard1");
    shards = Collections.singletonList(shard);

    dummyDdl =
        Ddl.builder().createTable("shadow_T").column("c1").string().endColumn().endTable().build();

    options = PipelineOptionsFactory.as(Options.class);
    options.setRunMode("regular");
    options.setDeadLetterQueueDirectory("gs://test/dlq");
    options.setSkipDirectoryName("skip");
    options.setFiltrationMode("none");
    options.setSourceType("mysql");
    options.setMaxShardConnections(100L);
    options.setTransformationJarPath("");
    options.setTransformationClassName("");
    options.setTransformationCustomParameters("");
    options.setSessionFilePath("");
    options.setSchemaOverridesFilePath("");
    options.setTableOverrides("");
    options.setColumnOverrides("");

    DataflowPipelineDebugOptions debugOptions = options.as(DataflowPipelineDebugOptions.class);
    debugOptions.setNumberOfWorkerHarnessThreads(0);

    DataflowPipelineWorkerPoolOptions workerPoolOptions =
        options.as(DataflowPipelineWorkerPoolOptions.class);
    workerPoolOptions.setMaxNumWorkers(1);
  }

  @Test
  public void testBuildPipeline_RegularMode() {

    try (MockedStatic<SpannerToSourceDb> mockedSpannerToSourceDb =
        Mockito.mockStatic(SpannerToSourceDb.class)) {

      SpannerIO.ReadChangeStream mockReadChangeStream = mock(SpannerIO.ReadChangeStream.class);
      mockedSpannerToSourceDb
          .when(() -> SpannerToSourceDb.getReadChangeStreamDoFn(any(), any()))
          .thenReturn(mockReadChangeStream);

      PCollection<DataChangeRecord> dummyCollection =
          pipeline.apply(Create.empty(TypeDescriptor.of(DataChangeRecord.class)));
      when(mockReadChangeStream.expand(any())).thenReturn(dummyCollection);

      SpannerToSourceDb.buildPipeline(
          pipeline,
          options,
          mockSourceSchema,
          shards,
          mockDdlView,
          mockShadowTableDdlView,
          mockSpannerConfig,
          mockSpannerMetadataConfig,
          10,
          "multi_shard",
          System.currentTimeMillis(),
          1);

      pipeline.run();
    }
  }

  @Test
  public void testGetReadChangeStreamDoFn() {
    options.setChangeStreamName("testStream");
    options.setMetadataInstance("testInstance");
    options.setMetadataDatabase("testDB");
    options.setSpannerPriority(com.google.cloud.spanner.Options.RpcPriority.HIGH);
    options.setStartTimestamp("");
    options.setEndTimestamp("");

    org.apache.beam.sdk.io.gcp.spanner.SpannerIO.ReadChangeStream readChangeStream =
        SpannerToSourceDb.getReadChangeStreamDoFn(options, mockSpannerConfig);
    org.junit.Assert.assertNotNull(readChangeStream);
  }

  @Test
  public void testBuildDlqManager() {
    org.apache.beam.runners.dataflow.options.DataflowPipelineOptions dfOptions =
        options.as(org.apache.beam.runners.dataflow.options.DataflowPipelineOptions.class);
    dfOptions.setTempLocation("gs://test/temp");
    options.setDeadLetterQueueDirectory("");
    options.setDlqMaxRetryCount(3);

    com.google.cloud.teleport.v2.cdc.dlq.DeadLetterQueueManager dlqManager =
        SpannerToSourceDb.buildDlqManager(options);
    org.junit.Assert.assertNotNull(dlqManager);
  }

  @Test
  public void testValidateMySQLNotReadOnly_NotReadOnly() throws Exception {
    try (MockedStatic<SpannerToSourceDb> mocked =
        Mockito.mockStatic(SpannerToSourceDb.class, Mockito.CALLS_REAL_METHODS)) {
      Connection mockConnection = mock(Connection.class);
      mocked
          .when(() -> SpannerToSourceDb.createJdbcConnection(any(), any(), any()))
          .thenReturn(mockConnection);

      Statement mockStatement = mock(Statement.class);
      when(mockConnection.createStatement()).thenReturn(mockStatement);
      ResultSet mockResultSet = mock(ResultSet.class);
      when(mockStatement.executeQuery("SELECT @@read_only")).thenReturn(mockResultSet);
      when(mockResultSet.next()).thenReturn(true);
      when(mockResultSet.getInt(1)).thenReturn(0); // 0 means NOT read-only

      Shard shard = new Shard();
      shard.setLogicalShardId("shard1");
      List<Shard> shards = List.of(shard);
      SpannerToSourceDb.validateMySQLNotReadOnly(shards);

      // Verify that it didn't throw exception
      mocked.verify(() -> SpannerToSourceDb.createJdbcConnection(any(), any(), any()));
    }
  }

  @Test(expected = RuntimeException.class)
  public void testValidateMySQLNotReadOnly_ReadOnly() throws Exception {
    try (MockedStatic<SpannerToSourceDb> mocked =
        Mockito.mockStatic(SpannerToSourceDb.class, Mockito.CALLS_REAL_METHODS)) {
      Connection mockConnection = mock(Connection.class);
      mocked
          .when(() -> SpannerToSourceDb.createJdbcConnection(any(), any(), any()))
          .thenReturn(mockConnection);

      Statement mockStatement = mock(Statement.class);
      when(mockConnection.createStatement()).thenReturn(mockStatement);
      ResultSet mockResultSet = mock(ResultSet.class);
      when(mockStatement.executeQuery("SELECT @@read_only")).thenReturn(mockResultSet);
      when(mockResultSet.next()).thenReturn(true);
      when(mockResultSet.getInt(1)).thenReturn(1); // 1 means read-only

      Shard shard = new Shard();
      shard.setLogicalShardId("shard1");
      List<Shard> shards = List.of(shard);
      SpannerToSourceDb.validateMySQLNotReadOnly(shards);
    }
  }

  @Test
  public void testFetchSourceSchema_MySQL() throws Exception {
    try (MockedStatic<SpannerToSourceDb> mocked =
        Mockito.mockStatic(SpannerToSourceDb.class, Mockito.CALLS_REAL_METHODS)) {
      SourceSchema dummySchema =
          SourceSchema.builder(
                  com.google.cloud.teleport.v2.spanner.sourceddl.SourceDatabaseType.MYSQL)
              .databaseName("testdb")
              .tables(com.google.common.collect.ImmutableMap.of())
              .build();

      mocked.when(() -> SpannerToSourceDb.getSourceSchema(any(), any())).thenReturn(dummySchema);

      Options options = mock(Options.class);
      List<Shard> shards = List.of(new Shard());

      SourceSchema result = SpannerToSourceDb.fetchSourceSchema(options, shards);

      org.junit.Assert.assertSame(dummySchema, result);
    }
  }

  @Test
  public void testCalculateConnectionPoolSizePerWorker_Success() {
    int result = SpannerToSourceDb.calculateConnectionPoolSizePerWorker(10L, 2);
    org.junit.Assert.assertEquals(5, result);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCalculateConnectionPoolSizePerWorker_Failure() {
    SpannerToSourceDb.calculateConnectionPoolSizePerWorker(2L, 10);
  }

  @Test
  public void testValidateMySQLNotReadOnly_NoVariable() throws Exception {
    Shard shard = new Shard();
    Connection mockConn = mock(Connection.class);
    Statement mockStmt = mock(Statement.class);
    ResultSet mockRs = mock(ResultSet.class);

    when(mockConn.createStatement()).thenReturn(mockStmt);
    when(mockStmt.executeQuery("SHOW VARIABLES LIKE 'read_only'")).thenReturn(mockRs);
    when(mockRs.next()).thenReturn(false); // No rows!

    try (org.mockito.MockedStatic<SpannerToSourceDb> mockedStatic =
        org.mockito.Mockito.mockStatic(
            SpannerToSourceDb.class, org.mockito.Mockito.CALLS_REAL_METHODS)) {
      mockedStatic
          .when(() -> SpannerToSourceDb.createJdbcConnection(any(), any(), any()))
          .thenReturn(mockConn);

      SpannerToSourceDb.validateMySQLNotReadOnly(List.of(shard));
      // Should not throw exception!
    }
  }

  @Test
  public void testGetSourceSchema_PostgreSQL() throws Exception {
    Options options = mock(Options.class);
    when(options.getSourceType())
        .thenReturn(com.google.cloud.teleport.v2.templates.constants.Constants.SOURCE_POSTGRESQL);
    List<Shard> shards = List.of(new Shard());

    com.google.cloud.teleport.v2.spanner.sourceddl.SourceSchema dummySchema =
        com.google.cloud.teleport.v2.spanner.sourceddl.SourceSchema.builder(
                com.google.cloud.teleport.v2.spanner.sourceddl.SourceDatabaseType.POSTGRESQL)
            .databaseName("db")
            .tables(com.google.common.collect.ImmutableMap.of())
            .build();

    try (org.mockito.MockedConstruction<
            com.google.cloud.teleport.v2.spanner.sourceddl.PostgreSQLInformationSchemaScanner>
        mocked =
            org.mockito.Mockito.mockConstruction(
                com.google.cloud.teleport.v2.spanner.sourceddl.PostgreSQLInformationSchemaScanner
                    .class,
                (mock, context) -> {
                  when(mock.scan()).thenReturn(dummySchema);
                })) {

      try (org.mockito.MockedStatic<SpannerToSourceDb> mockedStatic =
          org.mockito.Mockito.mockStatic(
              SpannerToSourceDb.class, org.mockito.Mockito.CALLS_REAL_METHODS)) {
        mockedStatic
            .when(() -> SpannerToSourceDb.createJdbcConnection(any(), any(), any()))
            .thenReturn(mock(Connection.class));

        com.google.cloud.teleport.v2.spanner.sourceddl.SourceSchema result =
            SpannerToSourceDb.getSourceSchema(options, shards);
        org.junit.Assert.assertSame(dummySchema, result);
      }
    }
  }

  @Test
  public void testGetSourceSchema_Cassandra() throws Exception {
    Options options = mock(Options.class);
    when(options.getSourceType()).thenReturn("cassandra");
    com.google.cloud.teleport.v2.spanner.migrations.shard.CassandraShard mockShard =
        mock(com.google.cloud.teleport.v2.spanner.migrations.shard.CassandraShard.class);
    when(mockShard.getKeySpaceName()).thenReturn("keyspace");
    List<Shard> shards = List.of(mockShard);

    com.google.cloud.teleport.v2.spanner.sourceddl.SourceSchema dummySchema =
        com.google.cloud.teleport.v2.spanner.sourceddl.SourceSchema.builder(
                com.google.cloud.teleport.v2.spanner.sourceddl.SourceDatabaseType.CASSANDRA)
            .databaseName("db")
            .tables(com.google.common.collect.ImmutableMap.of())
            .build();

    try (org.mockito.MockedConstruction<
            com.google.cloud.teleport.v2.spanner.sourceddl.CassandraInformationSchemaScanner>
        mocked =
            org.mockito.Mockito.mockConstruction(
                com.google.cloud.teleport.v2.spanner.sourceddl.CassandraInformationSchemaScanner
                    .class,
                (mock, context) -> {
                  when(mock.scan()).thenReturn(dummySchema);
                })) {

      try (org.mockito.MockedStatic<SpannerToSourceDb> mockedStatic =
          org.mockito.Mockito.mockStatic(
              SpannerToSourceDb.class, org.mockito.Mockito.CALLS_REAL_METHODS)) {
        mockedStatic
            .when(() -> SpannerToSourceDb.createCqlSession(any()))
            .thenReturn(mock(com.datastax.oss.driver.api.core.CqlSession.class));

        com.google.cloud.teleport.v2.spanner.sourceddl.SourceSchema result =
            SpannerToSourceDb.getSourceSchema(options, shards);
        org.junit.Assert.assertSame(dummySchema, result);
      }
    }
  }
}
