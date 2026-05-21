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

import com.datastax.oss.driver.api.core.CqlSession;
import com.google.cloud.spanner.Options.RpcPriority;
import com.google.cloud.teleport.v2.cdc.dlq.DeadLetterQueueManager;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.migrations.shard.CassandraShard;
import com.google.cloud.teleport.v2.spanner.migrations.shard.Shard;
import com.google.cloud.teleport.v2.spanner.sourceddl.CassandraInformationSchemaScanner;
import com.google.cloud.teleport.v2.spanner.sourceddl.PostgreSQLInformationSchemaScanner;
import com.google.cloud.teleport.v2.spanner.sourceddl.SourceDatabaseType;
import com.google.cloud.teleport.v2.spanner.sourceddl.SourceSchema;
import com.google.cloud.teleport.v2.templates.SpannerToSourceDb.Options;
import com.google.cloud.teleport.v2.templates.constants.Constants;
import com.google.common.collect.ImmutableMap;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Collections;
import java.util.List;
import org.apache.beam.runners.dataflow.options.DataflowPipelineDebugOptions;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.runners.dataflow.options.DataflowPipelineWorkerPoolOptions;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollectionView;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
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
  public void testGetReadChangeStreamDoFn() {
    options.setChangeStreamName("testStream");
    options.setMetadataInstance("testInstance");
    options.setMetadataDatabase("testDB");
    options.setSpannerPriority(RpcPriority.HIGH);
    options.setStartTimestamp("");
    options.setEndTimestamp("");

    SpannerIO.ReadChangeStream readChangeStream =
        SpannerToSourceDb.getReadChangeStreamDoFn(options, mockSpannerConfig);
    Assert.assertNotNull(readChangeStream);
  }

  @Test
  public void testBuildDlqManager() {
    DataflowPipelineOptions dfOptions = options.as(DataflowPipelineOptions.class);
    dfOptions.setTempLocation("gs://test/temp");
    options.setDeadLetterQueueDirectory("");
    options.setDlqMaxRetryCount(3);

    DeadLetterQueueManager dlqManager = SpannerToSourceDb.buildDlqManager(options);
    Assert.assertNotNull(dlqManager);
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
          SourceSchema.builder(SourceDatabaseType.MYSQL)
              .databaseName("testdb")
              .tables(ImmutableMap.of())
              .build();

      mocked.when(() -> SpannerToSourceDb.getSourceSchema(any(), any())).thenReturn(dummySchema);

      Options options = mock(Options.class);
      List<Shard> shards = List.of(new Shard());

      SourceSchema result = SpannerToSourceDb.fetchSourceSchema(options, shards);

      Assert.assertSame(dummySchema, result);
    }
  }

  @Test
  public void testCalculateConnectionPoolSizePerWorker_Success() {
    int result = SpannerToSourceDb.calculateConnectionPoolSizePerWorker(10L, 2);
    Assert.assertEquals(5, result);
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

    try (MockedStatic<SpannerToSourceDb> mockedStatic =
        Mockito.mockStatic(SpannerToSourceDb.class, Mockito.CALLS_REAL_METHODS)) {
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
    when(options.getSourceType()).thenReturn(Constants.SOURCE_POSTGRESQL);
    List<Shard> shards = List.of(new Shard());

    SourceSchema dummySchema =
        SourceSchema.builder(SourceDatabaseType.POSTGRESQL)
            .databaseName("db")
            .tables(ImmutableMap.of())
            .build();

    try (MockedConstruction<PostgreSQLInformationSchemaScanner> mocked =
        Mockito.mockConstruction(
            PostgreSQLInformationSchemaScanner.class,
            (mock, context) -> {
              when(mock.scan()).thenReturn(dummySchema);
            })) {

      try (MockedStatic<SpannerToSourceDb> mockedStatic =
          Mockito.mockStatic(SpannerToSourceDb.class, Mockito.CALLS_REAL_METHODS)) {
        mockedStatic
            .when(() -> SpannerToSourceDb.createJdbcConnection(any(), any(), any()))
            .thenReturn(mock(Connection.class));

        SourceSchema result = SpannerToSourceDb.getSourceSchema(options, shards);
        Assert.assertSame(dummySchema, result);
      }
    }
  }

  @Test
  public void testGetSourceSchema_Cassandra() throws Exception {
    Options options = mock(Options.class);
    when(options.getSourceType()).thenReturn("cassandra");
    CassandraShard mockShard = mock(CassandraShard.class);
    when(mockShard.getKeySpaceName()).thenReturn("keyspace");
    List<Shard> shards = List.of(mockShard);

    SourceSchema dummySchema =
        SourceSchema.builder(SourceDatabaseType.CASSANDRA)
            .databaseName("db")
            .tables(ImmutableMap.of())
            .build();

    try (MockedConstruction<CassandraInformationSchemaScanner> mocked =
        Mockito.mockConstruction(
            CassandraInformationSchemaScanner.class,
            (mock, context) -> {
              when(mock.scan()).thenReturn(dummySchema);
            })) {

      try (MockedStatic<SpannerToSourceDb> mockedStatic =
          Mockito.mockStatic(SpannerToSourceDb.class, Mockito.CALLS_REAL_METHODS)) {
        mockedStatic
            .when(() -> SpannerToSourceDb.createCqlSession(any()))
            .thenReturn(mock(CqlSession.class));

        SourceSchema result = SpannerToSourceDb.getSourceSchema(options, shards);
        Assert.assertSame(dummySchema, result);
      }
    }
  }
}
