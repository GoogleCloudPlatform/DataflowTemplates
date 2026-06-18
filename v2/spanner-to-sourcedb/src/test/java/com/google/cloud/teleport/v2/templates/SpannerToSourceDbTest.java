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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.TypedDriverOption;
import com.google.cloud.spanner.Options.RpcPriority;
import com.google.cloud.teleport.v2.cdc.dlq.DeadLetterQueueManager;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.migrations.shard.CassandraShard;
import com.google.cloud.teleport.v2.spanner.migrations.shard.Shard;
import com.google.cloud.teleport.v2.spanner.migrations.utils.CassandraDriverConfigLoader;
import com.google.cloud.teleport.v2.spanner.migrations.utils.JarFileReader;
import com.google.cloud.teleport.v2.spanner.sourceddl.CassandraInformationSchemaScanner;
import com.google.cloud.teleport.v2.spanner.sourceddl.MySqlInformationSchemaScanner;
import com.google.cloud.teleport.v2.spanner.sourceddl.PostgreSQLInformationSchemaScanner;
import com.google.cloud.teleport.v2.spanner.sourceddl.SourceDatabaseType;
import com.google.cloud.teleport.v2.spanner.sourceddl.SourceSchema;
import com.google.cloud.teleport.v2.templates.SpannerToSourceDb.Options;
import com.google.cloud.teleport.v2.templates.source.cassandra.CassandraSourceConnector;
import com.google.cloud.teleport.v2.templates.source.mysql.MySQLSourceConnector;
import com.google.cloud.teleport.v2.templates.source.postgres.PostgreSQLSourceConnector;
import com.google.common.collect.ImmutableMap;
import com.zaxxer.hikari.HikariDataSource;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Collections;
import java.util.List;
import org.apache.beam.runners.dataflow.options.DataflowPipelineDebugOptions;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.runners.dataflow.options.DataflowPipelineWorkerPoolOptions;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollectionView;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
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
  @Rule public TemporaryFolder tempFolder = new TemporaryFolder();

  @BeforeClass
  public static void setupFileSystem() {
    FileSystems.setDefaultPipelineOptions(TestPipeline.testingPipelineOptions());
  }

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
  public void testFetchSourceSchema_MySQL() throws Exception {
    SourceSchema dummySchema =
        SourceSchema.builder(SourceDatabaseType.MYSQL)
            .databaseName("testdb")
            .tables(ImmutableMap.of())
            .build();

    try (MockedConstruction<HikariDataSource> mockDsConstruction =
            Mockito.mockConstruction(
                HikariDataSource.class,
                (mockDs, context) -> {
                  when(mockDs.getConnection()).thenReturn(mock(Connection.class));
                });
         MockedConstruction<MySqlInformationSchemaScanner> mockScannerConstruction =
            Mockito.mockConstruction(
                MySqlInformationSchemaScanner.class,
                (mockScanner, context) -> {
                  when(mockScanner.scan()).thenReturn(dummySchema);
                })) {

      MySQLSourceConnector source =
          new MySQLSourceConnector();
      SourceSchema result = source.getSourceSchema(new Shard());
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
    when(mockStmt.executeQuery("SELECT @@read_only")).thenReturn(mockRs);
    when(mockRs.next()).thenReturn(true);
    when(mockRs.getInt(1)).thenReturn(0); // 0 means NOT read-only

    try (MockedConstruction<HikariDataSource> mockDsConstruction =
            Mockito.mockConstruction(
                HikariDataSource.class,
                (mockDs, context) -> {
                  when(mockDs.getConnection()).thenReturn(mockConn);
                })) {

      MySQLSourceConnector source =
          new MySQLSourceConnector();
      source.validateNotReadOnly(List.of(shard));
      // Should not throw exception!
    }
  }

  @Test(expected = RuntimeException.class)
  public void testValidateMySQLNotReadOnly_ReadOnly() throws Exception {
    Shard shard = new Shard();
    Connection mockConn = mock(Connection.class);
    Statement mockStmt = mock(Statement.class);
    ResultSet mockRs = mock(ResultSet.class);

    when(mockConn.createStatement()).thenReturn(mockStmt);
    when(mockStmt.executeQuery("SELECT @@read_only")).thenReturn(mockRs);
    when(mockRs.next()).thenReturn(true);
    when(mockRs.getInt(1)).thenReturn(1); // 1 means read-only

    try (MockedConstruction<HikariDataSource> mockDsConstruction =
            Mockito.mockConstruction(
                HikariDataSource.class,
                (mockDs, context) -> {
                  when(mockDs.getConnection()).thenReturn(mockConn);
                })) {

      MySQLSourceConnector source =
          new MySQLSourceConnector();
      source.validateNotReadOnly(List.of(shard));
    }
  }

  @Test
  public void testValidatePostgreSQLNotReadOnly_NoVariable() throws Exception {
    Shard shard = new Shard();
    Connection mockConn = mock(Connection.class);
    Statement mockStmt = mock(Statement.class);
    ResultSet mockRs = mock(ResultSet.class);

    when(mockConn.createStatement()).thenReturn(mockStmt);
    when(mockStmt.executeQuery("SELECT current_setting('transaction_read_only')")).thenReturn(mockRs);
    when(mockRs.next()).thenReturn(true);
    when(mockRs.getString(1)).thenReturn("off"); // "off" means NOT read-only

    try (MockedConstruction<HikariDataSource> mockDsConstruction =
            Mockito.mockConstruction(
                HikariDataSource.class,
                (mockDs, context) -> {
                  when(mockDs.getConnection()).thenReturn(mockConn);
                })) {

      PostgreSQLSourceConnector source = new PostgreSQLSourceConnector();
      source.validateNotReadOnly(List.of(shard));
      // Should not throw exception!
    }
  }

  @Test(expected = RuntimeException.class)
  public void testValidatePostgreSQLNotReadOnly_ReadOnly() throws Exception {
    Shard shard = new Shard();
    Connection mockConn = mock(Connection.class);
    Statement mockStmt = mock(Statement.class);
    ResultSet mockRs = mock(ResultSet.class);

    when(mockConn.createStatement()).thenReturn(mockStmt);
    when(mockStmt.executeQuery("SELECT current_setting('transaction_read_only')")).thenReturn(mockRs);
    when(mockRs.next()).thenReturn(true);
    when(mockRs.getString(1)).thenReturn("on"); // "on" means read-only

    try (MockedConstruction<HikariDataSource> mockDsConstruction =
            Mockito.mockConstruction(
                HikariDataSource.class,
                (mockDs, context) -> {
                  when(mockDs.getConnection()).thenReturn(mockConn);
                })) {

      PostgreSQLSourceConnector source = new PostgreSQLSourceConnector();
      source.validateNotReadOnly(List.of(shard));
    }
  }

  @Test
  public void testGetSourceSchema_PostgreSQL() throws Exception {
    SourceSchema dummySchema =
        SourceSchema.builder(SourceDatabaseType.POSTGRESQL)
            .databaseName("db")
            .tables(ImmutableMap.of())
            .build();

    try (MockedConstruction<HikariDataSource> mockDsConstruction =
            Mockito.mockConstruction(
                HikariDataSource.class,
                (mockDs, context) -> {
                  when(mockDs.getConnection()).thenReturn(mock(Connection.class));
                });
         MockedConstruction<PostgreSQLInformationSchemaScanner> mockScannerConstruction =
            Mockito.mockConstruction(
                PostgreSQLInformationSchemaScanner.class,
                (mockScanner, context) -> {
                  when(mockScanner.scan()).thenReturn(dummySchema);
                })) {

      PostgreSQLSourceConnector source = new PostgreSQLSourceConnector();
      SourceSchema result = source.getSourceSchema(new Shard());
      Assert.assertSame(dummySchema, result);
    }
  }

  @Test
  public void testGetSourceSchema_Cassandra() throws Exception {
    CassandraShard mockShard = mock(CassandraShard.class);
    when(mockShard.getKeySpaceName()).thenReturn("keyspace");

    SourceSchema dummySchema =
        SourceSchema.builder(SourceDatabaseType.CASSANDRA)
            .databaseName("db")
            .tables(ImmutableMap.of())
            .build();

    try (MockedStatic<CqlSession> mockedCqlSession = Mockito.mockStatic(CqlSession.class);
         MockedStatic<CassandraDriverConfigLoader> mockedConfigLoader = Mockito.mockStatic(CassandraDriverConfigLoader.class);
         MockedConstruction<CassandraInformationSchemaScanner> mockScannerConstruction =
            Mockito.mockConstruction(
                CassandraInformationSchemaScanner.class,
                (mockScanner, context) -> {
                  when(mockScanner.scan()).thenReturn(dummySchema);
                })) {

      CqlSessionBuilder mockBuilder = mock(CqlSessionBuilder.class);
      mockedCqlSession.when(CqlSession::builder).thenReturn(mockBuilder);
      when(mockBuilder.withConfigLoader(any())).thenReturn(mockBuilder);
      when(mockBuilder.build()).thenReturn(mock(CqlSession.class));

      mockedConfigLoader
          .when(() -> CassandraDriverConfigLoader.fromOptionsMap(any()))
          .thenReturn(mock(DriverConfigLoader.class));

      CassandraSourceConnector source = new CassandraSourceConnector();
      SourceSchema result = source.getSourceSchema(mockShard);
      Assert.assertSame(dummySchema, result);
    }
  }

  @Test
  public void testGetShardList_unsupportedSourceType() {
    RuntimeException exception =
        assertThrows(
            RuntimeException.class,
            () -> SpannerToSourceDb.getShardList("unsupported_db", "dummy/path.json"));
    assertNotNull(exception.getCause());
    assertEquals(IllegalArgumentException.class, exception.getCause().getClass());
  }

  @Test
  public void testGetShardList_fileNotFound() {
    RuntimeException exception =
        assertThrows(
            RuntimeException.class,
            () -> SpannerToSourceDb.getShardList("mysql", "non-existent-file.json"));
    assertNotNull(exception.getCause());
    assertEquals(RuntimeException.class, exception.getCause().getClass());
    assertEquals(
        "Failed to read configuration input file at non-existent-file.json. Make sure it is ASCII or UTF-8 encoded and contains a well-formed HOCON/JSON string.",
        exception.getCause().getMessage());
  }

  @Test
  public void testGetShardList_jdbc_validJsonWrapped() throws IOException {
    File tempFile = tempFolder.newFile("jdbc-config-wrapped.json");
    String wrappedJson =
        "{\n"
            + "  \"shardConfigs\": [\n"
            + "    {\n"
            + "      \"logicalShardId\": \"shard1\",\n"
            + "      \"host\": \"localhost\",\n"
            + "      \"port\": \"3306\",\n"
            + "      \"user\": \"test-user\",\n"
            + "      \"password\": \"secret-pass\",\n"
            + "      \"dbName\": \"testdb\"\n"
            + "    }\n"
            + "  ]\n"
            + "}";
    Files.writeString(tempFile.toPath(), wrappedJson);

    List<Shard> shards = SpannerToSourceDb.getShardList("mysql", tempFile.getAbsolutePath());
    assertNotNull(shards);
    assertEquals(1, shards.size());
    Shard shard = shards.get(0);
    assertEquals("shard1", shard.getLogicalShardId());
    assertEquals("localhost", shard.getHost());
    assertEquals("3306", shard.getPort());
    assertEquals("test-user", shard.getUserName());
    assertEquals("secret-pass", shard.getPassword());
    assertEquals("testdb", shard.getDbName());
  }

  @Test
  public void testGetShardList_cassandra_validConf() throws IOException {
    try (MockedStatic<JarFileReader> mockFileReader = mockStatic(JarFileReader.class)) {
      String testGcsPath = "gs://smt-test-bucket/cassandraConfig.conf";
      URL testUrl = com.google.common.io.Resources.getResource("test-cassandra-config.conf");
      mockFileReader
          .when(() -> JarFileReader.saveFilesLocally(testGcsPath))
          .thenReturn(new URL[] {testUrl});

      List<Shard> shards = SpannerToSourceDb.getShardList("cassandra", testGcsPath);
      assertNotNull(shards);
      assertEquals(1, shards.size());
      Shard shard = shards.get(0);
      assertEquals("127.0.0.1", shard.getHost());
      assertEquals("9042", shard.getPort());

      // Assert specific CassandraShard properties
      CassandraShard cassandraShard = (CassandraShard) shard;
      assertEquals("cassandra", cassandraShard.getUsername());
      assertEquals("my_keyspace", cassandraShard.getKeySpaceName());
      assertEquals(
          "cassandra",
          cassandraShard.getOptionsMap().get(TypedDriverOption.AUTH_PROVIDER_PASSWORD));
    }
  }
}
