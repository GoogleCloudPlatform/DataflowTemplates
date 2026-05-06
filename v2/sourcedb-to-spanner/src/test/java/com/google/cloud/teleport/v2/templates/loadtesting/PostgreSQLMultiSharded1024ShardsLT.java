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
package com.google.cloud.teleport.v2.templates.loadtesting;

import static com.google.cloud.teleport.v2.templates.loadtesting.CloudSqlShardOrchestrator.POSTGRES_14;
import static com.google.common.truth.Truth.assertThat;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.spanner.Struct;
import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateLoadTest;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.iowrapper.config.SQLDialect;
import com.google.cloud.teleport.v2.templates.SourceDbToSpanner;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineLauncher.LaunchConfig;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.artifacts.GcsArtifact;
import org.apache.beam.it.gcp.cloudsql.CloudSqlResourceManager;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A load test for {@link SourceDbToSpanner} Flex template which tests a massive 1,024 shards
 * migration from PostgreSQL to Spanner.
 *
 * <p>This test validates the graph size optimization by ensuring that a single Dataflow job can
 * successfully manage connections and data movement for thousands of tables across 32 physical
 * PostgreSQL instances.
 */
@Category({TemplateLoadTest.class, SkipDirectRunnerTest.class})
@TemplateLoadTest(SourceDbToSpanner.class)
@RunWith(JUnit4.class)
public class PostgreSQLMultiSharded1024ShardsLT extends SourceDbToSpannerLTBase {
  private static final Logger LOG =
      LoggerFactory.getLogger(PostgreSQLMultiSharded1024ShardsLT.class);
  private Instant startTime;

  private CloudSqlShardOrchestrator orchestrator;

  private final int numPhysicalInstances = 32;
  private final int numLogicalInstances = 32;

  private final Boolean skipBaseCleanup = false;

  @Before
  public void setUp() throws IOException {
    LOG.info("Began Setup for 1,024 Shards test (PostgreSQL)");
    super.setUp();
    startTime = Instant.now();

    String password = System.getProperty("cloudProxyPassword");
    if (password == null || password.isEmpty()) {
      throw new IllegalArgumentException("cloudProxyPassword system property must be set");
    }

    spannerResourceManager =
        SpannerResourceManager.builder(testName, project, region)
            .maybeUseStaticInstance()
            .setMonitoringClient(monitoringClient)
            .build();

    gcsResourceManager = createSpannerLTGcsResourceManager();
    this.dialect = SQLDialect.POSTGRESQL;

    orchestrator =
        new CloudSqlShardOrchestrator(
            SQLDialect.POSTGRESQL, POSTGRES_14, project, region, gcsResourceManager);
  }

  @After
  public void cleanUp() {
    if (skipBaseCleanup) {
      LOG.warn("skipping cleanup");
      return;
    }
    java.util.List<org.apache.beam.it.common.ResourceManager> resources = new ArrayList<>();
    resources.add(spannerResourceManager);
    resources.add(gcsResourceManager);
    ResourceManagerUtils.cleanResources(
        resources.toArray(new org.apache.beam.it.common.ResourceManager[0]));

    if (orchestrator != null) {
      orchestrator.cleanup();
      orchestrator = null;
    }

    LOG.info(
        "CleanupCompleted for 1,024 Shards test (PostgreSQL). Test took {}",
        Duration.between(startTime, Instant.now()));
  }

  @Test
  public void postgreSQLToSpanner1024ShardsTest() throws Exception {
    int numPhysicalShards = numPhysicalInstances;
    int numLogicalShardsPerPhysical = numLogicalInstances;
    int tablesPerShard = 5;

    // Step 1: Generate Shard Map
    Map<String, List<String>> shardMap = new HashMap<>();
    String randomSuffix =
        org.apache.commons.lang3.RandomStringUtils.randomAlphanumeric(4).toLowerCase();
    String timestamp =
        java.time.format.DateTimeFormatter.ofPattern("yyyy_MM_dd_HH_mm_ss")
            .withZone(java.time.ZoneId.of("UTC"))
            .format(java.time.Instant.now());

    for (int i = 0; i < numPhysicalShards; i++) {
      String instanceName = String.format("nokill-1k-shard-postgresql-%02d", i);
      List<String> logicalDbs = new ArrayList<>();
      for (int j = 1; j <= numLogicalShardsPerPhysical; j++) {
        // Name pattern: d_<random>_<timestamp>_p<physicalIdx>_l<logicalIdx>
        logicalDbs.add(String.format("d_%s_%s_p%02d_l%02d", randomSuffix, timestamp, i, j));
      }
      shardMap.put(instanceName, logicalDbs);
    }

    // Step 2: Initialize physical and logical environment
    String sourceConfigPath = orchestrator.initialize(shardMap, "shards.json");

    // Step 3: Data Generation within logical shards
    populateSourceDatabases(tablesPerShard);

    // Step 4: Spanner Setup
    createSpannerTables(tablesPerShard);

    String sessionFilePath = createAndUploadSessionFile(tablesPerShard);

    Map<String, String> params = getCommonParameters();
    params.put("sourceConfigURL", sourceConfigPath);
    params.put("sessionFilePath", sessionFilePath);
    params.put("sourceDbDialect", SQLDialect.POSTGRESQL.name());
    params.put("jdbcDriverClassName", "org.postgresql.Driver");
    params.put("maxConnections", "16");
    params.put("numWorkers", "16");
    params.put("maxNumWorkers", "16");
    params.put("workerMachineType", "n2-standard-4");

    LaunchConfig.Builder options = LaunchConfig.builder(testName, SPEC_PATH).setParameters(params);
    PipelineLauncher.LaunchInfo jobInfo = launchJob(options);

    PipelineOperator.Result result =
        pipelineOperator.waitUntilDone(createConfig(jobInfo, Duration.ofMinutes(60L)));
    assertThatResult(result).isLaunchFinished();

    // Step 6: Verification
    verifyMigration(numPhysicalShards * numLogicalShardsPerPhysical, tablesPerShard);

    // Collect metrics
    collectAndExportMetrics(jobInfo);
  }

  private void populateSourceDatabases(int tablesPerShard) throws Exception {
    LOG.info("Populating logical shards with data (PostgreSQL)");
    ExecutorService executor = Executors.newFixedThreadPool(64);

    for (Map.Entry<String, CloudSqlResourceManager> entry : orchestrator.managers.entrySet()) {
      String physicalInstanceName = entry.getKey();
      final CloudSqlResourceManager manager = entry.getValue();

      // Find logical DBs for this physical instance
      List<String> dbNames = orchestrator.requestedShardMap.get(physicalInstanceName);

      for (String dbName : dbNames) {
        executor.submit(
            () -> {
              try (Connection dbConn = getJdbcConnectionForDb(manager, dbName)) {
                for (int k = 0; k < tablesPerShard; k++) {
                  String tableName = "table_" + k;
                  try (Statement stmt = dbConn.createStatement()) {
                    stmt.executeUpdate(
                        "CREATE TABLE IF NOT EXISTS "
                            + tableName
                            + " (id INT PRIMARY KEY, data VARCHAR(100))");
                    stmt.executeUpdate(
                        "INSERT INTO "
                            + tableName
                            + " VALUES (1, 'data_from_instance_"
                            + physicalInstanceName
                            + "_db_"
                            + dbName
                            + "') ON CONFLICT (id) DO UPDATE SET data = EXCLUDED.data");
                  }
                }
              } catch (SQLException e) {
                LOG.error("Failed to populate shard {}", dbName, e);
                throw new RuntimeException(e);
              }
            });
      }
    }

    executor.shutdown();
    if (!executor.awaitTermination(60, TimeUnit.MINUTES)) {
      throw new RuntimeException("Source DB population timed out");
    }
  }

  private void createSpannerTables(int tablesPerShard) {
    LOG.info("Creating {} Spanner tables", tablesPerShard);
    for (int i = 0; i < tablesPerShard; i++) {
      String ddl =
          String.format(
              "CREATE TABLE table_%d ("
                  + "  migration_shard_id STRING(50) NOT NULL,"
                  + "  id INT64 NOT NULL,"
                  + "  data STRING(100),"
                  + ") PRIMARY KEY (migration_shard_id, id)",
              i);
      spannerResourceManager.executeDdlStatements(ImmutableList.of(ddl));
    }
  }

  private void verifyMigration(int numShards, int tablesPerShard) {
    LOG.info("Verifying migration of {} shards", numShards);
    for (int i = 0; i < tablesPerShard; i++) {
      String tableName = "table_" + i;
      assertThat(spannerResourceManager.getRowCount(tableName)).isEqualTo((long) numShards);

      // Verify distinct shard IDs
      ImmutableList<Struct> rows =
          spannerResourceManager.runQuery(
              "SELECT COUNT(DISTINCT migration_shard_id) FROM " + tableName);
      assertThat(rows.size()).isEqualTo(1);
      assertThat(rows.get(0).getLong(0)).isEqualTo((long) numShards);
    }
  }

  private Connection getJdbcConnectionForDb(CloudSqlResourceManager manager, String dbName)
      throws SQLException {
    String uri =
        String.format("jdbc:postgresql://%s:%d/%s", manager.getHost(), manager.getPort(), dbName);
    return DriverManager.getConnection(uri, manager.getUsername(), manager.getPassword());
  }

  private String createAndUploadSessionFile(int tablesPerShard) throws IOException {
    JSONObject session = new JSONObject();
    JSONObject spSchema = new JSONObject();
    JSONObject srcSchema = new JSONObject();
    JSONObject toSpanner = new JSONObject();
    JSONObject toSource = new JSONObject();
    JSONObject spannerToId = new JSONObject();
    JSONObject srcToId = new JSONObject();

    for (int i = 0; i < tablesPerShard; i++) {
      String tableName = "table_" + i;
      String tableId = tableName;

      // Spanner Schema: migration_shard_id (c1), id (c2), data (c3)
      JSONObject spTable = new JSONObject();
      spTable.put("Name", tableName);
      spTable.put("ColIds", new JSONArray(List.of("c1", "c2", "c3")));

      JSONObject spColDefs = new JSONObject();
      spColDefs.put(
          "c1",
          new JSONObject()
              .put("Name", "migration_shard_id")
              .put("T", new JSONObject().put("Name", "STRING")));
      spColDefs.put(
          "c2", new JSONObject().put("Name", "id").put("T", new JSONObject().put("Name", "INT64")));
      spColDefs.put(
          "c3",
          new JSONObject().put("Name", "data").put("T", new JSONObject().put("Name", "STRING")));
      spTable.put("ColDefs", spColDefs);

      JSONArray pks = new JSONArray();
      pks.put(new JSONObject().put("ColId", "c1"));
      pks.put(new JSONObject().put("ColId", "c2"));
      spTable.put("PrimaryKeys", pks);
      spTable.put("ShardIdColumn", "c1");

      spSchema.put(tableId, spTable);

      // Source Schema: must use SAME IDs for mapped columns (c2, c3)
      JSONObject srcTable = new JSONObject();
      srcTable.put("Name", tableName);
      srcTable.put("ColIds", new JSONArray(List.of("c2", "c3")));
      JSONObject srcColDefs = new JSONObject();
      srcColDefs.put(
          "c2",
          new JSONObject().put("Name", "id").put("Type", new JSONObject().put("Name", "INTEGER")));
      srcColDefs.put(
          "c3",
          new JSONObject()
              .put("Name", "data")
              .put("Type", new JSONObject().put("Name", "VARCHAR")));
      srcTable.put("ColDefs", srcColDefs);
      srcTable.put("PrimaryKeys", new JSONArray(List.of(new JSONObject().put("ColId", "c2"))));

      srcSchema.put(tableId, srcTable);

      // ToSpanner: Map Source Column Name to Spanner Column ID
      toSpanner.put(
          tableName,
          new JSONObject()
              .put("Name", tableName)
              .put("Cols", new JSONObject().put("id", "c2").put("data", "c3")));

      // ToSource: Map Spanner Table Name to Source Table Name and Spanner Column ID to Source
      // Column Name
      toSource.put(
          tableName,
          new JSONObject()
              .put("Name", tableName)
              .put("Cols", new JSONObject().put("c2", "id").put("c3", "data")));

      // SpannerToID: Map Spanner Table Name to internal Table ID and Spanner Column Name to ID
      spannerToId.put(
          tableName,
          new JSONObject()
              .put("Name", tableId)
              .put(
                  "Cols",
                  new JSONObject()
                      .put("migration_shard_id", "c1")
                      .put("id", "c2")
                      .put("data", "c3")));

      // SrcToID: Map Source Table Name to internal Table ID and Source Column Name to ID
      srcToId.put(
          tableName,
          new JSONObject()
              .put("Name", tableId)
              .put("Cols", new JSONObject().put("id", "c2").put("data", "c3")));
    }

    session.put("SpSchema", spSchema);
    session.put("SrcSchema", srcSchema);
    session.put("ToSpanner", toSpanner);
    session.put("ToSource", toSource);
    session.put("SpannerToID", spannerToId);
    session.put("SrcToID", srcToId);
    session.put("SyntheticPKeys", new JSONObject());

    String content = session.toString();
    GcsArtifact artifact =
        (GcsArtifact) gcsResourceManager.createArtifact("session.json", content.getBytes());
    com.google.cloud.storage.BlobInfo blobInfo = artifact.getBlob().asBlobInfo();
    return String.format("gs://%s/%s", blobInfo.getBucket(), blobInfo.getName());
  }
}
