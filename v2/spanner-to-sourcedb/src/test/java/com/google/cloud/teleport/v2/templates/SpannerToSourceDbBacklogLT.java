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

import static org.apache.beam.it.common.TestProperties.getProperty;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Struct;
import com.google.cloud.teleport.metadata.TemplateLoadTest;
import com.google.cloud.teleport.v2.spanner.migrations.shard.Shard;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.TestProperties;
import org.apache.beam.it.gcp.cloudsql.CloudSqlResourceManager;
import org.apache.beam.it.gcp.cloudsql.CloudSqlShardOrchestrator;
import org.apache.beam.it.gcp.cloudsql.CloudSqlShardOrchestrator.DatabaseType;
import org.apache.beam.it.gcp.dataflow.ClassicTemplateClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Performance / Load test for {@link SpannerToSourceDb} template.
 *
 * <p>Objective: Validate if Reverse Replication pipeline can successfully process a massive backlog
 * of 1 Billion rows (1 Terabyte of data) generated and imported into Spanner.
 */
@Category(TemplateLoadTest.class)
@TemplateLoadTest(SpannerToSourceDb.class)
@RunWith(JUnit4.class)
public class SpannerToSourceDbBacklogLT extends SpannerToSourceDbLTBase {

  private static final Logger LOG = LoggerFactory.getLogger(SpannerToSourceDbBacklogLT.class);

  private static final String TEMPLATE_SPEC_PATH =
      com.google.common.base.MoreObjects.firstNonNull(
          TestProperties.specPath(), "gs://dataflow-templates/latest/flex/Spanner_to_SourceDb");

  private final String spannerDdlResource = "SpannerToSourceDbBacklogLT/spanner-schema.sql";
  private final String sessionFileResource = "SpannerToSourceDbBacklogLT/session.json";
  private final String table = "MigrationLoadTest";

  private CloudSqlShardOrchestrator orchestrator;
  private Integer originalSpannerNodeCount = null;
  private Integer originalSpannerMetadataNodeCount = null;

  @Before
  public void setup() throws IOException {
    LOG.info(
        "Initializing resource managers for High-Scale Backlog Replication E2E Load Test via Orchestrator...");

    // Setup Spanner database and metadata database, GCS artifact resource manager, and session
    // files
    setupResourceManagers(spannerDdlResource, sessionFileResource);

    // Initialize the Cloud SQL Shard Orchestrator for dynamic GCP-level provisioning over Private
    // IP
    orchestrator =
        new CloudSqlShardOrchestrator(
            DatabaseType.MYSQL,
            CloudSqlShardOrchestrator.MYSQL_8_0,
            project,
            region,
            gcsResourceManager);

    Map<String, List<String>> shardMap = new HashMap<>();
    shardMap.put("nokill-high-resources-backlog-shard1", List.of("shard0", "shard1"));
    shardMap.put("nokill-high-resources-backlog-shard2", List.of("shard2", "shard3"));

    // Initialize the physical instances (reusing existing ones) and logical schemas
    orchestrator.initialize(shardMap, "orchestrator_shards_bulk.json");

    // Create logical table schemas inside each database shard
    LOG.info("Creating logical schemas on MySQL shards...");
    CloudSqlResourceManager manager1 =
        (CloudSqlResourceManager) orchestrator.managers.get("nokill-high-resources-backlog-shard1");
    CloudSqlResourceManager manager2 =
        (CloudSqlResourceManager) orchestrator.managers.get("nokill-high-resources-backlog-shard2");
    createLogicalTableSchema(manager1, "shard0");
    createLogicalTableSchema(manager1, "shard1");
    createLogicalTableSchema(manager2, "shard2");
    createLogicalTableSchema(manager2, "shard3");

    // Upload sharding configuration in the flat format expected by SpannerToSourceDb
    LOG.info("Generating and uploading flat sharding configuration to GCS...");
    createAndUploadShardConfigToGcs();

    // Store original node counts for cleanup
    originalSpannerNodeCount = getSpannerNodeCount(spannerResourceManager.getInstanceId());
    originalSpannerMetadataNodeCount =
        getSpannerNodeCount(spannerMetadataResourceManager.getInstanceId());
  }

  @After
  public void tearDown() {
    LOG.info("Cleaning up resources...");

    // Reset Spanner instance to its original node count if it was modified
    if (originalSpannerNodeCount != null && spannerResourceManager != null) {
      try {
        updateSpannerNodeCount(spannerResourceManager.getInstanceId(), originalSpannerNodeCount);
      } catch (Exception e) {
        LOG.warn("Failed to reset Spanner node count during teardown: ", e);
      }
    }
    // Reset Spanner Metadata instance to its original node count if it was modified
    if (originalSpannerMetadataNodeCount != null && spannerMetadataResourceManager != null) {
      try {
        updateSpannerNodeCount(
            spannerMetadataResourceManager.getInstanceId(), originalSpannerMetadataNodeCount);
      } catch (Exception e) {
        LOG.warn("Failed to reset Spanner Metadata node count during teardown: ", e);
      }
    }

    cleanupResourceManagers();
    if (orchestrator != null) {
      orchestrator.cleanup();
    }
  }

  @Test
  public void reverseReplicationBacklogLoadTest()
      throws IOException, java.text.ParseException, InterruptedException {
    LOG.info("Running High-Scale Backlog Replication E2E Load Test (1 Billion rows)...");

    // -------------------------------------------------------------
    // PHASE 1: Connectivity & Setup Sanity
    // -------------------------------------------------------------
    LOG.info("PHASE 1: Verifying Spanner & CloudSQL setup connectivity...");
    assertNotNull("Spanner resource manager should be initialized", spannerResourceManager);

    String testId = "test-id-12345";
    String testPayload = "test-payload-sanity";
    String testShardId = "shard_0";

    // Write/Read Spanner Ping Row
    List<Mutation> mutations = new ArrayList<>();
    mutations.add(
        Mutation.newInsertOrUpdateBuilder("MigrationLoadTest")
            .set("Id")
            .to(testId)
            .set("Payload")
            .to(testPayload)
            .set("migration_shard_id")
            .to(testShardId)
            .build());
    spannerResourceManager.write(mutations);

    List<Struct> spannerResults =
        spannerResourceManager.runQuery(
            String.format(
                "SELECT Payload FROM MigrationLoadTest WHERE migration_shard_id = '%s' AND Id = '%s'",
                testShardId, testId));
    assertNotNull("Results from Spanner should not be null", spannerResults);
    assertEquals("Should return exactly 1 row", 1, spannerResults.size());
    assertEquals(
        "Payload matches what was written to Spanner",
        testPayload,
        spannerResults.get(0).getString("Payload"));
    spannerResourceManager.write(
        List.of(
            Mutation.delete(
                "MigrationLoadTest", com.google.cloud.spanner.Key.of(testShardId, testId))));

    // Verify GCS configs exist
    String sessionGcsPath = getGcsPath(SESSION_FILE_NAME, gcsResourceManager);
    assertTrue("Session file should exist on GCS", sessionGcsPath.startsWith("gs://"));
    String shardGcsPath = getGcsPath(SOURCE_SHARDS_FILE_NAME, gcsResourceManager);
    assertTrue("Shard file should exist on GCS", shardGcsPath.startsWith("gs://"));

    // Verify CloudSQL connectivity
    CloudSqlResourceManager manager1 =
        (CloudSqlResourceManager) orchestrator.managers.get("nokill-high-resources-backlog-shard1");
    CloudSqlResourceManager manager2 =
        (CloudSqlResourceManager) orchestrator.managers.get("nokill-high-resources-backlog-shard2");
    assertNotNull("Shard 1 resource manager should be initialized", manager1);
    assertNotNull("Shard 2 resource manager should be initialized", manager2);
    verifyMySqlLogicalShard(manager1, "shard0");
    verifyMySqlLogicalShard(manager1, "shard1");
    verifyMySqlLogicalShard(manager2, "shard2");
    verifyMySqlLogicalShard(manager2, "shard3");

    // -------------------------------------------------------------
    // PHASE 2: Spanner Scale-Up & Avro Import
    // -------------------------------------------------------------
    LOG.info("PHASE 2: Scaling Spanner & running Avro Import...");

    // Record UTC start timestamp before import begins (to serve as change stream start timestamp)
    String startTimestamp = java.time.Instant.now().toString();
    LOG.info("Recorded UTC start timestamp for change stream: {}", startTimestamp);

    int scaleNodes =
        Integer.parseInt(getProperty("spannerScaleNodes", "25", TestProperties.Type.PROPERTY));
    updateSpannerNodeCount(spannerResourceManager.getInstanceId(), scaleNodes);

    // Verify scale-up
    int currentNodeCount = getSpannerNodeCount(spannerResourceManager.getInstanceId());
    assertEquals(
        "Spanner instance node count mismatch after scale-up", scaleNodes, currentNodeCount);

    // Run Avro Import with complete dataset (1 billion rows)
    String avroInputDir =
        getProperty(
            "avroInputDir",
            "gs://nokill-spanner-to-sourcedb-load/data/avro/",
            TestProperties.Type.PROPERTY);
    long expectedSpannerCount =
        Long.parseLong(
            getProperty("expectedSpannerCount", "1000000000", TestProperties.Type.PROPERTY));
    int importTimeoutMinutes =
        Integer.parseInt(getProperty("importTimeoutMinutes", "120", TestProperties.Type.PROPERTY));

    // Ensure avroInputDir ends with a trailing slash for the classic import template
    if (!avroInputDir.endsWith("/")) {
      avroInputDir = avroInputDir + "/";
    }

    LOG.info("Avro Input Directory: {}", avroInputDir);
    LOG.info("Expected Spanner count: {}", expectedSpannerCount);

    PipelineLauncher.LaunchInfo importJobInfo = launchClassicImportJob(avroInputDir);
    assertThatPipeline(importJobInfo).isRunning();

    PipelineOperator.Result importResult =
        pipelineOperator.waitUntilDone(
            createConfig(importJobInfo, Duration.ofMinutes(importTimeoutMinutes)));
    assertThatResult(importResult).isLaunchFinished();

    long spannerCount = spannerResourceManager.getRowCount(table);
    assertEquals("Spanner database row count mismatch", expectedSpannerCount, spannerCount);
    LOG.info("Import Phase successful! Imported {} rows.", spannerCount);

    // -------------------------------------------------------------
    // PHASE 3: Downscale & Reverse Replication E2E Verification
    // -------------------------------------------------------------
    // Downscale main Spanner instance to 5 nodes and upscale metadata Spanner instance to 20 nodes
    LOG.info(
        "Downscaling main Spanner instance to 5 nodes and upscaling metadata instance to 20 nodes before starting replication...");
    updateSpannerNodeCount(spannerResourceManager.getInstanceId(), 5);
    updateSpannerNodeCount(spannerMetadataResourceManager.getInstanceId(), 20);

    int reverseTimeoutMinutes =
        Integer.parseInt(getProperty("reverseTimeoutMinutes", "600", TestProperties.Type.PROPERTY));
    int maxShardConnections =
        Integer.parseInt(getProperty("maxShardConnections", "2000", TestProperties.Type.PROPERTY));
    int numWorkers =
        Integer.parseInt(getProperty("numWorkers", "200", TestProperties.Type.PROPERTY));
    int maxWorkers =
        Integer.parseInt(getProperty("maxWorkers", "200", TestProperties.Type.PROPERTY));
    String machineType = getProperty("machineType", "n2-highmem-8", TestProperties.Type.PROPERTY);

    long expectedShardCount =
        Long.parseLong(
            getProperty("expectedShardCount", "250000000", TestProperties.Type.PROPERTY));
    long metricThreshold =
        Long.parseLong(getProperty("metricThreshold", "999999999", TestProperties.Type.PROPERTY));

    PipelineLauncher.LaunchInfo reverseJobInfo =
        launchReverseReplicationJob(
            startTimestamp, numWorkers, maxWorkers, machineType, maxShardConnections);
    assertThatPipeline(reverseJobInfo).isRunning();

    // Poll success_record_count metric until it reaches the threshold
    long polledCount = 0;
    long startTimeMillis = System.currentTimeMillis();
    int numShards = 4;

    while (polledCount < metricThreshold) {
      if (System.currentTimeMillis() - startTimeMillis > reverseTimeoutMinutes * 60 * 1000) {
        throw new RuntimeException(
            "Reverse replication load check timed out after "
                + reverseTimeoutMinutes
                + " minutes.");
      }
      Thread.sleep(300000); // Poll every 5 minutes

      Double metricVal =
          pipelineLauncher.getMetric(
              project, region, reverseJobInfo.jobId(), "success_record_count");
      polledCount = metricVal != null ? metricVal.longValue() : 0;

      Double severeErrors =
          pipelineLauncher.getMetric(project, region, reverseJobInfo.jobId(), "severe_error_count");
      double severeErrorVal = severeErrors != null ? severeErrors : 0.0;

      Double skippedRecords =
          pipelineLauncher.getMetric(
              project, region, reverseJobInfo.jobId(), "skipped_record_count");
      double skippedRecordVal = skippedRecords != null ? skippedRecords : 0.0;

      LOG.info("--- PIPELINE PROGRESS UPDATE ---");
      LOG.info(
          "Time Elapsed: {} minutes / {} minutes",
          (System.currentTimeMillis() - startTimeMillis) / 60000,
          reverseTimeoutMinutes);
      LOG.info(
          "Polled success_record_count: {}. Target threshold: {}", polledCount, metricThreshold);
      LOG.info("Severe errors so far: {}", severeErrorVal);
      LOG.info("Skipped records so far: {}", skippedRecordVal);

      for (int i = 1; i <= numShards; ++i) {
        Double replicationLag =
            pipelineLauncher.getMetric(
                project,
                region,
                reverseJobInfo.jobId(),
                "replication_lag_in_seconds_Shard" + i + "_MEAN");
        LOG.info(
            "Replication Lag Mean Shard{}: {} seconds",
            i,
            replicationLag != null ? replicationLag : 0.0);
      }
      LOG.info("---------------------------------");
    }

    // Verify database parity on MySQL shards
    LOG.info(
        "Replication threshold reached. Verifying logical databases row counts on CloudSQL...");
    long count0 = getLogicalDatabaseRowCount(manager1, "shard0");
    long count1 = getLogicalDatabaseRowCount(manager1, "shard1");
    long count2 = getLogicalDatabaseRowCount(manager2, "shard2");
    long count3 = getLogicalDatabaseRowCount(manager2, "shard3");

    LOG.info(
        "Logical databases replicated row counts: shard0={}, shard1={}, shard2={}, shard3={}",
        count0,
        count1,
        count2,
        count3);
    assertEquals("shard0 row count mismatch", expectedShardCount, count0);
    assertEquals("shard1 row count mismatch", expectedShardCount, count1);
    assertEquals("shard2 row count mismatch", expectedShardCount, count2);
    assertEquals("shard3 row count mismatch", expectedShardCount, count3);

    LOG.info("All sharded replication backlog counts successfully verified. Cancelling job...");
    PipelineOperator.Result cancelResult =
        pipelineOperator.cancelJobAndFinish(createConfig(reverseJobInfo, Duration.ofMinutes(20)));
    assertThatResult(cancelResult).isLaunchFinished();

    exportMetrics(reverseJobInfo, numShards);
    LOG.info("High-Scale Backlog Replication E2E Load Test passed successfully!");
  }

  private void verifyMySqlLogicalShard(CloudSqlResourceManager manager, String dbName) {
    LOG.info("Verifying logical database: {}...", dbName);

    String testId = "test-id-" + dbName;
    String testPayload = "payload-" + dbName;

    // Insert test row
    String insertSql =
        String.format(
            "INSERT INTO %s.MigrationLoadTest (Id, Payload) VALUES ('%s', '%s')",
            dbName, testId, testPayload);
    manager.runSQLUpdate(insertSql);

    // Query test row back
    String selectSql =
        String.format("SELECT Payload FROM %s.MigrationLoadTest WHERE Id = '%s'", dbName, testId);
    List<Map<String, Object>> result = manager.runSQLQuery(selectSql);

    assertNotNull("Result from MySQL logical shard " + dbName + " should not be null", result);
    assertEquals("Should return exactly 1 row", 1, result.size());
    assertEquals(
        "Payload matches what was written to " + dbName, testPayload, result.get(0).get("Payload"));

    // Cleanup test row
    String deleteSql =
        String.format("DELETE FROM %s.MigrationLoadTest WHERE Id = '%s'", dbName, testId);
    manager.runSQLUpdate(deleteSql);
    LOG.info("Logical database {} verified successfully.", dbName);
  }

  private void createLogicalTableSchema(CloudSqlResourceManager manager, String dbName) {
    manager.runSQLUpdate(
        "CREATE TABLE IF NOT EXISTS "
            + dbName
            + ".MigrationLoadTest ("
            + "Id VARCHAR(36) NOT NULL,"
            + "Payload LONGTEXT NOT NULL,"
            + "PRIMARY KEY (Id)"
            + ") ENGINE=InnoDB");
  }

  private void createAndUploadShardConfigToGcs() throws IOException {
    CloudSqlResourceManager manager1 =
        (CloudSqlResourceManager) orchestrator.managers.get("nokill-high-resources-backlog-shard1");
    CloudSqlResourceManager manager2 =
        (CloudSqlResourceManager) orchestrator.managers.get("nokill-high-resources-backlog-shard2");

    JsonArray ja = new JsonArray();
    ja.add(createShardConfig("shard_0", "shard0", manager1));
    ja.add(createShardConfig("shard_1", "shard1", manager1));
    ja.add(createShardConfig("shard_2", "shard2", manager2));
    ja.add(createShardConfig("shard_3", "shard3", manager2));

    String shardFileContents = ja.toString();
    LOG.info("Shard file contents: {}", shardFileContents);
    gcsResourceManager.createArtifact(SOURCE_SHARDS_FILE_NAME, shardFileContents);
  }

  private JsonObject createShardConfig(
      String logicalShardId, String dbName, CloudSqlResourceManager manager) {
    Shard shard = new Shard();
    shard.setLogicalShardId(logicalShardId);
    shard.setUser(manager.getUsername());
    shard.setHost(manager.getHost());
    shard.setPassword(manager.getPassword());
    shard.setPort(String.valueOf(manager.getPort()));
    shard.setDbName(dbName);
    JsonObject jsObj = (JsonObject) new Gson().toJsonTree(shard).getAsJsonObject();
    jsObj.remove("secretManagerUri");
    return jsObj;
  }

  private PipelineLauncher.LaunchInfo launchClassicImportJob(String inputDir) throws IOException {
    ClassicTemplateClient classicClient = ClassicTemplateClient.builder(CREDENTIALS).build();

    Map<String, String> params = new HashMap<>();
    params.put("instanceId", spannerResourceManager.getInstanceId());
    params.put("databaseId", spannerResourceManager.getDatabaseId());
    params.put("inputDir", inputDir);

    PipelineLauncher.LaunchConfig options =
        PipelineLauncher.LaunchConfig.builder(
                "spanner-avro-import-backlog",
                "gs://dataflow-templates/latest/GCS_Avro_to_Cloud_Spanner")
            .setParameters(params)
            .addEnvironment("numWorkers", 80)
            .addEnvironment("maxWorkers", 500)
            .addEnvironment("machineType", "n2-highmem-8")
            .build();

    return classicClient.launch(project, region, options);
  }

  private PipelineLauncher.LaunchInfo launchReverseReplicationJob(
      String startTimestamp,
      int numWorkers,
      int maxWorkers,
      String machineType,
      int maxShardConnections)
      throws IOException {

    Map<String, String> params = new HashMap<>();
    params.put("changeStreamName", "MigrationStream");
    params.put("instanceId", spannerResourceManager.getInstanceId());
    params.put("databaseId", spannerResourceManager.getDatabaseId());
    params.put("spannerProjectId", project);
    params.put("metadataInstance", spannerMetadataResourceManager.getInstanceId());
    params.put("metadataDatabase", spannerMetadataResourceManager.getDatabaseId());
    params.put("sourceShardsFilePath", getGcsPath(SOURCE_SHARDS_FILE_NAME, gcsResourceManager));
    params.put("deadLetterQueueDirectory", getGcsPath("dlq", gcsResourceManager));
    params.put("startTimestamp", startTimestamp);
    params.put("maxShardConnections", String.valueOf(maxShardConnections));
    params.put("sessionFilePath", getGcsPath(SESSION_FILE_NAME, gcsResourceManager));
    params.put("workerMachineType", machineType);

    PipelineLauncher.LaunchConfig.Builder options =
        PipelineLauncher.LaunchConfig.builder(getClass().getSimpleName(), TEMPLATE_SPEC_PATH);
    options
        .addEnvironment("maxWorkers", maxWorkers)
        .addEnvironment("numWorkers", numWorkers)
        .addEnvironment("machineType", machineType)
        .addEnvironment(
            "additionalExperiments", java.util.Collections.singletonList("use_runner_v2"));

    options.setParameters(params);
    return pipelineLauncher.launch(project, region, options.build());
  }

  private long getLogicalDatabaseRowCount(CloudSqlResourceManager manager, String dbName) {
    String query =
        "SELECT /*+ SET_VAR(innodb_parallel_read_threads=94) */ COUNT(*) FROM "
            + dbName
            + ".MigrationLoadTest";
    List<Map<String, Object>> result = manager.runSQLQuery(query);
    if (result != null && !result.isEmpty()) {
      Map<String, Object> row = result.get(0);
      for (Object val : row.values()) {
        if (val instanceof Number) {
          return ((Number) val).longValue();
        }
      }
    }
    return 0;
  }

  public void updateSpannerNodeCount(String instanceId, int nodeCount) {
    com.google.cloud.spanner.SpannerOptions options =
        com.google.cloud.spanner.SpannerOptions.newBuilder().setProjectId(project).build();
    try (com.google.cloud.spanner.Spanner spanner = options.getService()) {
      com.google.cloud.spanner.InstanceAdminClient instanceAdminClient =
          spanner.getInstanceAdminClient();

      int fromCount = -1;
      if (spannerResourceManager != null
          && instanceId.equals(spannerResourceManager.getInstanceId())) {
        fromCount = originalSpannerNodeCount != null ? originalSpannerNodeCount : -1;
      } else if (spannerMetadataResourceManager != null
          && instanceId.equals(spannerMetadataResourceManager.getInstanceId())) {
        fromCount =
            originalSpannerMetadataNodeCount != null ? originalSpannerMetadataNodeCount : -1;
      }

      LOG.info(
          "Updating Spanner instance {} node count from {} to {}...",
          instanceId,
          fromCount,
          nodeCount);
      com.google.cloud.spanner.InstanceInfo instanceInfo =
          com.google.cloud.spanner.InstanceInfo.newBuilder(
                  com.google.cloud.spanner.InstanceId.of(project, instanceId))
              .setNodeCount(nodeCount)
              .build();
      instanceAdminClient
          .updateInstance(
              instanceInfo, com.google.cloud.spanner.InstanceInfo.InstanceField.NODE_COUNT)
          .get();
      LOG.info("Successfully updated Spanner instance {} node count to {}.", instanceId, nodeCount);
    } catch (Exception e) {
      LOG.error("Failed to update Spanner instance node count.", e);
      throw new RuntimeException("Failed to update Spanner node count", e);
    }
  }

  public int getSpannerNodeCount(String instanceId) {
    com.google.cloud.spanner.SpannerOptions options =
        com.google.cloud.spanner.SpannerOptions.newBuilder().setProjectId(project).build();
    try (com.google.cloud.spanner.Spanner spanner = options.getService()) {
      com.google.cloud.spanner.InstanceAdminClient instanceAdminClient =
          spanner.getInstanceAdminClient();
      com.google.cloud.spanner.Instance instance = instanceAdminClient.getInstance(instanceId);
      return instance.getNodeCount();
    } catch (Exception e) {
      LOG.error("Failed to retrieve Spanner instance node count.", e);
      throw new RuntimeException("Failed to get Spanner node count", e);
    }
  }
}
