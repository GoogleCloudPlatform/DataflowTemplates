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
import static org.junit.Assert.assertTrue;

import com.google.cloud.spanner.Instance;
import com.google.cloud.spanner.InstanceAdminClient;
import com.google.cloud.spanner.InstanceId;
import com.google.cloud.spanner.InstanceInfo;
import com.google.cloud.spanner.InstanceInfo.InstanceField;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.teleport.metadata.TemplateLoadTest;
import com.google.cloud.teleport.v2.spanner.migrations.shard.Shard;
import com.google.cloud.teleport.v2.spanner.migrations.source.config.JdbcShardConfig;
import com.google.common.base.MoreObjects;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import java.io.IOException;
import java.text.ParseException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.TestProperties;
import org.apache.beam.it.conditions.ConditionCheck;
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
public class SpannerToSourceDbLargeBacklogLT extends SpannerToSourceDbLTBase {

  private static final Logger LOG = LoggerFactory.getLogger(SpannerToSourceDbLargeBacklogLT.class);

  private static final String TEMPLATE_SPEC_PATH =
      MoreObjects.firstNonNull(
          TestProperties.specPath(), "gs://dataflow-templates/latest/flex/Spanner_to_SourceDb");

  private static final String SPANNER_DDL_RESOURCE =
      "SpannerToSourceDbLargeBacklogLT/spanner-schema.sql";
  private static final String SESSION_FILE_RESOURCE =
      "SpannerToSourceDbLargeBacklogLT/session.json";
  private static final String TABLE = "MigrationLoadTest";

  private static final String DEFAULT_PHYSICAL_SHARD_1 = "nokill-high-resources-backlog-shard1";
  private static final String DEFAULT_PHYSICAL_SHARD_2 = "nokill-high-resources-backlog-shard2";
  private static final String DEFAULT_SPANNER_SCALE_NODES = "25";
  private static final String DEFAULT_AVRO_INPUT_DIR =
      "gs://nokill-spanner-to-sourcedb-load/data/avro/";
  private static final String DEFAULT_EXPECTED_SPANNER_COUNT = "1000000000";
  private static final String DEFAULT_IMPORT_TIMEOUT_MINUTES = "120";
  private static final String DEFAULT_SPANNER_DOWNSCALE_NODES = "5";
  private static final String DEFAULT_METADATA_SCALE_NODES = "20";
  private static final String DEFAULT_REVERSE_TIMEOUT_MINUTES = "600";
  private static final String DEFAULT_MAX_SHARD_CONNECTIONS = "2000";
  private static final String DEFAULT_NUM_WORKERS = "200";
  private static final String DEFAULT_MAX_WORKERS = "200";
  private static final String DEFAULT_MACHINE_TYPE = "n2-highmem-8";
  private static final String DEFAULT_EXPECTED_SHARD_COUNT = "250000000";
  private static final String DEFAULT_METRIC_THRESHOLD = "1000000000";
  private static final String DEFAULT_VERIFICATION_TIMEOUT_MINUTES = "30";

  private CloudSqlShardOrchestrator orchestrator;
  private CloudSqlResourceManager manager1;
  private CloudSqlResourceManager manager2;
  private Integer originalSpannerNodeCount = null;
  private Integer originalSpannerMetadataNodeCount = null;

  @Before
  public void setup() throws IOException {
    setupResourceManagers(SPANNER_DDL_RESOURCE, SESSION_FILE_RESOURCE);

    orchestrator =
        new CloudSqlShardOrchestrator(
            DatabaseType.MYSQL,
            CloudSqlShardOrchestrator.MYSQL_8_0,
            project,
            region,
            gcsResourceManager);

    // The CloudSQL setup consists of 2 physical shards with 2 logical shards each
    String physicalShard1 =
        getProperty("physicalShard1", DEFAULT_PHYSICAL_SHARD_1, TestProperties.Type.PROPERTY);
    String physicalShard2 =
        getProperty("physicalShard2", DEFAULT_PHYSICAL_SHARD_2, TestProperties.Type.PROPERTY);

    Map<String, List<String>> shardMap = new HashMap<>();
    shardMap.put(physicalShard1, List.of("shard0", "shard1"));
    shardMap.put(physicalShard2, List.of("shard2", "shard3"));

    // Initialize the physical instances (reusing existing ones) and logical schemas
    orchestrator.initialize(shardMap, "orchestrator_shards_bulk.json");
    manager1 = (CloudSqlResourceManager) orchestrator.managers.get(physicalShard1);
    manager2 = (CloudSqlResourceManager) orchestrator.managers.get(physicalShard2);

    LOG.info("Creating logical schemas on MySQL shards...");
    createLogicalTableSchema(manager1, "shard0");
    createLogicalTableSchema(manager1, "shard1");
    createLogicalTableSchema(manager2, "shard2");
    createLogicalTableSchema(manager2, "shard3");

    LOG.info("Generating and uploading sharding configuration to GCS...");
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
      throws IOException, ParseException, InterruptedException {

    // -------------------------------------------------------------
    // PHASE 1: Import 1 billion rows to Spanner
    // -------------------------------------------------------------
    LOG.info("PHASE 1: Import 1 billion rows to Spanner");

    // Record UTC start timestamp before import begins (to serve as start timestamp to reverse
    // replication job)
    String startTimestamp = java.time.Instant.now().toString();
    LOG.info("Recorded UTC start timestamp for Reverse Replication Job: {}", startTimestamp);

    // Node count taken from manual test results available in go/reverse-backlog-manual-tests
    int scaleNodes =
        Integer.parseInt(
            getProperty(
                "spannerScaleNodes", DEFAULT_SPANNER_SCALE_NODES, TestProperties.Type.PROPERTY));
    updateSpannerNodeCount(spannerResourceManager.getInstanceId(), scaleNodes);

    // Verify scale-up - it is critical that the Spanner instance is scaled up, otherwise the
    // pipeline might run out of resources or face bottleneck issues.
    int currentNodeCount = getSpannerNodeCount(spannerResourceManager.getInstanceId());
    assertEquals(
        "Spanner instance node count mismatch after scale-up", scaleNodes, currentNodeCount);

    // Run Avro Import with complete dataset (1 billion rows)
    String avroInputDir =
        getProperty("avroInputDir", DEFAULT_AVRO_INPUT_DIR, TestProperties.Type.PROPERTY);
    long expectedSpannerCount =
        Long.parseLong(
            getProperty(
                "expectedSpannerCount",
                DEFAULT_EXPECTED_SPANNER_COUNT,
                TestProperties.Type.PROPERTY));
    int importTimeoutMinutes =
        Integer.parseInt(
            getProperty(
                "importTimeoutMinutes",
                DEFAULT_IMPORT_TIMEOUT_MINUTES,
                TestProperties.Type.PROPERTY));

    // Ensure avroInputDir ends with a trailing slash for the classic import template
    if (!avroInputDir.endsWith("/")) {
      avroInputDir = avroInputDir + "/";
    }

    LOG.info("Avro Input Directory: {}", avroInputDir);
    LOG.info("Expected Spanner count: {}", expectedSpannerCount);

    PipelineLauncher.LaunchInfo importJobInfo = launchImportJob(avroInputDir);
    assertThatPipeline(importJobInfo).isRunning();

    PipelineOperator.Result importResult =
        pipelineOperator.waitUntilDone(
            createConfig(importJobInfo, Duration.ofMinutes(importTimeoutMinutes)));
    assertThatResult(importResult).isLaunchFinished();

    long spannerCount = spannerResourceManager.getRowCount(TABLE);
    assertEquals("Spanner database row count mismatch", expectedSpannerCount, spannerCount);
    LOG.info("Import Phase successful! Imported {} rows.", spannerCount);

    // -------------------------------------------------------------
    // PHASE 2: Reverse Replication E2E Verification
    // -------------------------------------------------------------
    // Downscale main Spanner instance to 5 nodes and upscale metadata Spanner instance to 20 nodes
    // (go/reverse-backlog-manual-tests)
    int spannerDownscaleNodes =
        Integer.parseInt(
            getProperty(
                "spannerDownscaleNodes",
                DEFAULT_SPANNER_DOWNSCALE_NODES,
                TestProperties.Type.PROPERTY));
    int metadataScaleNodes =
        Integer.parseInt(
            getProperty(
                "metadataScaleNodes", DEFAULT_METADATA_SCALE_NODES, TestProperties.Type.PROPERTY));

    LOG.info(
        "Downscaling main Spanner instance to {} nodes and upscaling metadata instance to {} nodes before starting replication...",
        spannerDownscaleNodes,
        metadataScaleNodes);
    updateSpannerNodeCount(spannerResourceManager.getInstanceId(), spannerDownscaleNodes);
    updateSpannerNodeCount(spannerMetadataResourceManager.getInstanceId(), metadataScaleNodes);

    int reverseTimeoutMinutes =
        Integer.parseInt(
            getProperty(
                "reverseTimeoutMinutes",
                DEFAULT_REVERSE_TIMEOUT_MINUTES,
                TestProperties.Type.PROPERTY));
    int maxShardConnections =
        Integer.parseInt(
            getProperty(
                "maxShardConnections",
                DEFAULT_MAX_SHARD_CONNECTIONS,
                TestProperties.Type.PROPERTY));
    int numWorkers =
        Integer.parseInt(
            getProperty("numWorkers", DEFAULT_NUM_WORKERS, TestProperties.Type.PROPERTY));
    int maxWorkers =
        Integer.parseInt(
            getProperty("maxWorkers", DEFAULT_MAX_WORKERS, TestProperties.Type.PROPERTY));
    String machineType =
        getProperty("machineType", DEFAULT_MACHINE_TYPE, TestProperties.Type.PROPERTY);

    PipelineLauncher.LaunchInfo reverseJobInfo =
        launchReverseReplicationJob(
            startTimestamp, numWorkers, maxWorkers, machineType, maxShardConnections);
    assertThatPipeline(reverseJobInfo).isRunning();

    // This is a long running test (7-8 hours) and we don't expect the SQL count queries to pass
    // the assertion for the first few hours (when the replication backlog is being processed).
    // Asserting on direct database counts during early stages would be highly inefficient and put
    // unnecessary resource contention on the database shards. Thus, we poll the
    // "success_record_count" metric and only start asserting on SQL counts once the metric
    // threshold is met.
    // Note that the above metric can NOT act as a reliable source of truth for the success of a
    // replication pipeline. It is ONLY used as an indicator to start the SQL count verification.

    long expectedShardCount =
        Long.parseLong(
            getProperty(
                "expectedShardCount", DEFAULT_EXPECTED_SHARD_COUNT, TestProperties.Type.PROPERTY));
    long metricThreshold =
        Long.parseLong(
            getProperty("metricThreshold", DEFAULT_METRIC_THRESHOLD, TestProperties.Type.PROPERTY));

    long startTimeMillis = System.currentTimeMillis();
    int numShards = 4;

    ConditionCheck successRecordsCheck =
        new ConditionCheck() {
          @Override
          protected String getDescription() {
            return String.format(
                "Check if Dataflow metric success_record_count reaches %d", metricThreshold);
          }

          @Override
          protected CheckResult check() {
            try {
              Double successRecordsCount =
                  pipelineLauncher.getMetric(
                      project, region, reverseJobInfo.jobId(), "success_record_count");
              long polledCount = successRecordsCount != null ? successRecordsCount.longValue() : 0;

              LOG.info("--- PIPELINE PROGRESS UPDATE ---");
              LOG.info(
                  "Time Elapsed: {} minutes / {} minutes",
                  (System.currentTimeMillis() - startTimeMillis) / 60000,
                  reverseTimeoutMinutes);
              LOG.info(
                  "Polled success_record_count: {} / Target: {}", polledCount, metricThreshold);
              LOG.info("---------------------------------");

              if (polledCount >= metricThreshold) {
                return new CheckResult(true, String.format("Threshold reached: %d", polledCount));
              }
              return new CheckResult(
                  false, String.format("Current progress: %d rows", polledCount));
            } catch (Exception e) {
              return new CheckResult(false, "Failed to retrieve job metrics: " + e.getMessage());
            }
          }
        };

    PipelineOperator.Result result =
        pipelineOperator.waitForCondition(
            createConfig(
                reverseJobInfo,
                Duration.ofMinutes(reverseTimeoutMinutes), // total timeout
                Duration.ofMinutes(
                    15)), // Poll every 15 minutes. Since the test runs for 7-8 hours, 15-minute
            // intervals
            // print exactly 4 logs per hour, preventing clutter and API call costs.
            successRecordsCheck);

    assertThatResult(result).meetsConditions();

    // Verify database parity on MySQL shards with a retry loop to handle minor replication
    // synchronization lag
    LOG.info(
        "Replication threshold reached. Verifying logical databases row counts on CloudSQL with a catch-up retry loop...");
    long verificationStartTime = System.currentTimeMillis();
    int verificationTimeoutMinutes =
        Integer.parseInt(
            getProperty(
                "verificationTimeoutMinutes",
                DEFAULT_VERIFICATION_TIMEOUT_MINUTES,
                TestProperties.Type.PROPERTY));
    long verificationTimeoutMs =
        verificationTimeoutMinutes
            * 60
            * 1000; // Configurable minutes timeout for final parity catch-up
    boolean parityAchieved = false;
    long count0 = 0, count1 = 0, count2 = 0, count3 = 0;

    // We execute the logical database row counts using a parallel thread pool instead of running
    // them sequentially. At 1-billion row scale, executing sequential SELECT COUNT(*) on 4 massive
    // MySQL database shards sequentially takes around 12 minutes per iteration, causing the
    // catch-up verification timeout to exhaust after only two/three iterations. Running in parallel
    // reduces
    // the query round-trip time to the slowest single shard query (~3 minutes), ensuring the loop
    // has
    // ample opportunities to poll and catch up.
    ExecutorService executor = Executors.newFixedThreadPool(4);
    try {
      while (System.currentTimeMillis() - verificationStartTime < verificationTimeoutMs) {
        CompletableFuture<Long> f0 =
            CompletableFuture.supplyAsync(
                () -> getLogicalDatabaseRowCount(manager1, "shard0"), executor);
        CompletableFuture<Long> f1 =
            CompletableFuture.supplyAsync(
                () -> getLogicalDatabaseRowCount(manager1, "shard1"), executor);
        CompletableFuture<Long> f2 =
            CompletableFuture.supplyAsync(
                () -> getLogicalDatabaseRowCount(manager2, "shard2"), executor);
        CompletableFuture<Long> f3 =
            CompletableFuture.supplyAsync(
                () -> getLogicalDatabaseRowCount(manager2, "shard3"), executor);

        // Block and wait for all parallel database count queries to resolve and return their values
        // - neccessary for the subsequent if block to compute correctly
        count0 = f0.join();
        count1 = f1.join();
        count2 = f2.join();
        count3 = f3.join();

        LOG.info(
            "Polled replicated row counts: shard0={}, shard1={}, shard2={}, shard3={} (Target: {})",
            count0,
            count1,
            count2,
            count3,
            expectedShardCount);

        if (count0 == expectedShardCount
            && count1 == expectedShardCount
            && count2 == expectedShardCount
            && count3 == expectedShardCount) {
          parityAchieved = true;
          break;
        }

        LOG.info(
            "Database counts have not reached exact target parity yet. Retrying in 1 minute...");
        Thread.sleep(60000); // Polling retry interval of 1 minute
      }
    } finally {
      executor.shutdown();
    }

    assertTrue(
        String.format(
            "Logical database row count mismatch after replication verification timeout. Replicated: shard0=%d, shard1=%d, shard2=%d, shard3=%d (Expected: %d each)",
            count0, count1, count2, count3, expectedShardCount),
        parityAchieved);

    LOG.info("All sharded replication backlog counts successfully verified. Cancelling job...");
    PipelineOperator.Result cancelResult =
        pipelineOperator.cancelJobAndFinish(createConfig(reverseJobInfo, Duration.ofMinutes(20)));
    assertThatResult(cancelResult).isLaunchFinished();
    exportMetrics(reverseJobInfo, numShards);
  }

  private void createLogicalTableSchema(CloudSqlResourceManager manager, String dbName) {
    manager.runSQLUpdate(
        "CREATE TABLE IF NOT EXISTS "
            + dbName
            + "."
            + TABLE
            + " ("
            + "Id VARCHAR(36) NOT NULL,"
            + "Payload LONGTEXT NOT NULL,"
            + "PRIMARY KEY (Id)"
            + ") ENGINE=InnoDB");
  }

  private void createAndUploadShardConfigToGcs() throws IOException {
    List<Shard> shards = new ArrayList<>();
    shards.add(createShardConfig("shard_0", "shard0", manager1));
    shards.add(createShardConfig("shard_1", "shard1", manager1));
    shards.add(createShardConfig("shard_2", "shard2", manager2));
    shards.add(createShardConfig("shard_3", "shard3", manager2));

    JdbcShardConfig jdbcShardConfig = new JdbcShardConfig();
    jdbcShardConfig.setShardConfigs(shards);
    JsonObject jsObj = new Gson().toJsonTree(jdbcShardConfig).getAsJsonObject();
    String shardFileContents = jsObj.toString();
    LOG.info("Shard file contents: {}", shardFileContents);
    gcsResourceManager.createArtifact(SOURCE_SHARDS_FILE_NAME, shardFileContents);
  }

  private Shard createShardConfig(
      String logicalShardId, String dbName, CloudSqlResourceManager manager) {
    Shard shard = new Shard();
    shard.setLogicalShardId(logicalShardId);
    shard.setUser(manager.getUsername());
    shard.setHost(manager.getHost());
    shard.setPassword(manager.getPassword());
    shard.setPort(String.valueOf(manager.getPort()));
    shard.setDbName(dbName);
    return shard;
  }

  private PipelineLauncher.LaunchInfo launchImportJob(String inputDir) throws IOException {
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
            .addEnvironment("maxWorkers", 120)
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
    // We use the InnoDB Optimizer hint 'SET_VAR(innodb_parallel_read_threads=94)' to explicitly
    // instruct
    // MySQL to leverage parallel read threads for the COUNT(*) operation. On massive shards
    // containing
    // 250 million rows, this utilizes multiple CPU cores of the high-resource Cloud SQL instance,
    // accelerating the full-table scan from over 10 minutes down to 2 minutes.
    String query =
        "SELECT /*+ SET_VAR(innodb_parallel_read_threads=94) */ COUNT(*) FROM "
            + dbName
            + "."
            + TABLE;
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
    SpannerOptions options = SpannerOptions.newBuilder().setProjectId(project).build();
    try (Spanner spanner = options.getService()) {
      InstanceAdminClient instanceAdminClient = spanner.getInstanceAdminClient();

      InstanceInfo instanceInfo =
          InstanceInfo.newBuilder(InstanceId.of(project, instanceId))
              .setNodeCount(nodeCount)
              .build();

      int maxRetries = 3;
      long backoffMs = 10000; // 10 seconds initial backoff
      for (int attempt = 1; attempt <= maxRetries; attempt++) {
        try {
          LOG.info(
              "Updating Spanner instance {} node count to {}... (Attempt {}/{})",
              instanceId,
              nodeCount,
              attempt,
              maxRetries);
          instanceAdminClient.updateInstance(instanceInfo, InstanceField.NODE_COUNT).get();
          LOG.info(
              "Successfully updated Spanner instance {} node count to {}.", instanceId, nodeCount);
          return;
        } catch (Exception e) {
          if (attempt == maxRetries) {
            throw e;
          }
          LOG.warn(
              "Failed to update Spanner instance node count on attempt {}. Retrying in {} ms...",
              attempt,
              backoffMs,
              e);
          Thread.sleep(backoffMs);
          backoffMs *= 2; // Exponential backoff
        }
      }
    } catch (Exception e) {
      LOG.error("Failed to update Spanner instance node count after retries.", e);
      throw new RuntimeException("Failed to update Spanner node count", e);
    }
  }

  public int getSpannerNodeCount(String instanceId) {
    SpannerOptions options = SpannerOptions.newBuilder().setProjectId(project).build();
    try (Spanner spanner = options.getService()) {
      InstanceAdminClient instanceAdminClient = spanner.getInstanceAdminClient();
      Instance instance = instanceAdminClient.getInstance(instanceId);
      return instance.getNodeCount();
    } catch (Exception e) {
      LOG.error("Failed to retrieve Spanner instance node count.", e);
      throw new RuntimeException("Failed to get Spanner node count", e);
    }
  }
}
