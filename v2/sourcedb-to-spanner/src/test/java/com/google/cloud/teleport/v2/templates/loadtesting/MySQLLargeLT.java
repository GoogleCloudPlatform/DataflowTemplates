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

import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;
import static org.junit.Assume.assumeTrue;

import com.google.cloud.spanner.InstanceId;
import com.google.cloud.spanner.InstanceInfo;
import com.google.cloud.spanner.InstanceInfo.InstanceField;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.cloud.teleport.metadata.TemplateLoadTest;
import com.google.cloud.teleport.v2.reader.io.jdbc.iowrapper.config.SQLDialect;
import com.google.cloud.teleport.v2.templates.SourceDbToSpanner;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineLauncher.LaunchConfig;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.LoadTestBase;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manually-invoked benchmark for {@link SourceDbToSpanner} against the permanent, pre-populated
 * MySQL benchmark fleet.
 *
 * <p>Pick a scenario and run it:
 *
 * <pre>{@code
 * mvn test -pl v2/sourcedb-to-spanner -am \
 *   -Dtest=MySQLLargeLT#allTablesOneShard -Dsurefire.failIfNoSpecifiedTests=false
 * }</pre>
 *
 * <p>Everything else is fixed in {@link Scenario} and the constants below, so a run needs no
 * arguments. The scenarios differ only in how much data they move:
 *
 * <table>
 *   <caption>Benchmark scenarios</caption>
 *   <tr><th>Test method<th>Tables<th>Shards<th>Approx. size
 *   <tr><td>{@code singleTableOneShard}<td>1<td>1<td>64 GB
 *   <tr><td>{@code allTablesOneShard}<td>5,000<td>1<td>200 GB
 *   <tr><td>{@code singleTableAllShards}<td>1<td>1,000<td>64 TB
 *   <tr><td>{@code topEightTablesAllShards}<td>8<td>1,000<td>194 TB
 *   <tr><td>{@code allTablesAllShards}<td>5,000<td>1,000<td>200 TB
 * </table>
 *
 * <p>The category makes this dispatchable by name from the Spanner load-test workflow, but the
 * weekly schedule runs the whole category with no {@code -Dtest}, which would sweep in a run of
 * this size. {@link #setUpClass()} therefore skips unless the class is named explicitly.
 *
 * <p>Each run scales the instance to {@value #TARGET_NODES} nodes, creates and drops its own
 * database, and restores the instance to {@value #IDLE_NODES} nodes.
 */
@Category(TemplateLoadTest.class)
@TemplateLoadTest(SourceDbToSpanner.class)
@RunWith(JUnit4.class)
public class MySQLLargeLT extends SourceDbToSpannerLTBase {

  private static final Logger LOG = LoggerFactory.getLogger(MySQLLargeLT.class);

  private static final String DATAFLOW_PROJECT = "span-cloud-migrations-testing";
  private static final String REGION = "asia-southeast1";
  private static final String BUCKET = "smt-llt";
  private static final String SPANNER_PROJECT = "cloud-teleport-testing";
  private static final String SPANNER_INSTANCE = "nokill-smt-test-instance";
  private static final String SPANNER_HOST = "https://preprod-spanner.sandbox.googleapis.com";

  /** Pinned rather than inherited from the base class, which honours {@code -DspecPath}. */
  private static final String TEMPLATE =
      "gs://dataflow-templates/latest/flex/Sourcedb_to_Spanner_Flex";

  /**
   * SMT emits the full 5,000-table schema regardless of scenario, so one file covers them all. It
   * lives in GCS rather than in test resources because it is far too large to check in.
   */
  private static final String DDL_OBJECT = "llt-benchmark-spanner-schema.sql";

  /** Both configs resolve the source password from Secret Manager rather than embedding it. */
  private static final String ONE_SHARD =
      "gs://" + BUCKET + "/shard_config_single_shard_secret.json";

  private static final String ALL_SHARDS = "gs://" + BUCKET + "/shard_config_public_secret.json";

  /** Empty selects every table; otherwise the template expects colon-separated names. */
  private static final String ALL_TABLES = "";

  private static final String SINGLE_TABLE = "wide_int_pk_uniform";
  private static final String TOP_EIGHT_TABLES =
      "tall_int_pk_uniform:tall_int_pk_non_uniform:tall_str_pk_uniform:tall_str_pk_non_uniform"
          + ":timestamp_pk_non_uniform:composite_pk_fanout:wide_int_pk_uniform"
          + ":wide_int_pk_non_uniform";

  static final int TARGET_NODES = 2000;
  static final int IDLE_NODES = 100;

  private static final int WORKERS = 300;
  private static final String MACHINE_TYPE = "n2d-highmem-4";
  private static final Duration JOB_TIMEOUT = Duration.ofHours(12);

  /** Scenarios differ only in how much data they move. */
  private enum Scenario {
    SINGLE_TABLE_ONE_SHARD(ONE_SHARD, SINGLE_TABLE),
    ALL_TABLES_ONE_SHARD(ONE_SHARD, ALL_TABLES),
    SINGLE_TABLE_ALL_SHARDS(ALL_SHARDS, SINGLE_TABLE),
    TOP_EIGHT_TABLES_ALL_SHARDS(ALL_SHARDS, TOP_EIGHT_TABLES),
    ALL_TABLES_ALL_SHARDS(ALL_SHARDS, ALL_TABLES);

    private final String shardConfig;
    private final String tables;

    Scenario(String shardConfig, String tables) {
      this.shardConfig = shardConfig;
      this.tables = tables;
    }
  }

  @BeforeClass
  public static void setUpClass() {
    assumeTrue(
        "Skipped: name this class in -Dtest to run it.",
        System.getProperty("test", "").contains(MySQLLargeLT.class.getSimpleName()));
    System.setProperty("project", DATAFLOW_PROJECT);
    System.setProperty("region", REGION);
    System.setProperty("artifactBucket", BUCKET);
    LoadTestBase.setUpClass();
  }

  @Before
  public void setUp() throws IOException {
    super.setUp();

    // Scale before the database exists so schema application also runs at benchmark size.
    scaleInstance(TARGET_NODES);

    spannerResourceManager =
        SpannerResourceManager.builder(testName, SPANNER_PROJECT, REGION)
            .useStaticInstance()
            .setInstanceId(SPANNER_INSTANCE)
            .useCustomHost(SPANNER_HOST)
            .setMonitoringClient(monitoringClient)
            .setSuppressVerboseLogs(true)
            .build();
    gcsResourceManager = createSpannerLTGcsResourceManager();
    this.dialect = SQLDialect.MYSQL;
  }

  @After
  public void cleanUp() {
    try {
      if (spannerResourceManager != null) {
        // On a static instance this drops only the generated database.
        ResourceManagerUtils.cleanResources(spannerResourceManager, gcsResourceManager);
      }
    } finally {
      try {
        scaleInstance(IDLE_NODES);
      } catch (RuntimeException e) {
        LOG.error("Failed to scale {} down; do it manually", SPANNER_INSTANCE, e);
      }
    }
  }

  @Test
  public void singleTableOneShard() throws Exception {
    run(Scenario.SINGLE_TABLE_ONE_SHARD);
  }

  @Test
  public void allTablesOneShard() throws Exception {
    run(Scenario.ALL_TABLES_ONE_SHARD);
  }

  @Test
  public void singleTableAllShards() throws Exception {
    run(Scenario.SINGLE_TABLE_ALL_SHARDS);
  }

  @Test
  public void topEightTablesAllShards() throws Exception {
    run(Scenario.TOP_EIGHT_TABLES_ALL_SHARDS);
  }

  @Test
  public void allTablesAllShards() throws Exception {
    run(Scenario.ALL_TABLES_ALL_SHARDS);
  }

  private void run(Scenario scenario) throws Exception {
    createSpannerDDLFromGcs();

    Map<String, String> params = new HashMap<>();
    // projectId is the Spanner project, which is not the Dataflow project here.
    params.put("projectId", SPANNER_PROJECT);
    params.put("instanceId", spannerResourceManager.getInstanceId());
    params.put("databaseId", spannerResourceManager.getDatabaseId());
    params.put("spannerHost", SPANNER_HOST);
    params.put("sourceConfigURL", scenario.shardConfig);
    params.put("sourceDbDialect", SQLDialect.MYSQL.name());
    params.put("workerMachineType", MACHINE_TYPE);
    // Under the run-scoped prefix, so cleanupAll() reaps it in teardown.
    params.put("outputDirectory", getOutputDirectory());
    if (!scenario.tables.isEmpty()) {
      params.put("tables", scenario.tables);
    }

    LaunchConfig.Builder options =
        LaunchConfig.builder(testName, TEMPLATE)
            .setParameters(params)
            .addEnvironment("numWorkers", WORKERS)
            .addEnvironment("maxWorkers", WORKERS)
            .addEnvironment("ipConfiguration", "WORKER_IP_PRIVATE")
            .addEnvironment("stagingLocation", "gs://" + BUCKET + "/staging")
            .addEnvironment("tempLocation", "gs://" + BUCKET + "/temp");

    PipelineLauncher.LaunchInfo jobInfo = launchJob(options);
    LOG.info("Launched {} as job {}", scenario, jobInfo.jobId());

    // Drains on timeout; otherwise teardown would drop the database under a running job.
    PipelineOperator.Result result =
        pipelineOperator.waitUntilDoneAndFinish(createConfig(jobInfo, JOB_TIMEOUT));
    assertThatResult(result).isLaunchFinished();

    Map<String, Double> metrics = getMetrics(jobInfo);
    populateResourceManagerMetrics(metrics);
    LOG.info("Metrics for {}:\n{}", scenario, metrics);
  }

  private void createSpannerDDLFromGcs() {
    Storage storage =
        StorageOptions.newBuilder()
            .setProjectId(DATAFLOW_PROJECT)
            .setCredentials(CREDENTIALS)
            .build()
            .getService();
    String ddl =
        new String(storage.readAllBytes(BlobId.of(BUCKET, DDL_OBJECT)), StandardCharsets.UTF_8);
    spannerResourceManager.executeDdlStatements(
        Arrays.stream(ddl.split(";")).filter(d -> !d.isBlank()).collect(Collectors.toList()));
  }

  private void scaleInstance(int nodeCount) {
    LOG.info("Scaling {} to {} nodes", SPANNER_INSTANCE, nodeCount);
    SpannerOptions options =
        SpannerOptions.newBuilder().setProjectId(SPANNER_PROJECT).setHost(SPANNER_HOST).build();
    try (Spanner spanner = options.getService()) {
      InstanceInfo instance =
          InstanceInfo.newBuilder(InstanceId.of(SPANNER_PROJECT, SPANNER_INSTANCE))
              .setNodeCount(nodeCount)
              .build();
      spanner.getInstanceAdminClient().updateInstance(instance, InstanceField.NODE_COUNT).get();
    } catch (Exception e) {
      throw new IllegalStateException(
          String.format("Failed to scale %s to %d nodes", SPANNER_INSTANCE, nodeCount), e);
    }
  }
}
