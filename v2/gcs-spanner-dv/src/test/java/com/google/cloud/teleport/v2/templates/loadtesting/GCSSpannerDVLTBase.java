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

import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.teleport.v2.spanner.migrations.transformation.CustomTransformation;
import com.google.monitoring.v3.Aggregation.Aligner;
import com.google.monitoring.v3.TimeInterval;
import com.google.protobuf.util.Timestamps;
import java.io.IOException;
import java.text.ParseException;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import java.util.stream.Collectors;
import org.apache.beam.it.common.PipelineLauncher.LaunchConfig;
import org.apache.beam.it.common.PipelineLauncher.LaunchInfo;
import org.apache.beam.it.common.PipelineOperator.Result;
import org.apache.beam.it.common.utils.PipelineUtils;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.TemplateLoadTestBase;
import org.apache.beam.it.gcp.artifacts.utils.ArtifactUtils;
import org.apache.beam.it.gcp.bigquery.BigQueryResourceManager;
import org.apache.beam.it.gcp.bigquery.BigQueryResourceManagerException;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.gcp.storage.GcsResourceManager;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for {@code gcs-spanner-dv} load tests.
 *
 * <p>By default each test creates an ephemeral Spanner database (via {@link
 * SpannerResourceManager}) that it populates itself. Tests that validate against a pre-existing
 * database override {@link #staticSpannerTarget()}; in that mode no {@link SpannerResourceManager}
 * is created, so the static database is never modified or dropped.
 */
public abstract class GCSSpannerDVLTBase extends TemplateLoadTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(GCSSpannerDVLTBase.class);

  protected static final String SPEC_PATH =
      System.getProperty(
          "specPath", "gs://dataflow-templates/latest/flex/Avro_to_Spanner_Data_Validator");

  private static final int SPANNER_NODE_COUNT = 10;
  private static final int NUM_WORKERS = 1;
  private static final int MAX_WORKERS = 100;
  private static final String SPANNER_CPU_METRIC =
      "spanner.googleapis.com/instance/cpu/utilization";
  private static final String CUSTOM_JAR_PATH =
      "../spanner-custom-shard/target/spanner-custom-shard-1.0-SNAPSHOT.jar";
  private static final String NULL_KEY_PART = "NULL";

  /** A pre-existing Spanner database. Never modified or cleaned up by the test. */
  protected record StaticSpannerTarget(String projectId, String instanceId, String databaseId) {}

  /**
   * Ephemeral Spanner database manager. {@code null} when {@link #staticSpannerTarget()} is set.
   */
  protected SpannerResourceManager spannerResourceManager;

  protected BigQueryResourceManager bigQueryResourceManager;

  /** Created on first artifact upload; {@code null} if the test uploads nothing. */
  private GcsResourceManager gcsResourceManager;

  /** The Spanner database the validation job reads from. */
  private StaticSpannerTarget spannerTarget;

  @Before
  @Override
  public void setUp() throws IOException {
    super.setUp();
    Optional<StaticSpannerTarget> staticTarget = staticSpannerTarget();
    if (staticTarget.isPresent()) {
      spannerTarget = staticTarget.get();
    } else {
      spannerResourceManager =
          SpannerResourceManager.builder(testName, project, region)
              .maybeUseStaticInstance(Optional.of(4))
              .setNodeCount(SPANNER_NODE_COUNT)
              .setMonitoringClient(monitoringClient)
              .setSuppressVerboseLogs(true)
              .build();
      spannerTarget =
          new StaticSpannerTarget(
              project,
              spannerResourceManager.getInstanceId(),
              spannerResourceManager.getDatabaseId());
    }
    LOG.info("Spanner target: {} (static={})", spannerTarget, staticTarget.isPresent());

    bigQueryResourceManager =
        BigQueryResourceManager.builder(testName, project, CREDENTIALS).build();
    bigQueryResourceManager.createDataset(region);
  }

  /**
   * Override to validate against a pre-existing Spanner database instead of an ephemeral one
   * created by the test. Defaults to empty (ephemeral database).
   */
  protected Optional<StaticSpannerTarget> staticSpannerTarget() {
    return Optional.empty();
  }

  /**
   * Pipeline-level Dataflow resource hints (e.g. {@code cpu_count=4}, {@code min_ram=60GB}). Each
   * entry is passed as a separate {@code resourceHints=<hint>} pipeline option. Resource hints are
   * preferred over a fixed machine type to avoid failures caused by stockouts of a single type.
   */
  protected List<String> resourceHints() {
    return List.of("cpu_count=4");
  }

  protected LaunchInfo launchValidationJob(String gcsInputDirectory, Duration jobTimeout)
      throws IOException {
    return launchValidationJob(gcsInputDirectory, jobTimeout, null, Map.of(), Map.of());
  }

  /**
   * Launches the validation job and waits for it to finish.
   *
   * @param customTransformation optional custom transformation. Its jar must already be uploaded
   *     (see {@link #uploadCustomShardJarToGcs}); {@link CustomTransformation#jarPath()} is
   *     relative to this test's GCS artifact directory, as in {@code GCSSpannerDVITBase}.
   */
  protected LaunchInfo launchValidationJob(
      String gcsInputDirectory,
      Duration jobTimeout,
      @Nullable CustomTransformation customTransformation,
      Map<String, String> additionalParameters,
      Map<String, Object> environmentOptions)
      throws IOException {
    String jobName = PipelineUtils.createJobName(testName);

    Map<String, String> parameters = new HashMap<>();
    parameters.put("projectId", spannerTarget.projectId());
    parameters.put("instanceId", spannerTarget.instanceId());
    parameters.put("databaseId", spannerTarget.databaseId());
    parameters.put("bigQueryDataset", bigQueryResourceManager.getDatasetId());
    parameters.put("gcsInputDirectory", gcsInputDirectory);

    if (customTransformation != null) {
      LOG.info("Custom transformation provided: {}", customTransformation.classPath());
      parameters.put("transformationJarPath", getGcsPath(customTransformation.jarPath()));
      parameters.put("transformationClassName", customTransformation.classPath());
      if (customTransformation.customParameters() != null) {
        parameters.put("transformationCustomParameters", customTransformation.customParameters());
      }
    }

    parameters.put("runId", jobName);
    parameters.putAll(additionalParameters);

    List<String> additionalPipelineOptions =
        resourceHints().stream().map(hint -> "resourceHints=" + hint).collect(Collectors.toList());

    LaunchConfig.Builder options =
        LaunchConfig.builder(jobName, SPEC_PATH)
            .addEnvironment("numWorkers", NUM_WORKERS)
            .addEnvironment("maxWorkers", MAX_WORKERS)
            .addEnvironment("additionalPipelineOptions", additionalPipelineOptions)
            .setParameters(parameters);
    environmentOptions.forEach(options::addEnvironment);

    LOG.info(
        "Launching validation job {} with parameters {} and pipeline options {}",
        jobName,
        parameters,
        additionalPipelineOptions);
    LaunchInfo jobInfo = pipelineLauncher.launch(project, region, options.build());
    assertThatPipeline(jobInfo).isRunning();

    Result result = pipelineOperator.waitUntilDone(createConfig(jobInfo, jobTimeout));
    LOG.info("Validation job {} finished with result {}", jobInfo.jobId(), result);
    assertThatResult(result).isLaunchFinished();

    return jobInfo;
  }

  protected void collectAndExportMetrics(LaunchInfo jobInfo)
      throws ParseException, IOException, InterruptedException {
    Map<String, Double> metrics = getMetrics(jobInfo);
    if (spannerResourceManager != null) {
      spannerResourceManager.collectMetrics(metrics);
    } else {
      collectStaticSpannerMetrics(jobInfo, metrics);
    }
    exportMetricsToBigQuery(jobInfo, metrics);
  }

  /**
   * Mirrors {@link SpannerResourceManager#collectMetrics} for a static Spanner database. The
   * interval is the window in which Dataflow workers were active, so the averages are not diluted
   * by launcher startup or post-job idle time. Failures are logged and never fail the test.
   */
  private void collectStaticSpannerMetrics(LaunchInfo jobInfo, Map<String, Double> metrics) {
    try {
      TimeInterval workerInterval = getWorkerTimeInterval(jobInfo);
      TimeInterval interval =
          TimeInterval.newBuilder()
              .setStartTime(
                  workerInterval.hasStartTime()
                      ? workerInterval.getStartTime()
                      : Timestamps.parse(jobInfo.createTime()))
              .setEndTime(
                  workerInterval.hasEndTime()
                      ? workerInterval.getEndTime()
                      : Timestamps.fromMillis(System.currentTimeMillis()))
              .build();
      String filter =
          String.format(
              "metric.type=\"%s\" AND resource.type=\"spanner_instance\" AND"
                  + " resource.label.instance_id=\"%s\" AND metric.label.database=\"%s\"",
              SPANNER_CPU_METRIC, spannerTarget.instanceId(), spannerTarget.databaseId());
      LOG.info(
          "Collecting static Spanner CPU metrics from {} to {}",
          Timestamps.toString(interval.getStartTime()),
          Timestamps.toString(interval.getEndTime()));
      putIfNotNull(
          metrics,
          "Spanner_AverageCpuUtilization",
          monitoringClient.getAggregatedMetric(
              spannerTarget.projectId(), filter, interval, Aligner.ALIGN_MEAN));
      putIfNotNull(
          metrics,
          "Spanner_MaxCpuUtilization",
          monitoringClient.getAggregatedMetric(
              spannerTarget.projectId(), filter, interval, Aligner.ALIGN_MAX));
    } catch (Exception e) {
      // Spanner metrics are informational; never fail the validation assertions because of them.
      LOG.warn("Failed to collect static Spanner metrics", e);
    }
  }

  private static void putIfNotNull(Map<String, Double> metrics, String key, Double value) {
    if (value == null) {
      LOG.warn("No value for metric {}", key);
      return;
    }
    metrics.put(key, value);
  }

  /**
   * Row counts of the {@code MismatchedRecords} table grouped by (schema, table, mismatch type,
   * shard), keyed by {@link #mismatchKey}. Use this instead of reading the table row by row, which
   * is infeasible at load-test scale. Returns an empty map if the table does not exist, i.e. the
   * job wrote no mismatch rows.
   */
  protected Map<String, Long> countMismatchedRecords() {
    String query =
        String.format(
            "SELECT CONCAT(IFNULL(schema_name, '%1$s'), '/', table_name, '/', mismatch_type, '/',"
                + " IFNULL(shard_id, '%1$s')), COUNT(*) FROM `%2$s.%3$s.MismatchedRecords`"
                + " GROUP BY 1",
            NULL_KEY_PART,
            bigQueryResourceManager.getProjectId(),
            bigQueryResourceManager.getDatasetId());
    Map<String, Long> counts = new HashMap<>();
    TableResult result;
    try {
      result = bigQueryResourceManager.runQuery(query);
    } catch (BigQueryResourceManagerException e) {
      if (e.getCause() instanceof BigQueryException bqe && bqe.getCode() == 404) {
        LOG.info("MismatchedRecords table does not exist; treating as no mismatches");
        return counts;
      }
      throw e;
    }
    for (FieldValueList row : result.iterateAll()) {
      counts.put(row.get(0).getStringValue(), row.get(1).getLongValue());
    }
    LOG.info("MismatchedRecords counts: {}", new TreeMap<>(counts));
    return counts;
  }

  /**
   * Key format of {@link #countMismatchedRecords()}. Pass {@code null} for a NULL schema or shard
   * (e.g. unsharded sources, or Spanner-side {@code MISSING_IN_SOURCE} records).
   */
  protected static String mismatchKey(
      @Nullable String schemaName,
      String tableName,
      String mismatchType,
      @Nullable String shardId) {
    return String.join(
        "/",
        Objects.toString(schemaName, NULL_KEY_PART),
        tableName,
        mismatchType,
        Objects.toString(shardId, NULL_KEY_PART));
  }

  /**
   * Uploads the {@code spanner-custom-shard} jar (built by the same Maven reactor) to {@code
   * <gcsPathPrefix>/customTransformation.jar}. Mirrors {@code GCSSpannerDVITBase}.
   */
  protected void uploadCustomShardJarToGcs(String gcsPathPrefix) throws IOException {
    gcsResourceManager()
        .uploadArtifact(gcsPathPrefix + "/customTransformation.jar", CUSTOM_JAR_PATH);
  }

  /** Full {@code gs://} path of an artifact uploaded by this test. */
  protected String getGcsPath(String artifactId) {
    return ArtifactUtils.getFullGcsPath(
        gcsResourceManager().getBucket(),
        getClass().getSimpleName(),
        gcsResourceManager().runId(),
        artifactId);
  }

  private GcsResourceManager gcsResourceManager() {
    if (gcsResourceManager == null) {
      gcsResourceManager = createSpannerLTGcsResourceManager();
    }
    return gcsResourceManager;
  }

  @After
  public void cleanUp() {
    // ResourceManagerUtils skips null managers: spannerResourceManager is null in static mode and
    // gcsResourceManager is null unless the test uploaded artifacts.
    ResourceManagerUtils.cleanResources(
        spannerResourceManager, bigQueryResourceManager, gcsResourceManager);
  }
}
