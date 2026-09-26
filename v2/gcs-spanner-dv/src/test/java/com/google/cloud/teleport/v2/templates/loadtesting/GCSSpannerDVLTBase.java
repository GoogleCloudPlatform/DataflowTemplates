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

import com.google.monitoring.v3.Aggregation.Aligner;
import com.google.monitoring.v3.TimeInterval;
import com.google.protobuf.util.Timestamps;
import java.io.IOException;
import java.text.ParseException;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.beam.it.common.PipelineLauncher.LaunchConfig;
import org.apache.beam.it.common.PipelineLauncher.LaunchInfo;
import org.apache.beam.it.common.PipelineOperator.Result;
import org.apache.beam.it.common.ResourceManager;
import org.apache.beam.it.common.utils.PipelineUtils;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.TemplateLoadTestBase;
import org.apache.beam.it.gcp.bigquery.BigQueryResourceManager;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for {@code gcs-spanner-dv} load tests.
 *
 * <p>By default each test gets an ephemeral Spanner database (via {@link SpannerResourceManager})
 * and an ephemeral BigQuery dataset. Tests that validate against pre-existing static Spanner
 * resources override {@link #createSpannerResourceManager()} to return {@code null} and supply the
 * static identifiers through {@link #spannerProjectId()}, {@link #spannerInstanceId()} and {@link
 * #spannerDatabaseId()}. Worker sizing is controlled through Dataflow resource hints ({@link
 * #resourceHints()}) rather than a fixed machine type, to avoid failures caused by stockouts of a
 * single machine type.
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

  /** Ephemeral Spanner resource manager; {@code null} when the test uses static Spanner. */
  protected SpannerResourceManager spannerResourceManager;

  protected BigQueryResourceManager bigQueryResourceManager;

  @Before
  @Override
  public void setUp() throws IOException {
    super.setUp();
    spannerResourceManager = createSpannerResourceManager();
    LOG.info(
        "[DV-LT] Spanner target: project={}, instance={}, database={}, ephemeral={}",
        spannerProjectId(),
        spannerInstanceId(),
        spannerDatabaseId(),
        spannerResourceManager != null);

    bigQueryResourceManager =
        BigQueryResourceManager.builder(testName, project, CREDENTIALS).build();
    bigQueryResourceManager.createDataset(region);
    LOG.info(
        "[DV-LT] Created BigQuery dataset {}.{} in {}",
        bigQueryResourceManager.getProjectId(),
        bigQueryResourceManager.getDatasetId(),
        region);
  }

  /**
   * Creates the Spanner resource manager used by the test. Return {@code null} to validate against
   * static Spanner resources, in which case {@link #spannerInstanceId()} and {@link
   * #spannerDatabaseId()} must be overridden.
   */
  protected @Nullable SpannerResourceManager createSpannerResourceManager() {
    return SpannerResourceManager.builder(testName, project, region)
        .maybeUseStaticInstance(Optional.of(4))
        .setNodeCount(SPANNER_NODE_COUNT)
        .setMonitoringClient(monitoringClient)
        .setSuppressVerboseLogs(true)
        .build();
  }

  /** Project of the Spanner database under validation. */
  protected String spannerProjectId() {
    return project;
  }

  /** Instance of the Spanner database under validation. */
  protected String spannerInstanceId() {
    return Objects.requireNonNull(
            spannerResourceManager,
            "spannerInstanceId() must be overridden when no SpannerResourceManager is used")
        .getInstanceId();
  }

  /** Spanner database under validation. */
  protected String spannerDatabaseId() {
    return Objects.requireNonNull(
            spannerResourceManager,
            "spannerDatabaseId() must be overridden when no SpannerResourceManager is used")
        .getDatabaseId();
  }

  /**
   * Pipeline-level Dataflow resource hints (e.g. {@code cpu_count=4}, {@code min_ram=60GB}). Each
   * entry is passed as a separate {@code resourceHints=<hint>} pipeline option.
   */
  protected List<String> resourceHints() {
    return List.of("cpu_count=4");
  }

  protected LaunchInfo launchValidationJob(String gcsInputDirectory, Duration jobTimeout)
      throws IOException {
    return launchValidationJob(gcsInputDirectory, jobTimeout, Map.of(), Map.of());
  }

  protected LaunchInfo launchValidationJob(
      String gcsInputDirectory,
      Duration jobTimeout,
      Map<String, String> additionalParameters,
      Map<String, Object> environmentOptions)
      throws IOException {
    String jobName = PipelineUtils.createJobName(testName);

    Map<String, String> parameters = new HashMap<>();
    parameters.put("projectId", spannerProjectId());
    parameters.put("instanceId", spannerInstanceId());
    parameters.put("databaseId", spannerDatabaseId());
    parameters.put("bigQueryDataset", bigQueryResourceManager.getDatasetId());
    parameters.put("gcsInputDirectory", gcsInputDirectory);
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
        "[DV-LT] Launching validation job. jobName={}, specPath={}, project={}, region={},"
            + " numWorkers={}, maxWorkers={}, additionalPipelineOptions={},"
            + " extraEnvironment={}, timeout={}, parameters={}",
        jobName,
        SPEC_PATH,
        project,
        region,
        NUM_WORKERS,
        MAX_WORKERS,
        additionalPipelineOptions,
        environmentOptions,
        jobTimeout,
        new TreeMap<>(parameters));

    Instant launchStart = Instant.now();
    LaunchInfo jobInfo = pipelineLauncher.launch(project, region, options.build());
    LOG.info(
        "[DV-LT] Launched job. jobId={}, state={}, createTime={}, launchLatency={}",
        jobInfo.jobId(),
        jobInfo.state(),
        jobInfo.createTime(),
        Duration.between(launchStart, Instant.now()));
    assertThatPipeline(jobInfo).isRunning();

    LOG.info("[DV-LT] Waiting up to {} for job {} to finish", jobTimeout, jobInfo.jobId());
    Result result = pipelineOperator.waitUntilDone(createConfig(jobInfo, jobTimeout));
    LOG.info(
        "[DV-LT] Job {} wait result={}, elapsedSinceLaunch={}",
        jobInfo.jobId(),
        result,
        Duration.between(launchStart, Instant.now()));
    assertThatResult(result).isLaunchFinished();

    return jobInfo;
  }

  protected void collectAndExportMetrics(LaunchInfo jobInfo)
      throws ParseException, IOException, InterruptedException {
    LOG.info("[DV-LT] Collecting metrics for job {}", jobInfo.jobId());
    Map<String, Double> metrics = getMetrics(jobInfo);
    if (spannerResourceManager != null) {
      spannerResourceManager.collectMetrics(metrics);
    } else {
      collectStaticSpannerMetrics(jobInfo, metrics);
    }
    LOG.info("[DV-LT] Collected metrics for job {}: {}", jobInfo.jobId(), new TreeMap<>(metrics));
    exportMetricsToBigQuery(jobInfo, metrics);
  }

  /**
   * Mirrors {@link SpannerResourceManager#collectMetrics} for a static Spanner database, using the
   * job's lifetime (creation until now) as the measurement interval.
   */
  private void collectStaticSpannerMetrics(LaunchInfo jobInfo, Map<String, Double> metrics) {
    try {
      TimeInterval interval =
          TimeInterval.newBuilder()
              .setStartTime(Timestamps.parse(jobInfo.createTime()))
              .setEndTime(Timestamps.fromMillis(System.currentTimeMillis()))
              .build();
      String filter =
          String.format(
              "metric.type=\"%s\" AND resource.type=\"spanner_instance\" AND"
                  + " resource.label.instance_id=\"%s\" AND metric.label.database=\"%s\"",
              SPANNER_CPU_METRIC, spannerInstanceId(), spannerDatabaseId());
      LOG.info(
          "[DV-LT] Querying static Spanner CPU metrics. project={}, filter={}, interval={}",
          spannerProjectId(),
          filter,
          interval);
      putIfNotNull(
          metrics,
          "Spanner_AverageCpuUtilization",
          monitoringClient.getAggregatedMetric(
              spannerProjectId(), filter, interval, Aligner.ALIGN_MEAN));
      putIfNotNull(
          metrics,
          "Spanner_MaxCpuUtilization",
          monitoringClient.getAggregatedMetric(
              spannerProjectId(), filter, interval, Aligner.ALIGN_MAX));
    } catch (Exception e) {
      // Spanner metrics are informational; never fail the validation assertions because of them.
      LOG.warn("[DV-LT] Failed to collect static Spanner metrics", e);
    }
  }

  private static void putIfNotNull(Map<String, Double> metrics, String key, Double value) {
    if (value == null) {
      LOG.warn("[DV-LT] No value for metric {}", key);
      return;
    }
    metrics.put(key, value);
  }

  @After
  public void cleanUp() {
    ResourceManager[] managers =
        Stream.of(spannerResourceManager, bigQueryResourceManager)
            .filter(Objects::nonNull)
            .toArray(ResourceManager[]::new);
    LOG.info("[DV-LT] Cleaning up {} resource manager(s)", managers.length);
    ResourceManagerUtils.cleanResources(managers);
  }
}
