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

import com.google.cloud.teleport.v2.spanner.migrations.transformation.CustomTransformation;
import java.io.IOException;
import java.text.ParseException;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.apache.beam.it.common.PipelineLauncher.LaunchConfig;
import org.apache.beam.it.common.PipelineLauncher.LaunchInfo;
import org.apache.beam.it.common.PipelineOperator.Result;
import org.apache.beam.it.common.utils.PipelineUtils;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.TemplateLoadTestBase;
import org.apache.beam.it.gcp.artifacts.utils.ArtifactUtils;
import org.apache.beam.it.gcp.bigquery.BigQueryResourceManager;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.gcp.storage.GcsResourceManager;
import org.junit.After;

/**
 * Base class for Load Tests (LT) of the GCS-to-Spanner Data Validation ({@code gcs-spanner-dv})
 * template.
 */
public abstract class GCSSpannerDVLTBase extends TemplateLoadTestBase {

  private static final int SPANNER_NODE_COUNT = 10;
  private static final int NUM_WORKERS = 1;
  private static final int MAX_WORKERS = 100;
  private static final String CUSTOM_JAR_PATH =
      "../spanner-custom-shard/target/spanner-custom-shard-1.0-SNAPSHOT.jar";

  protected SpannerResourceManager spannerResourceManager;
  protected BigQueryResourceManager bigQueryResourceManager;
  protected GcsResourceManager gcsResourceManager;

  /** Sets up an ephemeral Spanner database, BigQuery dataset, and GCS manager. */
  protected void setUpResourceManagers() throws IOException {
    spannerResourceManager =
        SpannerResourceManager.builder(testName, project, region)
            .maybeUseStaticInstance(Optional.of(4))
            .setNodeCount(SPANNER_NODE_COUNT)
            .setMonitoringClient(monitoringClient)
            .setSuppressVerboseLogs(true)
            .build();
    setUpBigQueryAndGcsResourceManagers();
  }

  /** Sets up a pre-existing static Spanner database, BigQuery dataset, and GCS manager. */
  protected void setUpResourceManagers(
      String spannerProjectId, String spannerInstanceId, String spannerDatabaseId)
      throws IOException {
    spannerResourceManager =
        SpannerResourceManager.builder(testName, spannerProjectId, region)
            .setInstanceId(spannerInstanceId)
            .setDatabaseId(spannerDatabaseId)
            .useStaticDatabase()
            .setMonitoringClient(monitoringClient)
            .setSuppressVerboseLogs(true)
            .build();
    setUpBigQueryAndGcsResourceManagers();
  }

  private void setUpBigQueryAndGcsResourceManagers() throws IOException {
    bigQueryResourceManager =
        BigQueryResourceManager.builder(testName, project, CREDENTIALS).build();
    bigQueryResourceManager.createDataset(region);
    gcsResourceManager = createSpannerLTGcsResourceManager();
  }

  protected LaunchInfo launchValidationJob(String gcsInputDirectory, Duration jobTimeout)
      throws IOException {
    return launchValidationJob(gcsInputDirectory, jobTimeout, Map.of(), Map.of());
  }

  /**
   * Launches the validation job with a custom transformation and waits for it to finish.
   *
   * @param customTransformation custom transformation. Its jar must already be uploaded (see {@link
   *     #uploadCustomShardJarToGcs}); {@link CustomTransformation#jarPath()} is relative to this
   *     test's GCS artifact directory, as in {@code GCSSpannerDVITBase}.
   */
  protected LaunchInfo launchValidationJob(
      String gcsInputDirectory,
      Duration jobTimeout,
      CustomTransformation customTransformation,
      Map<String, String> additionalParameters,
      Map<String, Object> environmentOptions)
      throws IOException {
    Objects.requireNonNull(customTransformation, "customTransformation");
    Map<String, String> allParameters = new HashMap<>();
    allParameters.put("transformationJarPath", getGcsPath(customTransformation.jarPath()));
    allParameters.put("transformationClassName", customTransformation.classPath());
    if (customTransformation.customParameters() != null) {
      allParameters.put("transformationCustomParameters", customTransformation.customParameters());
    }
    allParameters.putAll(additionalParameters);
    return launchValidationJob(gcsInputDirectory, jobTimeout, allParameters, environmentOptions);
  }

  /** Launches the validation job and waits for it to finish. */
  protected LaunchInfo launchValidationJob(
      String gcsInputDirectory,
      Duration jobTimeout,
      Map<String, String> additionalParameters,
      Map<String, Object> environmentOptions)
      throws IOException {
    String jobName = PipelineUtils.createJobName(testName);

    // Populate base template parameters from the configured Spanner and BigQuery targets.
    Map<String, String> parameters = new HashMap<>();
    parameters.put("projectId", spannerResourceManager.getProjectId());
    parameters.put("instanceId", spannerResourceManager.getInstanceId());
    parameters.put("databaseId", spannerResourceManager.getDatabaseId());
    parameters.put("bigQueryDataset", bigQueryResourceManager.getDatasetId());
    parameters.put("gcsInputDirectory", gcsInputDirectory);
    parameters.put("runId", jobName);
    parameters.putAll(additionalParameters);

    // Default to cpu_count=4 resource hint; larger LTs can override "additionalPipelineOptions"
    // via environmentOptions (which replaces this map entry in LaunchConfig.Builder).
    LaunchConfig.Builder options =
        LaunchConfig.builder(jobName, getTemplateSpecPath())
            .addEnvironment("numWorkers", NUM_WORKERS)
            .addEnvironment("maxWorkers", MAX_WORKERS)
            .addEnvironment("additionalPipelineOptions", List.of("resourceHints=cpu_count=4"))
            .setParameters(parameters);
    environmentOptions.forEach(options::addEnvironment);

    // Launch the pipeline and wait until it finishes.
    LaunchInfo jobInfo = pipelineLauncher.launch(project, region, options.build());
    assertThatPipeline(jobInfo).isRunning();

    Result result = pipelineOperator.waitUntilDone(createConfig(jobInfo, jobTimeout));
    assertThatResult(result).isLaunchFinished();

    return jobInfo;
  }

  protected void collectAndExportMetrics(LaunchInfo jobInfo)
      throws ParseException, IOException, InterruptedException {
    Map<String, Double> metrics = getMetrics(jobInfo);
    spannerResourceManager.collectMetrics(metrics);
    exportMetricsToBigQuery(jobInfo, metrics);
  }

  protected void uploadCustomShardJarToGcs(String gcsPathPrefix) throws IOException {
    gcsResourceManager.uploadArtifact(gcsPathPrefix + "/customTransformation.jar", CUSTOM_JAR_PATH);
  }

  protected String getGcsPath(String artifactId) {
    return ArtifactUtils.getFullGcsPath(
        gcsResourceManager.getBucket(),
        getClass().getSimpleName(),
        gcsResourceManager.runId(),
        artifactId);
  }

  @After
  public final void cleanUp() {
    ResourceManagerUtils.cleanResources(
        spannerResourceManager, bigQueryResourceManager, gcsResourceManager);
  }
}
