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

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.beam.it.common.PipelineLauncher.LaunchConfig;
import org.apache.beam.it.common.PipelineLauncher.LaunchInfo;
import org.apache.beam.it.common.PipelineOperator.Result;
import org.apache.beam.it.common.utils.PipelineUtils;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.TemplateLoadTestBase;
import org.apache.beam.it.gcp.bigquery.BigQueryResourceManager;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for {@code gcs-spanner-dv} load tests.
 *
 * <p>This class is deliberately thin. It owns only resource-manager wiring, phase timing, and
 * cleanup. All fixture logic (schema generation, data population, expectations) belongs in the
 * concrete load test, because no two load tests are expected to share a fixture.
 *
 * <p><b>Runner note:</b> load tests extend {@link TemplateLoadTestBase} rather than {@code
 * TemplateTestBase} (used by the integration tests in this module). {@code TemplateTestBase} stages
 * the template from the local source tree whenever {@code -DspecPath} is absent, which is exactly
 * how the weekly load-test workflow invokes Maven. Load tests must instead exercise the
 * <em>released</em> template, so the spec path defaults to the published flex template.
 *
 * <p><b>JUnit lifecycle:</b> {@link #setUpLoadTestResources()} is annotated {@link Before} and is
 * <em>not</em> an override of {@code LoadTestBase#setUp()}. JUnit runs superclass {@code @Before}
 * methods first, so the framework's own setup completes before this one. Subclasses must likewise
 * declare a distinctly named {@code @Before} method rather than redeclaring {@code setUp()}, which
 * would silently override the framework hook.
 */
public abstract class GCSSpannerDVLTBase extends TemplateLoadTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(GCSSpannerDVLTBase.class);

  /** Grep handle for isolating this test's output in a multi-hour CI log. */
  protected static final String LOG_TAG = "[DV-LT]";

  /**
   * The released flex template under test. Overridable with {@code -DspecPath} to point a run at a
   * release candidate or a locally staged build.
   */
  protected static final String SPEC_PATH =
      System.getProperty(
          "specPath", "gs://dataflow-templates/latest/flex/Avro_to_Spanner_Data_Validator");

  /**
   * Index into {@code TestConstants.SPANNER_TEST_INSTANCES}, which resolves to {@code teleport4}.
   * Chosen to avoid {@code teleport3}, used by the {@code sourcedb-to-spanner} load tests.
   *
   * <p>Node count is intentionally not configured: {@code maybeUseStaticInstance} forces reuse of a
   * pooled instance on the CI project, and {@code setNodeCount} is ignored on that path.
   */
  private static final int STATIC_SPANNER_INSTANCE_INDEX = 4;

  protected SpannerResourceManager spannerResourceManager;

  /** Destination for the pipeline's own validation output, and the source of all assertions. */
  protected BigQueryResourceManager bigQueryResourceManager;

  private final Map<String, Duration> phaseTimings = new LinkedHashMap<>();
  private Instant testStart;

  /** A phase body that is allowed to throw, so phases can wrap arbitrary setup code. */
  @FunctionalInterface
  protected interface PhaseBody {
    void run() throws Exception;
  }

  @Before
  public void setUpLoadTestResources() {
    testStart = Instant.now();
    spannerResourceManager =
        SpannerResourceManager.builder(testName, project, region)
            .maybeUseStaticInstance(Optional.of(STATIC_SPANNER_INSTANCE_INDEX))
            // Without this the manager logs every DDL statement it executes, which is thousands
            // of lines for a large schema.
            .setSuppressVerboseLogs(true)
            .build();

    bigQueryResourceManager =
        BigQueryResourceManager.builder(testName, project, CREDENTIALS).build();
    bigQueryResourceManager.createDataset(region);

    LOG.info(
        "{} Resources resolved: project={} region={} spannerInstance={} spannerDatabase={}"
            + " bigQueryDataset={} specPath={}",
        LOG_TAG,
        project,
        region,
        spannerResourceManager.getInstanceId(),
        spannerResourceManager.getDatabaseId(),
        bigQueryResourceManager.getDatasetId(),
        SPEC_PATH);
  }

  /**
   * Launches the validation template against the fixture built by the subclass and blocks until the
   * job reaches a terminal state.
   *
   * <p>No wait is needed between populating Spanner and launching: {@code SpannerReaderTransform}
   * reads at an exact staleness of a few seconds, but Dataflow worker startup alone takes minutes,
   * so the read timestamp is always well after the fixture was committed. The integration tests
   * sleep here only because the direct runner starts reading almost immediately.
   *
   * @param gcsInputDirectory the Avro source directory to validate against
   * @param jobTimeout how long to wait for the job to finish before failing the test
   */
  protected LaunchInfo launchValidationJob(String gcsInputDirectory, Duration jobTimeout)
      throws IOException {
    String jobName = PipelineUtils.createJobName(testName);

    Map<String, String> parameters = new HashMap<>();
    parameters.put("projectId", project);
    parameters.put("instanceId", spannerResourceManager.getInstanceId());
    parameters.put("databaseId", spannerResourceManager.getDatabaseId());
    parameters.put("bigQueryDataset", bigQueryResourceManager.getDatasetId());
    parameters.put("gcsInputDirectory", gcsInputDirectory);
    parameters.put("runId", jobName);

    LOG.info("{} Launching job {} with parameters {}", LOG_TAG, jobName, parameters);

    LaunchConfig.Builder options =
        LaunchConfig.builder(jobName, SPEC_PATH)
            .addEnvironment("additionalPipelineOptions", List.of("resourceHints=cpu_count=4"))
            .setParameters(parameters);

    LaunchInfo jobInfo = pipelineLauncher.launch(project, region, options.build());
    assertThatPipeline(jobInfo).isRunning();
    LOG.info("{} Job launched: id={} state={}", LOG_TAG, jobInfo.jobId(), jobInfo.state());

    Result result = pipelineOperator.waitUntilDone(createConfig(jobInfo, jobTimeout));
    LOG.info("{} Job {} finished with result {}", LOG_TAG, jobInfo.jobId(), result);
    assertThatResult(result).isLaunchFinished();

    return jobInfo;
  }

  /**
   * Runs {@code body} as a named phase, logging its start, end and duration, and recording the
   * duration for the end-of-test summary.
   *
   * <p>The duration is recorded even when the body throws, so a failing run still reports how far
   * it got and how long each completed phase took. This matters because these tests cannot be run
   * locally: every diagnosis has to come out of a single CI log.
   */
  protected void phase(String name, PhaseBody body) throws Exception {
    LOG.info("{} PHASE START {}", LOG_TAG, name);
    Instant start = Instant.now();
    try {
      body.run();
    } finally {
      Duration elapsed = Duration.between(start, Instant.now());
      phaseTimings.put(name, elapsed);
      LOG.info("{} PHASE END   {} took {}s", LOG_TAG, name, elapsed.toSeconds());
    }
  }

  @After
  public void cleanUpLoadTestResources() {
    Instant cleanupStart = Instant.now();
    try {
      // Matches every other LT base in this repo. The single-argument overload delegates to
      // cleanResources(false, ...), which logs a cleanup failure instead of failing the test: a
      // leaked database is worth a WARN, but it is not evidence of a pipeline defect and should
      // not turn a green run red. On a static instance this drops the database only; the pooled
      // instance survives. Null managers are skipped by the helper.
      ResourceManagerUtils.cleanResources(spannerResourceManager, bigQueryResourceManager);
    } finally {
      phaseTimings.put("cleanup", Duration.between(cleanupStart, Instant.now()));
      logPhaseSummary();
    }
  }

  private void logPhaseSummary() {
    StringBuilder summary = new StringBuilder("\n").append(LOG_TAG).append(" PHASE SUMMARY\n");
    phaseTimings.forEach(
        (name, elapsed) ->
            summary.append(String.format("  %-18s %6ds%n", name, elapsed.toSeconds())));
    if (testStart != null) {
      summary.append(
          String.format(
              "  %-18s %6ds%n", "TOTAL", Duration.between(testStart, Instant.now()).toSeconds()));
    }
    LOG.info(summary.toString());
  }
}
