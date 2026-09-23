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
import java.util.HashMap;
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

/** Base class for {@code gcs-spanner-dv} load tests. */
public abstract class GCSSpannerDVLTBase extends TemplateLoadTestBase {

  protected static final String SPEC_PATH =
      System.getProperty(
          "specPath", "gs://dataflow-templates/latest/flex/Avro_to_Spanner_Data_Validator");

  protected SpannerResourceManager spannerResourceManager;
  protected BigQueryResourceManager bigQueryResourceManager;

  @Before
  @Override
  public void setUp() throws IOException {
    super.setUp();
    spannerResourceManager =
        SpannerResourceManager.builder(testName, project, region)
            .maybeUseStaticInstance(Optional.of(4))
            .setSuppressVerboseLogs(true)
            .build();

    bigQueryResourceManager =
        BigQueryResourceManager.builder(testName, project, CREDENTIALS).build();
    bigQueryResourceManager.createDataset(region);
  }

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

    LaunchConfig.Builder options =
        LaunchConfig.builder(jobName, SPEC_PATH)
            .addEnvironment("additionalPipelineOptions", List.of("resourceHints=cpu_count=4"))
            .setParameters(parameters);

    LaunchInfo jobInfo = pipelineLauncher.launch(project, region, options.build());
    assertThatPipeline(jobInfo).isRunning();

    Result result = pipelineOperator.waitUntilDone(createConfig(jobInfo, jobTimeout));
    assertThatResult(result).isLaunchFinished();

    return jobInfo;
  }

  @After
  public void cleanUp() {
    ResourceManagerUtils.cleanResources(spannerResourceManager, bigQueryResourceManager);
  }
}
