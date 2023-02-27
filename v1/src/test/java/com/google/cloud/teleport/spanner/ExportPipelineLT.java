/*
 * Copyright (C) 2023 Google LLC
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
package com.google.cloud.teleport.spanner;

import static com.google.cloud.teleport.it.artifacts.ArtifactUtils.createGcsClient;
import static com.google.cloud.teleport.it.artifacts.ArtifactUtils.getFullGcsPath;
import static com.google.cloud.teleport.it.matchers.TemplateAsserts.assertThatPipeline;
import static com.google.cloud.teleport.it.matchers.TemplateAsserts.assertThatResult;
import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.storage.Storage;
import com.google.cloud.teleport.it.DataGenerator;
import com.google.cloud.teleport.it.TemplateLoadTestBase;
import com.google.cloud.teleport.it.TestProperties;
import com.google.cloud.teleport.it.artifacts.ArtifactClient;
import com.google.cloud.teleport.it.artifacts.GcsArtifactClient;
import com.google.cloud.teleport.it.common.ResourceManagerUtils;
import com.google.cloud.teleport.it.launcher.PipelineLauncher.LaunchConfig;
import com.google.cloud.teleport.it.launcher.PipelineLauncher.LaunchInfo;
import com.google.cloud.teleport.it.launcher.PipelineOperator.Result;
import com.google.cloud.teleport.it.spanner.DefaultSpannerResourceManager;
import com.google.cloud.teleport.it.spanner.SpannerResourceManager;
import com.google.cloud.teleport.metadata.TemplateLoadTest;
import com.google.common.base.MoreObjects;
import com.google.re2j.Pattern;
import java.io.IOException;
import java.text.ParseException;
import java.time.Duration;
import java.util.function.Function;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Performance tests for {@link ExportPipeline Spanner to GCS Avro} template. */
@TemplateLoadTest(ExportPipeline.class)
@RunWith(JUnit4.class)
public class ExportPipelineLT extends TemplateLoadTestBase {
  private static final String SPEC_PATH =
      MoreObjects.firstNonNull(
          TestProperties.specPath(), "gs://dataflow-templates/latest/Cloud_Spanner_to_GCS_Avro");
  private static final String ARTIFACT_BUCKET = TestProperties.artifactBucket();
  private static final String TEST_ROOT_DIR = ExportPipelineLT.class.getSimpleName().toLowerCase();
  // 35,000,000 messages of the given schema make up approximately 10GB

  private static final String NUM_MESSAGES = "35000000";
  private static final String INPUT_PCOLLECTION =
      "Run Export/Read all rows from Spanner/Read from Cloud Spanner/Read from Partitions.out0";
  private static final String OUTPUT_PCOLLECTION =
      "Run Export/Store Avro files/Write/RewindowIntoGlobal/Window.Assign.out0";
  private static SpannerResourceManager spannerResourceManager;
  private static ArtifactClient artifactClient;

  @Before
  public void setup() throws IOException {
    // Set up resource managers
    spannerResourceManager =
        DefaultSpannerResourceManager.builder(testName, PROJECT, REGION).build();
    Storage gcsClient = createGcsClient(CREDENTIALS);
    artifactClient = GcsArtifactClient.builder(gcsClient, ARTIFACT_BUCKET, TEST_ROOT_DIR).build();
  }

  @After
  public void teardown() {
    ResourceManagerUtils.cleanResources(spannerResourceManager, artifactClient);
  }

  @Test
  public void testBacklog10gb() throws IOException, ParseException, InterruptedException {
    testBacklog10gb(Function.identity());
  }

  public void testBacklog10gb(Function<LaunchConfig.Builder, LaunchConfig.Builder> paramsAdder)
      throws IOException, ParseException, InterruptedException {
    // Arrange
    String name = testName;
    // create spanner table
    String createTableStatement =
        String.format(
            "CREATE TABLE `%s` (\n"
                + "  eventId STRING(1024) NOT NULL,\n"
                + "  eventTimestamp INT64,\n"
                + "  ipv4 STRING(1024),\n"
                + "  ipv6 STRING(1024),\n"
                + "  country STRING(1024),\n"
                + "  username STRING(1024),\n"
                + "  quest STRING(1024),\n"
                + "  score INT64,\n"
                + "  completed BOOL,\n"
                + ") PRIMARY KEY(eventId)",
            name);
    spannerResourceManager.createTable(createTableStatement);
    DataGenerator dataGenerator =
        DataGenerator.builderWithSchemaTemplate(testName, "GAME_EVENT")
            .setQPS("1000000")
            .setMessagesLimit(NUM_MESSAGES)
            .setSinkType("SPANNER")
            .setProjectId(PROJECT)
            .setSpannerInstanceName(spannerResourceManager.getInstanceId())
            .setSpannerDatabaseName(spannerResourceManager.getDatabaseId())
            .setSpannerTableName(name)
            .setNumWorkers("50")
            .setMaxNumWorkers("100")
            .build();
    dataGenerator.execute(Duration.ofMinutes(30));
    LaunchConfig options =
        paramsAdder
            .apply(
                LaunchConfig.builder(testName, SPEC_PATH)
                    .addParameter("instanceId", spannerResourceManager.getInstanceId())
                    .addParameter("databaseId", spannerResourceManager.getDatabaseId())
                    .addParameter("outputDir", getTestMethodDirPath()))
            .build();

    // Act
    LaunchInfo info = pipelineLauncher.launch(PROJECT, REGION, options);
    assertThatPipeline(info).isRunning();
    Result result = pipelineOperator.waitUntilDone(createConfig(info, Duration.ofMinutes(60)));

    // Assert
    assertThatResult(result).isLaunchFinished();
    // check to see if messages reached the output topic
    assertThat(artifactClient.listArtifacts(name, Pattern.compile(".*"))).isNotEmpty();

    // export results
    exportMetricsToBigQuery(info, getMetrics(info, INPUT_PCOLLECTION, OUTPUT_PCOLLECTION));
  }

  private String getTestMethodDirPath() {
    return getFullGcsPath(ARTIFACT_BUCKET, TEST_ROOT_DIR, artifactClient.runId(), testName);
  }
}
