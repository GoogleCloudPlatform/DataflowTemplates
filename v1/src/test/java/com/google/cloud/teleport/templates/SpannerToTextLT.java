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
package com.google.cloud.teleport.templates;

import static com.google.cloud.teleport.it.common.matchers.TemplateAsserts.assertThatPipeline;
import static com.google.cloud.teleport.it.common.matchers.TemplateAsserts.assertThatResult;
import static com.google.cloud.teleport.it.gcp.artifacts.utils.ArtifactUtils.createStorageClient;
import static com.google.cloud.teleport.it.gcp.artifacts.utils.ArtifactUtils.getFullGcsPath;
import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.storage.Storage;
import com.google.cloud.teleport.it.common.PipelineLauncher.LaunchConfig;
import com.google.cloud.teleport.it.common.PipelineLauncher.LaunchInfo;
import com.google.cloud.teleport.it.common.PipelineOperator.Result;
import com.google.cloud.teleport.it.common.TestProperties;
import com.google.cloud.teleport.it.common.utils.ResourceManagerUtils;
import com.google.cloud.teleport.it.gcp.TemplateLoadTestBase;
import com.google.cloud.teleport.it.gcp.artifacts.GcsArtifactClient;
import com.google.cloud.teleport.it.gcp.datagenerator.DataGenerator;
import com.google.cloud.teleport.it.gcp.spanner.SpannerResourceManager;
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
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Performance test for {@link SpannerToText Spanner to GCS Text} template. */
@Category(TemplateLoadTest.class)
@TemplateLoadTest(SpannerToText.class)
@RunWith(JUnit4.class)
public class SpannerToTextLT extends TemplateLoadTestBase {
  private static final String SPEC_PATH =
      MoreObjects.firstNonNull(
          TestProperties.specPath(), "gs://dataflow-templates/latest/Spanner_to_GCS_Text");
  private static final String ARTIFACT_BUCKET = TestProperties.artifactBucket();
  private static final String TEST_ROOT_DIR = SpannerToTextLT.class.getSimpleName().toLowerCase();
  // 35,000,000 messages of the given schema make up approximately 10GB
  private static final String NUM_MESSAGES = "35000000";
  private static final String INPUT_PCOLLECTION =
      "Read all records/Read from Cloud Spanner/Read from Partitions.out0";
  private static final String OUTPUT_PCOLLECTION =
      "Write to storage/WriteFiles/RewindowIntoGlobal/Window.Assign.out0";
  private static SpannerResourceManager spannerResourceManager;
  private static GcsArtifactClient gcsClient;

  @Before
  public void setup() throws IOException {
    // Set up resource managers
    spannerResourceManager = SpannerResourceManager.builder(testName, project, region).build();
    Storage storageClient = createStorageClient(CREDENTIALS);
    gcsClient = GcsArtifactClient.builder(storageClient, ARTIFACT_BUCKET, TEST_ROOT_DIR).build();
  }

  @After
  public void teardown() {
    ResourceManagerUtils.cleanResources(spannerResourceManager, gcsClient);
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
            .setProjectId(project)
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
                    .addParameter("spannerProjectId", project)
                    .addParameter("spannerInstanceId", spannerResourceManager.getInstanceId())
                    .addParameter("spannerDatabaseId", spannerResourceManager.getDatabaseId())
                    .addParameter("spannerTable", name)
                    .addParameter("textWritePrefix", getTestMethodDirPath()))
            .build();

    // Act
    LaunchInfo info = pipelineLauncher.launch(project, region, options);
    assertThatPipeline(info).isRunning();
    Result result = pipelineOperator.waitUntilDone(createConfig(info, Duration.ofMinutes(60)));

    // Assert
    assertThatResult(result).isLaunchFinished();
    // check to see if messages reached the output bucket
    assertThat(gcsClient.listArtifacts(name, Pattern.compile(".*"))).isNotEmpty();

    // export results
    exportMetricsToBigQuery(info, getMetrics(info, INPUT_PCOLLECTION, OUTPUT_PCOLLECTION));
  }

  private String getTestMethodDirPath() {
    return getFullGcsPath(ARTIFACT_BUCKET, TEST_ROOT_DIR, gcsClient.runId(), testName);
  }
}
