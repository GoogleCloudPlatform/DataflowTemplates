/*
 * Copyright (C) 2022 Google LLC
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

import static com.google.cloud.teleport.it.artifacts.ArtifactUtils.createGcsClient;
import static com.google.cloud.teleport.it.artifacts.ArtifactUtils.createTestPath;
import static com.google.cloud.teleport.it.artifacts.ArtifactUtils.createTestSuiteDirPath;
import static com.google.cloud.teleport.it.dataflow.DataflowUtils.createJobName;
import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.teleport.it.TestProperties;
import com.google.cloud.teleport.it.artifacts.ArtifactClient;
import com.google.cloud.teleport.it.artifacts.ArtifactGcsSdkClient;
import com.google.cloud.teleport.it.dataflow.DataflowOperator;
import com.google.cloud.teleport.it.dataflow.DataflowOperator.Result;
import com.google.cloud.teleport.it.dataflow.DataflowTemplateClient;
import com.google.cloud.teleport.it.dataflow.DataflowTemplateClient.JobInfo;
import com.google.cloud.teleport.it.dataflow.DataflowTemplateClient.JobState;
import com.google.cloud.teleport.it.dataflow.DataflowTemplateClient.LaunchOptions;
import com.google.cloud.teleport.it.dataflow.FlexTemplateClient;
import com.google.cloud.teleport.v2.templates.StreamingDataGenerator.SinkType;
import com.google.common.io.Resources;
import com.google.re2j.Pattern;
import java.io.IOException;
import java.util.List;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Integration test for {@link StreamingDataGenerator}. */
@RunWith(JUnit4.class)
public final class StreamingDataGeneratorIT {
  private static final TestProperties PROPERTIES = new TestProperties();

  private static final String SCHEMA_FILE = "gameevent.json";
  private static final String LOCAL_SCHEMA_PATH = Resources.getResource(SCHEMA_FILE).getPath();

  private static final String TEST_ROOT_DIR = "streaming-data-generator";
  private static final String TEST_DIR = createTestSuiteDirPath(TEST_ROOT_DIR);
  private static final String SCHEMA_FILE_GCS_PATH = String.format("%s/%s", TEST_DIR, SCHEMA_FILE);

  private static final String NUM_SHARDS_KEY = "numShards";
  private static final String OUTPUT_DIRECTORY_KEY = "outputDirectory";
  private static final String QPS_KEY = "qps";
  private static final String SCHEMA_LOCATION_KEY = "schemaLocation";
  private static final String SINK_TYPE_KEY = "sinkType";
  private static final String WINDOW_DURATION_KEY = "windowDuration";

  private static final String DEFAULT_QPS = "15";
  private static final String DEFAULT_WINDOW_DURATION = "60s";

  private static ArtifactClient artifactClient;

  @BeforeClass
  public static void setUpClass() throws IOException {
    Storage gcsClient = createGcsClient(PROPERTIES.googleCredentials());
    artifactClient = new ArtifactGcsSdkClient(gcsClient);
    artifactClient.uploadArtifact(
        PROPERTIES.artifactBucket(), SCHEMA_FILE_GCS_PATH, LOCAL_SCHEMA_PATH);
  }

  @AfterClass
  public static void tearDownClass() {
    artifactClient.deleteTestDir(PROPERTIES.artifactBucket(), TEST_DIR);
  }

  @Test
  public void testFakeMessagesToGcs() throws IOException {
    String name = "teleport-flex-streaming-data-generator-gcs";
    String outputDir = createTestPath(TEST_DIR, name);
    String jobName = createJobName(name);
    LaunchOptions options =
        LaunchOptions.builder(jobName, PROPERTIES.specPath())
            .addParameter(
                SCHEMA_LOCATION_KEY,
                String.format("gs://%s/%s", PROPERTIES.artifactBucket(), SCHEMA_FILE_GCS_PATH))
            .addParameter(QPS_KEY, DEFAULT_QPS)
            .addParameter(SINK_TYPE_KEY, SinkType.GCS.name())
            .addParameter(WINDOW_DURATION_KEY, DEFAULT_WINDOW_DURATION)
            .addParameter(
                OUTPUT_DIRECTORY_KEY,
                String.format("gs://%s/%s", PROPERTIES.artifactBucket(), outputDir))
            .addParameter(NUM_SHARDS_KEY, "1")
            .build();
    DataflowTemplateClient dataflow =
        FlexTemplateClient.builder().setCredentials(PROPERTIES.googleCredentials()).build();

    JobInfo info = dataflow.launchTemplate(PROPERTIES.project(), PROPERTIES.region(), options);
    assertThat(info.state()).isIn(JobState.RUNNING_STATES);

    Result result =
        new DataflowOperator(dataflow)
            .waitForConditionAndFinish(
                createConfig(info),
                () -> {
                  List<Blob> outputFiles =
                      artifactClient.listArtifacts(
                          PROPERTIES.artifactBucket(), outputDir, Pattern.compile(".*output-.*"));
                  return !outputFiles.isEmpty();
                });
    assertThat(result).isEqualTo(Result.CONDITION_MET);
  }

  private static DataflowOperator.Config createConfig(JobInfo info) {
    return DataflowOperator.Config.builder()
        .setJobId(info.jobId())
        .setProject(PROPERTIES.project())
        .setRegion(PROPERTIES.region())
        .build();
  }
}
