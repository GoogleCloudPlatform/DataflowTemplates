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
import static com.google.cloud.teleport.it.artifacts.ArtifactUtils.getFullGcsPath;
import static com.google.cloud.teleport.it.dataflow.DataflowUtils.createJobName;
import static com.google.common.truth.Truth.assertThat;

import com.google.auth.Credentials;
import com.google.cloud.storage.Storage;
import com.google.cloud.teleport.it.TestProperties;
import com.google.cloud.teleport.it.artifacts.Artifact;
import com.google.cloud.teleport.it.artifacts.ArtifactClient;
import com.google.cloud.teleport.it.artifacts.GcsArtifactClient;
import com.google.cloud.teleport.it.dataflow.DataflowOperator;
import com.google.cloud.teleport.it.dataflow.DataflowOperator.Result;
import com.google.cloud.teleport.it.dataflow.DataflowTemplateClient;
import com.google.cloud.teleport.it.dataflow.DataflowTemplateClient.JobInfo;
import com.google.cloud.teleport.it.dataflow.DataflowTemplateClient.JobState;
import com.google.cloud.teleport.it.dataflow.DataflowTemplateClient.LaunchConfig;
import com.google.cloud.teleport.it.dataflow.FlexTemplateClient;
import com.google.cloud.teleport.v2.templates.StreamingDataGenerator.SinkType;
import com.google.common.io.Resources;
import com.google.re2j.Pattern;
import java.io.IOException;
import java.util.List;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Integration test for {@link StreamingDataGenerator}. */
@RunWith(JUnit4.class)
public final class StreamingDataGeneratorIT {
  @Rule public final TestName testName = new TestName();

  private static final String ARTIFACT_BUCKET = TestProperties.artifactBucket();
  private static final Credentials CREDENTIALS = TestProperties.googleCredentials();
  private static final String PROJECT = TestProperties.project();
  private static final String REGION = TestProperties.region();
  private static final String SPEC_PATH = TestProperties.specPath();

  private static final String SCHEMA_FILE = "gameevent.json";
  private static final String LOCAL_SCHEMA_PATH = Resources.getResource(SCHEMA_FILE).getPath();

  private static final String TEST_ROOT_DIR = StreamingDataGeneratorIT.class.getSimpleName();

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
    Storage gcsClient = createGcsClient(CREDENTIALS);
    artifactClient = GcsArtifactClient.builder(gcsClient, ARTIFACT_BUCKET, TEST_ROOT_DIR).build();
    artifactClient.uploadArtifact(SCHEMA_FILE, LOCAL_SCHEMA_PATH);
  }

  @AfterClass
  public static void tearDownClass() {
    artifactClient.cleanupRun();
  }

  @Test
  public void testFakeMessagesToGcs() throws IOException {
    // Arrange
    String name = testName.getMethodName();
    String jobName = createJobName(name);

    LaunchConfig options =
        LaunchConfig.builder(jobName, SPEC_PATH)
            // TODO(zhoufek): See if it is possible to use the properties interface and generate
            // the map from the set values.
            .addParameter(SCHEMA_LOCATION_KEY, getGcsSchemaLocation(SCHEMA_FILE))
            .addParameter(QPS_KEY, DEFAULT_QPS)
            .addParameter(SINK_TYPE_KEY, SinkType.GCS.name())
            .addParameter(WINDOW_DURATION_KEY, DEFAULT_WINDOW_DURATION)
            .addParameter(OUTPUT_DIRECTORY_KEY, getTestMethodDirPath(name))
            .addParameter(NUM_SHARDS_KEY, "1")
            .build();
    DataflowTemplateClient dataflow =
        FlexTemplateClient.builder().setCredentials(CREDENTIALS).build();

    // Act
    JobInfo info = dataflow.launchTemplate(PROJECT, REGION, options);
    assertThat(info.state()).isIn(JobState.ACTIVE_STATES);

    Result result =
        new DataflowOperator(dataflow)
            .waitForConditionAndFinish(
                createConfig(info),
                () -> {
                  List<Artifact> outputFiles =
                      artifactClient.listArtifacts(name, Pattern.compile(".*output-.*"));
                  return !outputFiles.isEmpty();
                });

    // Assert
    assertThat(result).isEqualTo(Result.CONDITION_MET);
  }

  private static String getTestMethodDirPath(String testMethod) {
    return getFullGcsPath(ARTIFACT_BUCKET, TEST_ROOT_DIR, artifactClient.runId(), testMethod);
  }

  private static String getGcsSchemaLocation(String schemaFile) {
    return getFullGcsPath(ARTIFACT_BUCKET, TEST_ROOT_DIR, artifactClient.runId(), schemaFile);
  }

  private static DataflowOperator.Config createConfig(JobInfo info) {
    return DataflowOperator.Config.builder()
        .setJobId(info.jobId())
        .setProject(PROJECT)
        .setRegion(REGION)
        .build();
  }
}
