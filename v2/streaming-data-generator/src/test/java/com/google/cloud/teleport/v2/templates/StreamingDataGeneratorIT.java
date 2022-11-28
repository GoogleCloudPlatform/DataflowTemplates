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

import static com.google.cloud.teleport.it.dataflow.DataflowUtils.createJobName;
import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.teleport.it.TemplateTestBase;
import com.google.cloud.teleport.it.artifacts.Artifact;
import com.google.cloud.teleport.it.dataflow.DataflowOperator;
import com.google.cloud.teleport.it.dataflow.DataflowOperator.Result;
import com.google.cloud.teleport.it.dataflow.DataflowTemplateClient;
import com.google.cloud.teleport.it.dataflow.DataflowTemplateClient.JobInfo;
import com.google.cloud.teleport.it.dataflow.DataflowTemplateClient.JobState;
import com.google.cloud.teleport.it.dataflow.DataflowTemplateClient.LaunchConfig;
import com.google.cloud.teleport.it.dataflow.FlexTemplateClient;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.cloud.teleport.v2.templates.StreamingDataGenerator.SinkType;
import com.google.common.io.Resources;
import com.google.re2j.Pattern;
import java.io.IOException;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Integration test for {@link StreamingDataGenerator}. */
@TemplateIntegrationTest(StreamingDataGenerator.class)
@RunWith(JUnit4.class)
public final class StreamingDataGeneratorIT extends TemplateTestBase {

  private static final String SCHEMA_FILE = "gameevent.json";
  private static final String LOCAL_SCHEMA_PATH = Resources.getResource(SCHEMA_FILE).getPath();

  private static final String NUM_SHARDS_KEY = "numShards";
  private static final String OUTPUT_DIRECTORY_KEY = "outputDirectory";
  private static final String QPS_KEY = "qps";
  private static final String SCHEMA_LOCATION_KEY = "schemaLocation";
  private static final String SINK_TYPE_KEY = "sinkType";
  private static final String WINDOW_DURATION_KEY = "windowDuration";

  private static final String DEFAULT_QPS = "15";
  private static final String DEFAULT_WINDOW_DURATION = "60s";

  @Before
  public void setUp() throws IOException {
    artifactClient.uploadArtifact(SCHEMA_FILE, LOCAL_SCHEMA_PATH);
  }

  @Test
  public void testFakeMessagesToGcs() throws IOException {
    // Arrange
    String name = testName.getMethodName();
    String jobName = createJobName(name);

    LaunchConfig options =
        LaunchConfig.builder(jobName, specPath)
            // TODO(zhoufek): See if it is possible to use the properties interface and generate
            // the map from the set values.
            .addParameter(SCHEMA_LOCATION_KEY, getGcsPath(SCHEMA_FILE))
            .addParameter(QPS_KEY, DEFAULT_QPS)
            .addParameter(SINK_TYPE_KEY, SinkType.GCS.name())
            .addParameter(WINDOW_DURATION_KEY, DEFAULT_WINDOW_DURATION)
            .addParameter(OUTPUT_DIRECTORY_KEY, getGcsPath(name))
            .addParameter(NUM_SHARDS_KEY, "1")
            .build();
    DataflowTemplateClient dataflow =
        FlexTemplateClient.builder().setCredentials(credentials).build();

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
}
