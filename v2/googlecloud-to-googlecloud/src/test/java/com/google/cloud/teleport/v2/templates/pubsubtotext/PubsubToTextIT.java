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
package com.google.cloud.teleport.v2.templates.pubsubtotext;

import static com.google.cloud.teleport.it.artifacts.ArtifactUtils.createGcsClient;
import static com.google.cloud.teleport.it.artifacts.ArtifactUtils.getFullGcsPath;
import static com.google.cloud.teleport.it.dataflow.DataflowUtils.createJobName;
import static com.google.common.truth.Truth.assertThat;

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.FixedCredentialsProvider;
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
import com.google.cloud.teleport.it.pubsub.DefaultPubsubResourceManager;
import com.google.cloud.teleport.it.pubsub.PubsubResourceManager;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.SubscriptionName;
import com.google.pubsub.v1.TopicName;
import com.google.re2j.Pattern;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Integration test for {@link PubsubToText} (Cloud_PubSub_to_GCS_Text_Flex). */
@RunWith(JUnit4.class)
public final class PubsubToTextIT {

  @Rule public final TestName testName = new TestName();

  private static final String ARTIFACT_BUCKET = TestProperties.artifactBucket();
  private static final Credentials CREDENTIALS = TestProperties.googleCredentials();
  private static final CredentialsProvider CREDENTIALS_PROVIDER =
      FixedCredentialsProvider.create(CREDENTIALS);
  private static final String PROJECT = TestProperties.project();
  private static final String REGION = TestProperties.region();
  private static final String SPEC_PATH = TestProperties.specPath();

  private static final String TEST_ROOT_DIR = PubsubToTextIT.class.getSimpleName();

  private static final String INPUT_TOPIC = "inputTopic";
  private static final String INPUT_SUBSCRIPTION = "inputSubscription";
  private static final String NUM_SHARDS_KEY = "numShards";
  private static final String OUTPUT_DIRECTORY_KEY = "outputDirectory";
  private static final String WINDOW_DURATION_KEY = "windowDuration";
  private static final String OUTPUT_FILENAME_PREFIX = "outputFilenamePrefix";

  private static final String DEFAULT_WINDOW_DURATION = "10s";

  private static PubsubResourceManager pubsubResourceManager;
  private static ArtifactClient artifactClient;

  @Before
  public void setup() throws IOException {
    Storage gcsClient = createGcsClient(CREDENTIALS);

    pubsubResourceManager =
        DefaultPubsubResourceManager.builder(testName.getMethodName(), PROJECT)
            .credentialsProvider(CREDENTIALS_PROVIDER)
            .build();

    artifactClient = GcsArtifactClient.builder(gcsClient, ARTIFACT_BUCKET, TEST_ROOT_DIR).build();
  }

  @After
  public void tearDownClass() {
    artifactClient.cleanupRun();
    pubsubResourceManager.cleanupAll();
  }

  @Test
  public void testTopicToGcs() throws IOException {
    // Arrange
    String name = testName.getMethodName();
    String jobName = createJobName(name);
    String messageString = String.format("msg-%s", jobName);
    Pattern expectedFilePattern = Pattern.compile(".*topic-output-.*");

    TopicName topic = pubsubResourceManager.createTopic("input");
    LaunchConfig options =
        LaunchConfig.builder(jobName, SPEC_PATH)
            .addParameter(INPUT_TOPIC, topic.toString())
            .addParameter(WINDOW_DURATION_KEY, DEFAULT_WINDOW_DURATION)
            .addParameter(OUTPUT_DIRECTORY_KEY, getTestMethodDirPath(name))
            .addParameter(NUM_SHARDS_KEY, "1")
            .addParameter(OUTPUT_FILENAME_PREFIX, "topic-output-")
            .build();
    DataflowTemplateClient dataflow =
        FlexTemplateClient.builder().setCredentials(CREDENTIALS).build();

    // Act
    JobInfo info = dataflow.launchTemplate(PROJECT, REGION, options);
    assertThat(info.state()).isIn(JobState.ACTIVE_STATES);

    AtomicReference<List<Artifact>> artifacts = new AtomicReference<>();

    Result result =
        new DataflowOperator(dataflow)
            .waitForConditionAndFinish(
                createConfig(info),
                () -> {
                  ByteString messageData = ByteString.copyFromUtf8(messageString);
                  pubsubResourceManager.publish(topic, ImmutableMap.of(), messageData);

                  artifacts.set(artifactClient.listArtifacts(name, expectedFilePattern));
                  return !artifacts.get().isEmpty();
                });

    // Assert
    assertThat(result).isEqualTo(Result.CONDITION_MET);

    // Make sure that files contain only the messages produced by this test
    String allMessages =
        artifacts.get().stream()
            .map(artifact -> new String(artifact.contents()))
            .collect(Collectors.joining());
    assertThat(allMessages.replace(messageString, "").trim()).isEmpty();
  }

  @Test
  public void testSubscriptionToGcs() throws IOException {
    // Arrange
    String name = testName.getMethodName();
    String jobName = createJobName(name);
    String messageString = String.format("msg-%s", jobName);
    Pattern expectedFilePattern = Pattern.compile(".*subscription-output-.*");

    TopicName topic = pubsubResourceManager.createTopic("input");

    SubscriptionName subscription = pubsubResourceManager.createSubscription(topic, jobName + "-1");

    LaunchConfig options =
        LaunchConfig.builder(jobName, SPEC_PATH)
            .addParameter(INPUT_SUBSCRIPTION, subscription.toString())
            .addParameter(WINDOW_DURATION_KEY, DEFAULT_WINDOW_DURATION)
            .addParameter(OUTPUT_DIRECTORY_KEY, getTestMethodDirPath(name))
            .addParameter(NUM_SHARDS_KEY, "1")
            .addParameter(OUTPUT_FILENAME_PREFIX, "subscription-output-")
            .build();
    DataflowTemplateClient dataflow =
        FlexTemplateClient.builder().setCredentials(CREDENTIALS).build();

    // Act
    JobInfo info = dataflow.launchTemplate(PROJECT, REGION, options);
    assertThat(info.state()).isIn(JobState.ACTIVE_STATES);

    AtomicReference<List<Artifact>> artifacts = new AtomicReference<>();

    Result result =
        new DataflowOperator(dataflow)
            .waitForConditionAndFinish(
                createConfig(info),
                () -> {
                  ByteString messageData = ByteString.copyFromUtf8(messageString);
                  pubsubResourceManager.publish(topic, ImmutableMap.of(), messageData);

                  artifacts.set(artifactClient.listArtifacts(name, expectedFilePattern));
                  return !artifacts.get().isEmpty();
                });

    // Assert
    assertThat(result).isEqualTo(Result.CONDITION_MET);

    // Make sure that files contain only the messages produced by this test
    String allMessages =
        artifacts.get().stream()
            .map(artifact -> new String(artifact.contents()))
            .collect(Collectors.joining());
    assertThat(allMessages.replace(messageString, "").trim()).isEmpty();
  }

  private static String getTestMethodDirPath(String testMethod) {
    return getFullGcsPath(ARTIFACT_BUCKET, TEST_ROOT_DIR, artifactClient.runId(), testMethod);
  }

  private static DataflowOperator.Config createConfig(JobInfo info) {
    return DataflowOperator.Config.builder()
        .setJobId(info.jobId())
        .setProject(PROJECT)
        .setRegion(REGION)
        .build();
  }
}
