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

import static com.google.cloud.teleport.it.dataflow.DataflowUtils.createJobName;
import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.teleport.it.TemplateTestBase;
import com.google.cloud.teleport.it.artifacts.Artifact;
import com.google.cloud.teleport.it.dataflow.DataflowOperator;
import com.google.cloud.teleport.it.dataflow.DataflowOperator.Result;
import com.google.cloud.teleport.it.dataflow.DataflowTemplateClient.JobInfo;
import com.google.cloud.teleport.it.dataflow.DataflowTemplateClient.JobState;
import com.google.cloud.teleport.it.dataflow.DataflowTemplateClient.LaunchConfig;
import com.google.cloud.teleport.it.pubsub.DefaultPubsubResourceManager;
import com.google.cloud.teleport.it.pubsub.PubsubResourceManager;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
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
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Integration test for {@link PubsubToText} (Cloud_PubSub_to_GCS_Text_Flex). */
@TemplateIntegrationTest(PubsubToText.class)
@RunWith(JUnit4.class)
public final class PubsubToTextIT extends TemplateTestBase {

  private static final String INPUT_TOPIC = "inputTopic";
  private static final String INPUT_SUBSCRIPTION = "inputSubscription";
  private static final String NUM_SHARDS_KEY = "numShards";
  private static final String OUTPUT_DIRECTORY_KEY = "outputDirectory";
  private static final String WINDOW_DURATION_KEY = "windowDuration";
  private static final String OUTPUT_FILENAME_PREFIX = "outputFilenamePrefix";

  private static final String DEFAULT_WINDOW_DURATION = "10s";

  private static PubsubResourceManager pubsubResourceManager;

  @Before
  public void setup() throws IOException {
    pubsubResourceManager =
        DefaultPubsubResourceManager.builder(testName.getMethodName(), PROJECT)
            .credentialsProvider(credentialsProvider)
            .build();
  }

  @After
  public void tearDownClass() {
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
    LaunchConfig.Builder options =
        LaunchConfig.builder(jobName, specPath)
            .addParameter(INPUT_TOPIC, topic.toString())
            .addParameter(WINDOW_DURATION_KEY, DEFAULT_WINDOW_DURATION)
            .addParameter(OUTPUT_DIRECTORY_KEY, getGcsPath(name))
            .addParameter(NUM_SHARDS_KEY, "1")
            .addParameter(OUTPUT_FILENAME_PREFIX, "topic-output-");

    // Act
    JobInfo info = launchTemplate(options);
    assertThat(info.state()).isIn(JobState.ACTIVE_STATES);

    AtomicReference<List<Artifact>> artifacts = new AtomicReference<>();

    Result result =
        new DataflowOperator(getDataflowClient())
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

    LaunchConfig.Builder options =
        LaunchConfig.builder(jobName, specPath)
            .addParameter(INPUT_SUBSCRIPTION, subscription.toString())
            .addParameter(WINDOW_DURATION_KEY, DEFAULT_WINDOW_DURATION)
            .addParameter(OUTPUT_DIRECTORY_KEY, getGcsPath(name))
            .addParameter(NUM_SHARDS_KEY, "1")
            .addParameter(OUTPUT_FILENAME_PREFIX, "subscription-output-");

    // Act
    JobInfo info = launchTemplate(options);
    assertThat(info.state()).isIn(JobState.ACTIVE_STATES);

    AtomicReference<List<Artifact>> artifacts = new AtomicReference<>();

    Result result =
        new DataflowOperator(getDataflowClient())
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
}
