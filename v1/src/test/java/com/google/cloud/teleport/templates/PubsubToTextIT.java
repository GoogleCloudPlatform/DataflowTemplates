/*
 * Copyright (C) 2024 Google LLC
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

import static com.google.common.truth.Truth.assertThat;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.TopicName;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.beam.it.common.PipelineLauncher.LaunchConfig;
import org.apache.beam.it.common.PipelineLauncher.LaunchInfo;
import org.apache.beam.it.common.PipelineOperator.Result;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.TemplateTestBase;
import org.apache.beam.it.gcp.artifacts.Artifact;
import org.apache.beam.it.gcp.pubsub.PubsubResourceManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Integration test for {@link PubsubToText} (Cloud_PubSub_to_GCS_Text). */
// SkipDirectRunnerTest: PubsubIO doesn't trigger panes on the DirectRunner.
@Category(TemplateIntegrationTest.class)
@TemplateIntegrationTest(PubsubToText.class)
@RunWith(JUnit4.class)
public final class PubsubToTextIT extends TemplateTestBase {

  private static final String INPUT_TOPIC = "inputTopic";
  private static final String OUTPUT_DIRECTORY_KEY = "outputDirectory";
  private static final String OUTPUT_FILENAME_PREFIX = "outputFilenamePrefix";

  private PubsubResourceManager pubsubResourceManager;

  @Before
  public void setup() throws IOException {
    pubsubResourceManager =
        PubsubResourceManager.builder(testName, PROJECT, credentialsProvider).build();
  }

  @After
  public void tearDown() {
    ResourceManagerUtils.cleanResources(pubsubResourceManager);
  }

  @Test
  public void testTopicToGcs() throws IOException {
    testTopicToGcsBase(Function.identity());
  }

  @Test
  public void testTopicToGcsUsingAtleastOnceMode() throws IOException {
    ArrayList<String> experiments = new ArrayList<>();
    experiments.add("streaming_mode_at_least_once");
    testTopicToGcsBase(
        b ->
            b.addEnvironment("additionalExperiments", experiments)
                .addEnvironment("enableStreamingEngine", true));
  }

  private void testTopicToGcsBase(Function<LaunchConfig.Builder, LaunchConfig.Builder> paramsAdder)
      throws IOException {
    // Arrange
    String messageString = String.format("msg-%s", testName);
    Pattern expectedFilePattern = Pattern.compile(".*topic-output-.*");

    TopicName topic = pubsubResourceManager.createTopic("input");
    LaunchConfig.Builder options =
        paramsAdder.apply(
            LaunchConfig.builder(testName, specPath)
                .addParameter(INPUT_TOPIC, topic.toString())
                .addParameter(OUTPUT_DIRECTORY_KEY, getGcsPath(testName))
                .addParameter(OUTPUT_FILENAME_PREFIX, "topic-output-"));
    // LaunchConfig.Builder options =
    //     LaunchConfig.builder(testName, specPath)
    //         .addParameter(INPUT_TOPIC, topic.toString())
    //         .addParameter(OUTPUT_DIRECTORY_KEY, getGcsPath(testName))
    //         .addParameter(OUTPUT_FILENAME_PREFIX, "topic-output-");

    // Act
    LaunchInfo info = launchTemplate(options);
    assertThatPipeline(info).isRunning();

    ByteString messageData = ByteString.copyFromUtf8(messageString);
    pubsubResourceManager.publish(topic, ImmutableMap.of(), messageData);

    AtomicReference<List<Artifact>> artifacts = new AtomicReference<>();

    Result result =
        pipelineOperator()
            .waitForConditionAndFinish(
                createConfig(info),
                () -> {

                  // For tests that run against topics, sending repeatedly will make it work for
                  // cases in which the on-demand subscription is created after sending messages.
                  pubsubResourceManager.publish(topic, ImmutableMap.of(), messageData);

                  artifacts.set(gcsClient.listArtifacts(testName, expectedFilePattern));
                  return !artifacts.get().isEmpty();
                });

    // Assert
    assertThatResult(result).meetsConditions();

    // Make sure that files contain only the messages produced by this test
    String allMessages =
        artifacts.get().stream()
            .map(artifact -> new String(artifact.contents()))
            .collect(Collectors.joining());
    assertThat(allMessages.replace(messageString, "").trim()).isEmpty();
  }
}
