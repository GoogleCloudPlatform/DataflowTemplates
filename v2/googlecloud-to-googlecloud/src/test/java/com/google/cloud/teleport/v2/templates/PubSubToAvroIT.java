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
package com.google.cloud.teleport.v2.templates;

import static com.google.common.truth.Truth.assertThat;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.cloud.teleport.v2.avro.AvroPubsubMessageRecord;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.SubscriptionName;
import com.google.pubsub.v1.TopicName;
import java.io.IOException;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.reflect.ReflectDatumReader;
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

/** Integration test for {@link PubsubToAvro} PubSub to Avro. */
// SkipDirectRunnerTest: PubsubIO doesn't trigger panes on the DirectRunner.
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(PubsubToAvro.class)
@RunWith(JUnit4.class)
public class PubSubToAvroIT extends TemplateTestBase {
  private PubsubResourceManager pubsubResourceManager;

  @Before
  public void setUp() throws IOException {
    pubsubResourceManager =
        PubsubResourceManager.builder(testName, PROJECT, credentialsProvider).build();
  }

  @After
  public void tearDown() {
    ResourceManagerUtils.cleanResources(pubsubResourceManager);
  }

  @Test
  public void testTopicToAvro() throws IOException {
    // Arrange
    String name = testName;
    Pattern expectedFilePattern = Pattern.compile(".*topic-output-.*");
    TopicName topic = pubsubResourceManager.createTopic("input-topic");

    // Act
    LaunchInfo info =
        launchTemplate(
            LaunchConfig.builder(testName, specPath)
                .addParameter("inputTopic", topic.toString())
                .addParameter("outputDirectory", getGcsPath(testName))
                .addParameter("windowDuration", "10s")
                .addParameter("avroTempDirectory", getGcsPath("avro_tmp"))
                .addParameter("outputFilenamePrefix", "topic-output-")
                .addParameter("numShards", "2"));

    assertThatPipeline(info).isRunning();

    ImmutableSet<String> messages =
        ImmutableSet.of("message1-" + name, "message2-" + name, "message3-" + name);

    AtomicReference<List<Artifact>> artifacts = new AtomicReference<>();
    Result result =
        pipelineOperator()
            .waitForConditionAndFinish(
                createConfig(info),
                () -> {

                  // For tests that run against topics, sending repeatedly will make it work for
                  // cases in which the on-demand subscription is created after sending messages.
                  for (String message : messages) {
                    pubsubResourceManager.publish(
                        topic, ImmutableMap.of(), ByteString.copyFromUtf8(message));
                  }

                  artifacts.set(gcsClient.listArtifacts(testName, expectedFilePattern));
                  return !artifacts.get().isEmpty();
                });

    // Assert
    assertThatResult(result).meetsConditions();
    assertThat(
            artifacts.get().stream()
                .flatMap(a -> deserialize(a.contents()))
                .map(r -> new String(r.getMessage()))
                .collect(Collectors.toUnmodifiableSet()))
        .isEqualTo(messages);
  }

  @Test
  public void testSubscriptionToAvro() throws IOException {
    // Arrange
    String name = testName;
    Pattern expectedFilePattern = Pattern.compile(".*subscription-output-.*");
    TopicName topic = pubsubResourceManager.createTopic("input-topic");
    SubscriptionName subscriptionName =
        pubsubResourceManager.createSubscription(topic, "input-subscription");

    ImmutableSet<String> messages =
        ImmutableSet.of("message1-" + name, "message2-" + name, "message3-" + name);
    for (String message : messages) {
      pubsubResourceManager.publish(topic, ImmutableMap.of(), ByteString.copyFromUtf8(message));
    }

    // Act
    LaunchInfo info =
        launchTemplate(
            LaunchConfig.builder(testName, specPath)
                .addParameter("inputSubscription", subscriptionName.toString())
                .addParameter("outputDirectory", getGcsPath(testName))
                .addParameter("windowDuration", "10s")
                .addParameter("avroTempDirectory", getGcsPath("avro_tmp"))
                .addParameter("outputFilenamePrefix", "subscription-output-"));
    assertThatPipeline(info).isRunning();

    AtomicReference<List<Artifact>> artifacts = new AtomicReference<>();
    Result result =
        pipelineOperator()
            .waitForConditionAndFinish(
                createConfig(info),
                () -> {
                  artifacts.set(gcsClient.listArtifacts(testName, expectedFilePattern));
                  return !artifacts.get().isEmpty();
                });

    // Assert
    assertThatResult(result).meetsConditions();
    assertThat(
            artifacts.get().stream()
                .flatMap(a -> deserialize(a.contents()))
                .map(r -> new String(r.getMessage()))
                .collect(Collectors.toUnmodifiableSet()))
        .isEqualTo(messages);
  }

  private Stream<AvroPubsubMessageRecord> deserialize(byte[] bytes) {
    DataFileReader<AvroPubsubMessageRecord> dataFileReader;
    try {
      dataFileReader =
          new DataFileReader<>(
              new SeekableByteArrayInput(bytes),
              new ReflectDatumReader<>(AvroPubsubMessageRecord.class));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    return StreamSupport.stream(
        Spliterators.spliteratorUnknownSize(dataFileReader, Spliterator.IMMUTABLE), false);
  }
}
