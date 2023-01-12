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
package com.google.cloud.teleport.templates;

import static com.google.cloud.teleport.it.dataflow.DataflowUtils.createJobName;
import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.teleport.avro.AvroPubsubMessageRecord;
import com.google.cloud.teleport.it.TemplateTestBase;
import com.google.cloud.teleport.it.artifacts.Artifact;
import com.google.cloud.teleport.it.dataflow.DataflowClient.JobInfo;
import com.google.cloud.teleport.it.dataflow.DataflowClient.JobState;
import com.google.cloud.teleport.it.dataflow.DataflowClient.LaunchConfig;
import com.google.cloud.teleport.it.dataflow.DataflowOperator;
import com.google.cloud.teleport.it.dataflow.DataflowOperator.Result;
import com.google.cloud.teleport.it.pubsub.DefaultPubsubResourceManager;
import com.google.cloud.teleport.it.pubsub.PubsubResourceManager;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.TopicName;
import com.google.re2j.Pattern;
import java.io.IOException;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.reflect.ReflectDatumReader;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Integration test for {@link PubsubToAvro} PubSub Subscription to Bigquery. */
@Category(TemplateIntegrationTest.class)
@TemplateIntegrationTest(value = PubsubToAvro.class, template = "Cloud_PubSub_to_Avro")
@RunWith(JUnit4.class)
public class PubSubToAvroIT extends TemplateTestBase {
  private static PubsubResourceManager pubsubResourceManager;

  @Before
  public void setUp() throws IOException {
    pubsubResourceManager =
        DefaultPubsubResourceManager.builder(testName.getMethodName(), PROJECT)
            .credentialsProvider(credentialsProvider)
            .build();
  }

  @After
  public void tearDown() {
    pubsubResourceManager.cleanupAll();
  }

  @Test
  public void testTopicToAvro() throws IOException {
    // Arrange
    String name = testName.getMethodName();
    String jobName = createJobName(name);
    Pattern expectedFilePattern = Pattern.compile(".*topic-output-.*");
    TopicName topic = pubsubResourceManager.createTopic("input-topic");

    // Act
    JobInfo info =
        launchTemplate(
            LaunchConfig.builder(jobName, specPath)
                .addParameter("inputTopic", topic.toString())
                .addParameter("outputDirectory", getGcsPath(name))
                .addParameter("avroTempDirectory", getGcsPath("avro_tmp"))
                .addParameter("outputFilenamePrefix", "topic-output-"));
    assertThat(info.state()).isIn(JobState.ACTIVE_STATES);

    ImmutableSet<String> messages =
        ImmutableSet.of("message1-" + jobName, "message2-" + jobName, "message3-" + jobName);
    messages.forEach(
        m -> pubsubResourceManager.publish(topic, ImmutableMap.of(), ByteString.copyFromUtf8(m)));

    AtomicReference<List<Artifact>> artifacts = new AtomicReference<>();
    Result result =
        new DataflowOperator(getDataflowClient())
            .waitForConditionAndFinish(
                createConfig(info),
                () -> {
                  artifacts.set(artifactClient.listArtifacts(name, expectedFilePattern));
                  return !artifacts.get().isEmpty();
                });

    // Assert
    assertThat(result).isEqualTo(Result.CONDITION_MET);
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
