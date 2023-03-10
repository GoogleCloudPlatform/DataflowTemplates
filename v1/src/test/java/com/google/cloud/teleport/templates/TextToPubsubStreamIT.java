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

import static com.google.cloud.teleport.it.artifacts.ArtifactUtils.getFullGcsPath;
import static com.google.cloud.teleport.it.matchers.TemplateAsserts.assertThatPipeline;
import static com.google.cloud.teleport.it.matchers.TemplateAsserts.assertThatResult;
import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.teleport.it.TemplateTestBase;
import com.google.cloud.teleport.it.common.ResourceManagerUtils;
import com.google.cloud.teleport.it.launcher.PipelineLauncher.LaunchConfig;
import com.google.cloud.teleport.it.launcher.PipelineLauncher.LaunchInfo;
import com.google.cloud.teleport.it.launcher.PipelineOperator.Result;
import com.google.cloud.teleport.it.pubsub.DefaultPubsubResourceManager;
import com.google.cloud.teleport.it.pubsub.PubsubResourceManager;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.common.io.ByteStreams;
import com.google.pubsub.v1.PullResponse;
import com.google.pubsub.v1.SubscriptionName;
import com.google.pubsub.v1.TopicName;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.util.MimeTypes;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Integration test for {@link TextToPubsubStream} template. */
@Category(TemplateIntegrationTest.class)
@TemplateIntegrationTest(TextToPubsubStream.class)
@RunWith(JUnit4.class)
public class TextToPubsubStreamIT extends TemplateTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(TextToPubsubStreamIT.class);
  private static final String TEST_ROOT_DIR = TextToPubsubStreamIT.class.getSimpleName();

  private PubsubResourceManager pubsubResourceManager;

  @ClassRule public static TemporaryFolder tempFolder = new TemporaryFolder();

  @Before
  public void setUp() throws IOException {
    pubsubResourceManager =
        DefaultPubsubResourceManager.builder(testName, PROJECT)
            .credentialsProvider(credentialsProvider)
            .build();
  }

  @After
  public void tearDown() {
    ResourceManagerUtils.cleanResources(pubsubResourceManager);
  }

  @Test
  public void testTextToTopic() throws IOException {
    // Arrange
    TopicName outputTopic = pubsubResourceManager.createTopic("topic");
    SubscriptionName outputSubscription =
        pubsubResourceManager.createSubscription(outputTopic, "output-subscription");
    String messageString = String.format("msg-%s", testName);
    File file = tempFolder.newFile();
    writeToFile(file.getAbsolutePath(), messageString);
    LaunchConfig.Builder options =
        LaunchConfig.builder(testName, specPath)
            .addParameter("inputFilePattern", getInputFilePattern())
            .addParameter("outputTopic", outputTopic.toString());

    // Act
    LaunchInfo info = launchTemplate(options);
    assertThatPipeline(info).isRunning();
    AtomicReference<PullResponse> records = new AtomicReference<>();
    Result result =
        pipelineOperator()
            .waitForConditionAndFinish(
                createConfig(info),
                () -> {
                  try {
                    gcsClient.uploadArtifact(messageString, file.getAbsolutePath());
                  } catch (IOException e) {
                    LOG.error("Error encountered when trying to upload artifact.", e);
                  }
                  records.set(pubsubResourceManager.pull(outputSubscription, 5));
                  return records.get().getReceivedMessagesList().size() > 0;
                });
    assertThatResult(result).meetsConditions();

    List<String> actualMessages =
        records.get().getReceivedMessagesList().stream()
            .map(receivedMessage -> receivedMessage.getMessage().getData().toStringUtf8())
            .collect(Collectors.toList());
    assertThat(actualMessages).isEqualTo(Collections.nCopies(actualMessages.size(), messageString));
  }

  private String getInputFilePattern() {
    return getFullGcsPath(artifactBucketName, TEST_ROOT_DIR, gcsClient.runId(), "*");
  }

  /**
   * Helper to generate files for testing.
   *
   * @param filePath The path to the file to write.
   * @param fileContents The content to write.
   * @return The file written.
   * @throws IOException If an error occurs while creating or writing the file.
   */
  private static ResourceId writeToFile(String filePath, String fileContents) throws IOException {
    ResourceId resourceId = FileSystems.matchNewResource(filePath, false);
    // Write the file contents to the channel and close.
    try (ReadableByteChannel readChannel =
        Channels.newChannel(new ByteArrayInputStream(fileContents.getBytes()))) {
      try (WritableByteChannel writeChannel = FileSystems.create(resourceId, MimeTypes.TEXT)) {
        ByteStreams.copy(readChannel, writeChannel);
      }
    }
    return resourceId;
  }
}
