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
import static com.google.cloud.teleport.it.gcp.artifacts.utils.ArtifactUtils.getFullGcsPath;
import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.teleport.it.common.PipelineLauncher.LaunchConfig;
import com.google.cloud.teleport.it.common.PipelineLauncher.LaunchInfo;
import com.google.cloud.teleport.it.common.PipelineOperator.Result;
import com.google.cloud.teleport.it.common.utils.ResourceManagerUtils;
import com.google.cloud.teleport.it.gcp.TemplateTestBase;
import com.google.cloud.teleport.it.gcp.pubsub.DefaultPubsubResourceManager;
import com.google.cloud.teleport.it.gcp.pubsub.conditions.PubsubMessagesCheck;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.pubsub.v1.SubscriptionName;
import com.google.pubsub.v1.TopicName;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Integration test for {@link TextToPubsub} template. */
@Category(TemplateIntegrationTest.class)
@TemplateIntegrationTest(TextToPubsub.class)
@RunWith(JUnit4.class)
public class TextToPubsubIT extends TemplateTestBase {
  private static final String TEST_ROOT_DIR = TextToPubsubIT.class.getSimpleName();
  private DefaultPubsubResourceManager pubsubResourceManager;

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
    List<String> expectedArtifacts =
        List.of("message1-" + testName, "message2-" + testName, "message3-" + testName);
    createArtifacts(expectedArtifacts);
    LaunchConfig.Builder options =
        LaunchConfig.builder(testName, specPath)
            .addParameter("inputFilePattern", getInputFilePattern())
            .addParameter("outputTopic", outputTopic.toString());

    // Act
    LaunchInfo info = launchTemplate(options);
    assertThatPipeline(info).isRunning();

    PubsubMessagesCheck pubsubCheck =
        PubsubMessagesCheck.builder(pubsubResourceManager, outputSubscription)
            .setMinMessages(expectedArtifacts.size())
            .build();
    Result result = pipelineOperator().waitForCondition(createConfig(info), pubsubCheck);

    assertThatResult(result).meetsConditions();
    assertThat(
            pubsubCheck.getReceivedMessageList().stream()
                .map(receivedMessage -> receivedMessage.getMessage().getData().toStringUtf8())
                .collect(Collectors.toList()))
        .containsAtLeastElementsIn(expectedArtifacts);
  }

  private void createArtifacts(List<String> expectedArtifacts) {
    for (String artifact : expectedArtifacts) {
      gcsClient.createArtifact(artifact, artifact.getBytes());
    }
  }

  private String getInputFilePattern() {
    return getFullGcsPath(artifactBucketName, TEST_ROOT_DIR, gcsClient.runId(), "*");
  }
}
