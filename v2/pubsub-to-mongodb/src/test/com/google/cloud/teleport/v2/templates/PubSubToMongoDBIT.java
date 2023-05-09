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

import static com.google.cloud.teleport.it.common.matchers.TemplateAsserts.assertThatPipeline;
import static com.google.cloud.teleport.it.common.matchers.TemplateAsserts.assertThatResult;
import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.teleport.it.common.PipelineLauncher.LaunchConfig;
import com.google.cloud.teleport.it.common.PipelineLauncher.LaunchInfo;
import com.google.cloud.teleport.it.common.PipelineOperator.Result;
import com.google.cloud.teleport.it.common.utils.ResourceManagerUtils;
import com.google.cloud.teleport.it.gcp.TemplateTestBase;
import com.google.cloud.teleport.it.gcp.bigquery.DefaultBigQueryResourceManager;
import com.google.cloud.teleport.it.gcp.pubsub.DefaultPubsubResourceManager;
import com.google.cloud.teleport.it.mongodb.DefaultMongoDBResourceManager;
import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.SubscriptionName;
import com.google.pubsub.v1.TopicName;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Integration test for {@link PubSubToMongoDB}. */
@TemplateIntegrationTest(PubSubToMongoDB.class)
// SkipDirectRunnerTest: PubsubIO doesn't trigger panes on the DirectRunner.
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@RunWith(JUnit4.class)
public final class PubSubToMongoDBIT extends TemplateTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(PubSubToMongoDBIT.class);

  private DefaultMongoDBResourceManager mongoResourceManager;

  private DefaultPubsubResourceManager pubsubResourceManager;

  private BigQueryResourceManager bigQueryClient;

  @Before
  public void setup() throws IOException {

    pubsubResourceManager =
        DefaultPubsubResourceManager.builder(testName, PROJECT)
            .credentialsProvider(credentialsProvider)
            .build();

    mongoResourceManager = DefaultMongoDBResourceManager.builder(testName).setHost(HOST_IP).build();

    bigQueryClient =
        DefaultBigQueryResourceManager.builder(testName, PROJECT)
            .setCredentials(credentials)
            .build();
  }

  @After
  public void tearDown() {
    ResourceManagerUtils.cleanResources(
        pubsubResourceManager, mongoResourceManager, bigQueryClient);
  }

  @Test
  public void testPubsubToMongoDB() throws IOException, ExecutionException, InterruptedException {
    // Arrange
    TopicName tc = pubsubResourceManager.createTopic(testName);
    SubscriptionName subscription = pubsubResourceManager.createSubscription(tc, "sub-" + testName);
    bigQueryClient.createDataset(REGION);

    mongoResourceManager.createCollection(testName);

    String outDeadLetterTopicName =
        pubsubResourceManager.createTopic("outDead" + testName).getTopic();

    LaunchConfig.Builder options =
        LaunchConfig.builder(testName, specPath)
            .addParameter("mongoDBUri", mongoResourceManager.getUri())
            .addParameter("database", mongoResourceManager.getDatabaseName())
            .addParameter("collection", testName)
            .addParameter("inputSubscription", subscription.toString())
            .addParameter(
                "deadletterTable", PROJECT + ":" + bigQueryClient.getDatasetId() + ".dlq");

    // Act
    LaunchInfo info = launchTemplate(options);
    LOG.info("Triggered template job");

    assertThatPipeline(info).isRunning();

    List<String> inMessages =
        Arrays.asList("{\"id\": 1, \"name\": \"Fox\"}", "{\"id\": 2, \"name\": \"Dog\"}");

    Result result =
        pipelineOperator()
            .waitForConditionAndFinish(
                createConfig(info),
                () -> {

                  // For tests that run against topics, sending repeatedly will make it work for
                  // cases in which the on-demand subscription is created after sending messages.
                  for (final String message : inMessages) {
                    ByteString data = ByteString.copyFromUtf8(message);
                    pubsubResourceManager.publish(tc, ImmutableMap.of(), data);
                  }

                  return mongoResourceManager.readCollection(testName).spliterator().estimateSize()
                      >= inMessages.size();
                });

    // Assert
    assertThatResult(result).meetsConditions();
  }
}
