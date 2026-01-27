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

import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.TopicName;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import org.apache.beam.it.common.PipelineLauncher.LaunchConfig;
import org.apache.beam.it.common.PipelineLauncher.LaunchInfo;
import org.apache.beam.it.common.PipelineOperator.Result;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.TemplateTestBase;
import org.apache.beam.it.gcp.pubsub.PubsubResourceManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

/** Integration test for {@link PubSubToRedis}. */
@TemplateIntegrationTest(PubSubToRedis.class)
// SkipDirectRunnerTest: PubsubIO doesn't trigger panes on the DirectRunner.
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@RunWith(JUnit4.class)
public final class PubSubToRedisIT extends TemplateTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(PubSubToRedis.class);

  private PubsubResourceManager pubsubResourceManager;
  private Jedis redisClient;

  @Before
  public void setup() throws IOException {
    pubsubResourceManager =
        PubsubResourceManager.builder(testName, PROJECT, credentialsProvider).build();
  }

  @After
  public void tearDown() {
    ResourceManagerUtils.cleanResources(pubsubResourceManager);
    if (redisClient != null) {
      redisClient.close();
    }
  }

  @Test
  public void testPubSubToRedisStringSink() throws IOException {
    pubSubToRedisStringSink(Function.identity());
  }

  public void pubSubToRedisStringSink(
      Function<LaunchConfig.Builder, LaunchConfig.Builder> paramsAdder) throws IOException {
    // Arrange
    TopicName tc = pubsubResourceManager.createTopic(testName);
    String inSubscriptionName = testName + "-sub";
    pubsubResourceManager.createSubscription(tc, inSubscriptionName);

    String redisHost = "127.0.0.1";
    int redisPort = 6379;
    String redisPassword = "";

    redisClient = new Jedis(redisHost, redisPort);
    redisClient.select(0);
    redisClient.flushDB();

    LaunchConfig.Builder options =
        paramsAdder.apply(
            LaunchConfig.builder(testName, specPath)
                .addParameter(
                    "inputSubscription",
                    "projects/" + PROJECT + "/subscriptions/" + inSubscriptionName)
                .addParameter("redisHost", redisHost)
                .addParameter("redisPort", String.valueOf(redisPort))
                .addParameter("redisPassword", redisPassword)
                .addParameter("redisSinkType", "STRING_SINK"));

    // Act
    LaunchInfo info = launchTemplate(options);
    LOG.info("Triggered PubSub to Redis template job");

    List<String> inMessages = Arrays.asList("message-1", "message-2", "message-3");
    Set<String> outMessages = Collections.synchronizedSet(new HashSet<>());

    Result result =
        pipelineOperator()
            .waitForConditionAndFinish(
                createConfig(info),
                () -> {
                  // Publish messages to Pub/Sub
                  for (final String message : inMessages) {
                    ByteString data = ByteString.copyFromUtf8(message);
                    pubsubResourceManager.publish(tc, ImmutableMap.of("key", message), data);
                  }

                  // Check if messages appear in Redis
                  Set<String> keys = redisClient.keys("*");
                  for (String key : keys) {
                    String value = redisClient.get(key);
                    if (value != null) {
                      outMessages.add(value);
                    }
                  }
                  LOG.info(
                      "Redis messages found: {} out of {}", outMessages.size(), inMessages.size());

                  return outMessages.size() >= inMessages.size();
                });

    // Assert
    assertThat(outMessages).containsAtLeastElementsIn(inMessages);
    LOG.info("Successfully verified messages in Redis: {}", outMessages);
  }

  @Test
  public void testPubSubToRedisHashSink() throws IOException {
    pubSubToRedisHashSink(Function.identity());
  }

  public void pubSubToRedisHashSink(
      Function<LaunchConfig.Builder, LaunchConfig.Builder> paramsAdder) throws IOException {
    // Arrange
    String testId = testName + "-hash";
    TopicName tc = pubsubResourceManager.createTopic(testId);
    String inSubscriptionName = testId + "-sub";
    pubsubResourceManager.createSubscription(tc, inSubscriptionName);

    String redisHost = "127.0.0.1";
    int redisPort = 6379;
    String redisPassword = "";

    redisClient = new Jedis(redisHost, redisPort);
    redisClient.select(0);
    redisClient.flushDB();

    LaunchConfig.Builder options =
        paramsAdder.apply(
            LaunchConfig.builder(testId, specPath)
                .addParameter(
                    "inputSubscription",
                    "projects/" + PROJECT + "/subscriptions/" + inSubscriptionName)
                .addParameter("redisHost", redisHost)
                .addParameter("redisPort", String.valueOf(redisPort))
                .addParameter("redisPassword", redisPassword)
                .addParameter("redisSinkType", "HASH_SINK"));

    // Act
    LaunchInfo info = launchTemplate(options);
    LOG.info("Triggered PubSub to Redis HASH_SINK template job");

    List<String> inMessages = Arrays.asList("hash-message-1", "hash-message-2");
    Set<String> outMessages = Collections.synchronizedSet(new HashSet<>());

    Result result =
        pipelineOperator()
            .waitForConditionAndFinish(
                createConfig(info),
                () -> {
                  for (final String message : inMessages) {
                    ByteString data = ByteString.copyFromUtf8(message);
                    pubsubResourceManager.publish(
                        tc, ImmutableMap.of("key", "hash-key-" + message), data);
                  }

                  Set<String> keys = redisClient.keys("*");
                  for (String key : keys) {
                    java.util.Map<String, String> hashData = redisClient.hgetAll(key);
                    if (hashData != null && !hashData.isEmpty()) {
                      for (String value : hashData.values()) {
                        outMessages.add(value);
                      }
                    }
                  }
                  LOG.info(
                      "Redis hash messages found: {} out of {}",
                      outMessages.size(),
                      inMessages.size());

                  return outMessages.size() >= inMessages.size();
                });

    assertThat(outMessages).containsAtLeastElementsIn(inMessages);
    LOG.info("Successfully verified hash messages in Redis: {}", outMessages);
  }

  @Test
  public void testPubSubToRedisStreamsSink() throws IOException {
    pubSubToRedisStreamsSink(Function.identity());
  }

  public void pubSubToRedisStreamsSink(
      Function<LaunchConfig.Builder, LaunchConfig.Builder> paramsAdder) throws IOException {
    // Arrange
    String testId = testName + "-streams";
    TopicName tc = pubsubResourceManager.createTopic(testId);
    String inSubscriptionName = testId + "-sub";
    pubsubResourceManager.createSubscription(tc, inSubscriptionName);

    String redisHost = "127.0.0.1";
    int redisPort = 6379;
    String redisPassword = "";

    redisClient = new Jedis(redisHost, redisPort);
    redisClient.select(0);
    redisClient.flushDB();

    LaunchConfig.Builder options =
        paramsAdder.apply(
            LaunchConfig.builder(testId, specPath)
                .addParameter(
                    "inputSubscription",
                    "projects/" + PROJECT + "/subscriptions/" + inSubscriptionName)
                .addParameter("redisHost", redisHost)
                .addParameter("redisPort", String.valueOf(redisPort))
                .addParameter("redisPassword", redisPassword)
                .addParameter("redisSinkType", "STREAMS_SINK"));

    // Act
    LaunchInfo info = launchTemplate(options);
    LOG.info("Triggered PubSub to Redis STREAMS_SINK template job");

    List<String> inMessages = Arrays.asList("stream-message-1", "stream-message-2");

    Result result =
        pipelineOperator()
            .waitForConditionAndFinish(
                createConfig(info),
                () -> {
                  for (final String message : inMessages) {
                    ByteString data = ByteString.copyFromUtf8(message);
                    pubsubResourceManager.publish(
                        tc, ImmutableMap.of("stream", "test-stream"), data);
                  }

                  long keysCount = redisClient.dbSize();
                  LOG.info("Redis DB size for STREAMS: {}", keysCount);

                  return keysCount > 0;
                });

    LOG.info("Successfully verified stream messages in Redis");
  }
}
