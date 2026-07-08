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
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatRecords;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.ReceivedMessage;
import com.google.pubsub.v1.SubscriptionName;
import com.google.pubsub.v1.TopicName;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.it.common.PipelineLauncher.LaunchConfig;
import org.apache.beam.it.common.PipelineLauncher.LaunchInfo;
import org.apache.beam.it.common.PipelineOperator.Result;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.JDBCBaseIT;
import org.apache.beam.it.gcp.pubsub.PubsubResourceManager;
import org.apache.beam.it.gcp.pubsub.conditions.PubsubMessagesCheck;
import org.apache.beam.it.jdbc.JDBCResourceManager;
import org.apache.beam.it.jdbc.MySQLResourceManager;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Integration test for {@link PubsubToJdbc} Flex template. */
@Category(TemplateIntegrationTest.class)
@TemplateIntegrationTest(PubsubToJdbc.class)
@RunWith(JUnit4.class)
public final class PubsubToJdbcIT extends JDBCBaseIT {

  private PubsubResourceManager pubsubResourceManager;
  private MySQLResourceManager jdbcResourceManager;

  private static final int MESSAGES_COUNT = 10;
  private static final int BAD_MESSAGES_COUNT = 3;

  @Before
  public void setUp() throws IOException {
    pubsubResourceManager =
        PubsubResourceManager.builder(testName, PROJECT, credentialsProvider).build();
    jdbcResourceManager = MySQLResourceManager.builder(testName).build();
  }

  @After
  public void cleanUp() {
    ResourceManagerUtils.cleanResources(pubsubResourceManager, jdbcResourceManager);
  }

  @Test
  public void testPubsubToJdbc() throws IOException {
    // Arrange
    Map<String, String> columns = new LinkedHashMap<>();
    columns.put("id", "NUMERIC NOT NULL");
    columns.put("job", "VARCHAR(32)");
    columns.put("name", "VARCHAR(32)");
    JDBCResourceManager.JDBCSchema schema = new JDBCResourceManager.JDBCSchema(columns, "id");
    TopicName topic = pubsubResourceManager.createTopic("input");
    jdbcResourceManager.createTable(testName, schema);

    SubscriptionName subscription = pubsubResourceManager.createSubscription(topic, "sub-1");
    TopicName dlqTopic = pubsubResourceManager.createTopic("output-dlq");
    SubscriptionName dlqSubscription =
        pubsubResourceManager.createSubscription(dlqTopic, "dlq-sub-1");

    LaunchConfig.Builder options =
        LaunchConfig.builder(testName, specPath)
            .addParameter("inputSubscription", subscription.toString())
            .addParameter("driverClassName", MYSQL_DRIVER)
            .addParameter("driverJars", mySqlDriverGCSPath())
            .addParameter("connectionUrl", jdbcResourceManager.getUri())
            .addParameter("username", jdbcResourceManager.getUsername())
            .addParameter("password", jdbcResourceManager.getPassword())
            .addParameter(
                "statement", "INSERT INTO " + testName + " (id, job, name) VALUES (?,?,?)")
            .addParameter("outputDeadletterTopic", dlqTopic.toString());

    // Act
    LaunchInfo info = launchTemplate(options);
    assertThatPipeline(info).isRunning();

    for (int i = 1; i <= MESSAGES_COUNT; i++) {
      Map<String, Object> message = Map.of("id", i, "job", testName, "name", "message");
      ByteString messageData = ByteString.copyFromUtf8(new JSONObject(message).toString());
      pubsubResourceManager.publish(topic, ImmutableMap.of(), messageData);
    }

    for (int i = 1; i <= BAD_MESSAGES_COUNT; i++) {
      Map<String, Object> message =
          Map.of("id", (char) ('a' + i), "job", testName, "name", "message");
      ByteString messageData = ByteString.copyFromUtf8(new JSONObject(message).toString());
      pubsubResourceManager.publish(topic, ImmutableMap.of(), messageData);
    }

    PubsubMessagesCheck dlqCheck =
        PubsubMessagesCheck.builder(pubsubResourceManager, dlqSubscription)
            .setMinMessages(BAD_MESSAGES_COUNT)
            .build();

    Result result =
        pipelineOperator()
            .waitForConditionsAndFinish(
                createConfig(info),
                () -> jdbcResourceManager.getRowCount(testName) >= MESSAGES_COUNT,
                dlqCheck);

    // Assert
    assertThatResult(result).meetsConditions();

    List<Map<String, Object>> records = jdbcResourceManager.readTable(testName);
    List<ReceivedMessage> dlqReceivedMessageList = dlqCheck.getReceivedMessageList();

    // Make sure record can be read and UDF changed name to uppercase
    assertThatRecords(records)
        .hasRecordsUnordered(List.of(Map.of("id", 1, "job", testName, "name", "message")));
    assertThat(dlqReceivedMessageList.size()).isAtLeast(BAD_MESSAGES_COUNT);
  }
}
