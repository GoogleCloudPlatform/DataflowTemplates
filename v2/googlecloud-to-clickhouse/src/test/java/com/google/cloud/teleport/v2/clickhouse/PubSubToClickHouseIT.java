/*
 * Copyright (C) 2025 Google LLC
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
package com.google.cloud.teleport.v2.clickhouse;

import static com.google.common.truth.Truth.assertThat;
import static org.apache.beam.it.common.utils.PipelineUtils.createJobName;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatRecords;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.cloud.teleport.v2.clickhouse.templates.PubSubToClickHouse;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.SubscriptionName;
import com.google.pubsub.v1.TopicName;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.it.clickhouse.ClickHouseResourceManager;
import org.apache.beam.it.clickhouse.conditions.ClickHouseRowsCheck;
import org.apache.beam.it.common.PipelineLauncher.LaunchConfig;
import org.apache.beam.it.common.PipelineLauncher.LaunchInfo;
import org.apache.beam.it.common.PipelineOperator.Result;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.TemplateTestBase;
import org.apache.beam.it.gcp.pubsub.PubsubResourceManager;
import org.apache.beam.it.gcp.pubsub.conditions.PubsubMessagesCheck;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Integration tests for {@link PubSubToClickHouse}. */
@Category(TemplateIntegrationTest.class)
@TemplateIntegrationTest(PubSubToClickHouse.class)
@RunWith(JUnit4.class)
public class PubSubToClickHouseIT extends TemplateTestBase {

  private static final int MESSAGES_COUNT = 10;
  private static final int BAD_MESSAGES_COUNT = 3;

  private PubsubResourceManager pubsubResourceManager;
  private ClickHouseResourceManager clickHouseResourceManager;

  @Before
  public void setup() throws IOException {
    pubsubResourceManager =
        PubsubResourceManager.builder(testName, PROJECT, credentialsProvider).build();
    clickHouseResourceManager = ClickHouseResourceManager.builder(testId).build();
  }

  @After
  public void tearDown() {
    ResourceManagerUtils.cleanResources(pubsubResourceManager, clickHouseResourceManager);
  }

  /**
   * Tests the happy path: valid JSON messages land in the main ClickHouse table, and malformed
   * messages are routed to a dead-letter ClickHouse table.
   */
  @Test
  public void testPubSubToClickHouse() throws IOException {
    // Arrange
    TopicName topic = pubsubResourceManager.createTopic("input");
    SubscriptionName subscription = pubsubResourceManager.createSubscription(topic, "input-sub");

    String tableName = createJobName(testName).replace("-", "_");
    String dlqTableName = tableName + "_dlq";

    Map<String, String> columns = new LinkedHashMap<>();
    columns.put("id", "String");
    columns.put("name", "String");
    clickHouseResourceManager.createTable(tableName, columns, null, null);

    Map<String, String> dlqColumns = new LinkedHashMap<>();
    dlqColumns.put("raw_message", "String");
    dlqColumns.put("error_message", "String");
    dlqColumns.put("stack_trace", "String");
    dlqColumns.put("failed_at", "String");
    clickHouseResourceManager.createTable(dlqTableName, dlqColumns, null, null);

    // Act
    LaunchInfo info =
        launchTemplate(
            LaunchConfig.builder(testName, specPath)
                .addParameter("inputSubscription", subscription.toString())
                .addParameter("clickHouseUrl", getClickHouseHttpUrl())
                .addParameter("clickHouseDatabase", "default")
                .addParameter("clickHouseTable", tableName)
                .addParameter("clickHouseUsername", "default")
                .addParameter("clickHousePassword", "")
                .addParameter("clickHouseDeadLetterTable", dlqTableName)
                .addParameter("batchRowCount", "1"));
    assertThatPipeline(info).isRunning();

    List<Map<String, Object>> expectedRows = new ArrayList<>();
    for (int i = 0; i < MESSAGES_COUNT; i++) {
      String name = testName + i;
      pubsubResourceManager.publish(
          topic,
          ImmutableMap.of(),
          ByteString.copyFromUtf8(String.format("{\"id\": \"%d\", \"name\": \"%s\"}", i, name)));
      expectedRows.add(Map.of("id", String.valueOf(i), "name", name));
    }
    for (int i = 0; i < BAD_MESSAGES_COUNT; i++) {
      pubsubResourceManager.publish(
          topic, ImmutableMap.of(), ByteString.copyFromUtf8("bad message " + i));
    }

    Result result =
        pipelineOperator()
            .waitForConditionAndCancel(
                createConfig(info),
                ClickHouseRowsCheck.builder(clickHouseResourceManager, tableName)
                    .setMinRows(MESSAGES_COUNT)
                    .build(),
                ClickHouseRowsCheck.builder(clickHouseResourceManager, dlqTableName)
                    .setMinRows(BAD_MESSAGES_COUNT)
                    .build());

    // Assert
    assertThatResult(result).meetsConditions();
    assertThat(clickHouseResourceManager.count(tableName)).isAtLeast(MESSAGES_COUNT);
    assertThat(clickHouseResourceManager.count(dlqTableName)).isAtLeast(BAD_MESSAGES_COUNT);
    assertThatRecords(clickHouseResourceManager.fetchAll(tableName))
        .hasRecordsUnordered(expectedRows);
  }

  /**
   * Tests dead-letter routing to a Pub/Sub topic: malformed messages are published to the DLQ
   * topic, and valid messages land in the main ClickHouse table.
   */
  @Test
  public void testPubSubToClickHouseWithPubSubDlq() throws IOException {
    // Arrange
    TopicName inputTopic = pubsubResourceManager.createTopic("input");
    SubscriptionName inputSubscription =
        pubsubResourceManager.createSubscription(inputTopic, "input-sub");
    TopicName dlqTopic = pubsubResourceManager.createTopic("dlq");
    SubscriptionName dlqSubscription =
        pubsubResourceManager.createSubscription(dlqTopic, "dlq-sub");

    String tableName = createJobName(testName).replace("-", "_");

    Map<String, String> columns = new LinkedHashMap<>();
    columns.put("id", "String");
    columns.put("name", "String");
    clickHouseResourceManager.createTable(tableName, columns, null, null);

    // Act
    LaunchInfo info =
        launchTemplate(
            LaunchConfig.builder(testName, specPath)
                .addParameter("inputSubscription", inputSubscription.toString())
                .addParameter("clickHouseUrl", getClickHouseHttpUrl())
                .addParameter("clickHouseDatabase", "default")
                .addParameter("clickHouseTable", tableName)
                .addParameter("clickHouseUsername", "default")
                .addParameter("clickHousePassword", "")
                .addParameter("deadLetterTopic", dlqTopic.toString())
                .addParameter("batchRowCount", "1"));
    assertThatPipeline(info).isRunning();

    List<Map<String, Object>> expectedRows = new ArrayList<>();
    for (int i = 0; i < MESSAGES_COUNT; i++) {
      String name = testName + i;
      pubsubResourceManager.publish(
          inputTopic,
          ImmutableMap.of(),
          ByteString.copyFromUtf8(String.format("{\"id\": \"%d\", \"name\": \"%s\"}", i, name)));
      expectedRows.add(Map.of("id", String.valueOf(i), "name", name));
    }
    for (int i = 0; i < BAD_MESSAGES_COUNT; i++) {
      pubsubResourceManager.publish(
          inputTopic, ImmutableMap.of(), ByteString.copyFromUtf8("bad message " + i));
    }

    PubsubMessagesCheck dlqCheck =
        PubsubMessagesCheck.builder(pubsubResourceManager, dlqSubscription)
            .setMinMessages(BAD_MESSAGES_COUNT)
            .build();

    Result result =
        pipelineOperator()
            .waitForConditionAndCancel(
                createConfig(info),
                ClickHouseRowsCheck.builder(clickHouseResourceManager, tableName)
                    .setMinRows(MESSAGES_COUNT)
                    .build(),
                dlqCheck);

    // Assert
    assertThatResult(result).meetsConditions();
    assertThat(clickHouseResourceManager.count(tableName)).isAtLeast(MESSAGES_COUNT);
    assertThat(dlqCheck.getReceivedMessageList().size()).isAtLeast(BAD_MESSAGES_COUNT);
    assertThatRecords(clickHouseResourceManager.fetchAll(tableName))
        .hasRecordsUnordered(expectedRows);
  }

  /**
   * Tests that both dead-letter destinations receive failed messages simultaneously when both
   * {@code --clickHouseDeadLetterTable} and {@code --deadLetterTopic} are configured.
   */
  @Test
  public void testPubSubToClickHouseWithBothDlqDestinations() throws IOException {
    // Arrange
    TopicName inputTopic = pubsubResourceManager.createTopic("input");
    SubscriptionName inputSubscription =
        pubsubResourceManager.createSubscription(inputTopic, "input-sub");
    TopicName dlqTopic = pubsubResourceManager.createTopic("dlq");
    SubscriptionName dlqSubscription =
        pubsubResourceManager.createSubscription(dlqTopic, "dlq-sub");

    String tableName = createJobName(testName).replace("-", "_");
    String dlqTableName = tableName + "_dlq";

    Map<String, String> columns = new LinkedHashMap<>();
    columns.put("id", "String");
    columns.put("name", "String");
    clickHouseResourceManager.createTable(tableName, columns, null, null);

    Map<String, String> dlqColumns = new LinkedHashMap<>();
    dlqColumns.put("raw_message", "String");
    dlqColumns.put("error_message", "String");
    dlqColumns.put("stack_trace", "String");
    dlqColumns.put("failed_at", "String");
    clickHouseResourceManager.createTable(dlqTableName, dlqColumns, null, null);

    // Act
    LaunchInfo info =
        launchTemplate(
            LaunchConfig.builder(testName, specPath)
                .addParameter("inputSubscription", inputSubscription.toString())
                .addParameter("clickHouseUrl", getClickHouseHttpUrl())
                .addParameter("clickHouseDatabase", "default")
                .addParameter("clickHouseTable", tableName)
                .addParameter("clickHouseUsername", "default")
                .addParameter("clickHousePassword", "")
                .addParameter("clickHouseDeadLetterTable", dlqTableName)
                .addParameter("deadLetterTopic", dlqTopic.toString())
                .addParameter("batchRowCount", "1"));
    assertThatPipeline(info).isRunning();

    List<Map<String, Object>> expectedRows = new ArrayList<>();
    for (int i = 0; i < MESSAGES_COUNT; i++) {
      String name = testName + i;
      pubsubResourceManager.publish(
          inputTopic,
          ImmutableMap.of(),
          ByteString.copyFromUtf8(String.format("{\"id\": \"%d\", \"name\": \"%s\"}", i, name)));
      expectedRows.add(Map.of("id", String.valueOf(i), "name", name));
    }
    for (int i = 0; i < BAD_MESSAGES_COUNT; i++) {
      pubsubResourceManager.publish(
          inputTopic, ImmutableMap.of(), ByteString.copyFromUtf8("bad message " + i));
    }

    PubsubMessagesCheck pubsubDlqCheck =
        PubsubMessagesCheck.builder(pubsubResourceManager, dlqSubscription)
            .setMinMessages(BAD_MESSAGES_COUNT)
            .build();

    Result result =
        pipelineOperator()
            .waitForConditionAndCancel(
                createConfig(info),
                ClickHouseRowsCheck.builder(clickHouseResourceManager, tableName)
                    .setMinRows(MESSAGES_COUNT)
                    .build(),
                ClickHouseRowsCheck.builder(clickHouseResourceManager, dlqTableName)
                    .setMinRows(BAD_MESSAGES_COUNT)
                    .build(),
                pubsubDlqCheck);

    // Assert
    assertThatResult(result).meetsConditions();
    assertThat(clickHouseResourceManager.count(tableName)).isAtLeast(MESSAGES_COUNT);
    assertThat(clickHouseResourceManager.count(dlqTableName)).isAtLeast(BAD_MESSAGES_COUNT);
    assertThat(pubsubDlqCheck.getReceivedMessageList().size()).isAtLeast(BAD_MESSAGES_COUNT);
    assertThatRecords(clickHouseResourceManager.fetchAll(tableName))
        .hasRecordsUnordered(expectedRows);
  }

  /**
   * Tests that messages with numeric fields (Int64, Float64) are correctly parsed and written to
   * the main ClickHouse table without any conversion exceptions routing them to the dead-letter
   * table.
   */
  @Test
  public void testPubSubToClickHouseWithNumericColumns() throws IOException {
    // Arrange
    TopicName topic = pubsubResourceManager.createTopic("input");
    SubscriptionName subscription = pubsubResourceManager.createSubscription(topic, "input-sub");

    String tableName = createJobName(testName).replace("-", "_");
    String dlqTableName = tableName + "_dlq";

    Map<String, String> columns = new LinkedHashMap<>();
    columns.put("id", "Int64");
    columns.put("score", "Float64");
    clickHouseResourceManager.createTable(tableName, columns, null, null);

    Map<String, String> dlqColumns = new LinkedHashMap<>();
    dlqColumns.put("raw_message", "String");
    dlqColumns.put("error_message", "String");
    dlqColumns.put("stack_trace", "String");
    dlqColumns.put("failed_at", "String");
    clickHouseResourceManager.createTable(dlqTableName, dlqColumns, null, null);

    // Act
    LaunchInfo info =
        launchTemplate(
            LaunchConfig.builder(testName, specPath)
                .addParameter("inputSubscription", subscription.toString())
                .addParameter("clickHouseUrl", getClickHouseHttpUrl())
                .addParameter("clickHouseDatabase", "default")
                .addParameter("clickHouseTable", tableName)
                .addParameter("clickHouseUsername", "default")
                .addParameter("clickHousePassword", "")
                .addParameter("clickHouseDeadLetterTable", dlqTableName)
                .addParameter("batchRowCount", "1"));
    assertThatPipeline(info).isRunning();

    for (int i = 0; i < MESSAGES_COUNT; i++) {
      pubsubResourceManager.publish(
          topic,
          ImmutableMap.of(),
          ByteString.copyFromUtf8(String.format("{\"id\": %d, \"score\": %.1f}", i, i * 1.5)));
    }

    Result result =
        pipelineOperator()
            .waitForConditionAndCancel(
                createConfig(info),
                ClickHouseRowsCheck.builder(clickHouseResourceManager, tableName)
                    .setMinRows(MESSAGES_COUNT)
                    .build());

    // Assert — all rows landed in main table; none were routed to DLQ due to type errors
    assertThatResult(result).meetsConditions();
    assertThat(clickHouseResourceManager.count(tableName)).isAtLeast(MESSAGES_COUNT);
    assertThat(clickHouseResourceManager.count(dlqTableName)).isEqualTo(0);
  }

  /**
   * Tests that messages with DateTime fields are correctly parsed and written to the main
   * ClickHouse table without any conversion exceptions routing them to the dead-letter table.
   */
  @Test
  public void testPubSubToClickHouseWithDateTimeColumns() throws IOException {
    // Arrange
    TopicName topic = pubsubResourceManager.createTopic("input");
    SubscriptionName subscription = pubsubResourceManager.createSubscription(topic, "input-sub");

    String tableName = createJobName(testName).replace("-", "_");
    String dlqTableName = tableName + "_dlq";

    Map<String, String> columns = new LinkedHashMap<>();
    columns.put("id", "String");
    columns.put("created_at", "DateTime");
    clickHouseResourceManager.createTable(tableName, columns, null, null);

    Map<String, String> dlqColumns = new LinkedHashMap<>();
    dlqColumns.put("raw_message", "String");
    dlqColumns.put("error_message", "String");
    dlqColumns.put("stack_trace", "String");
    dlqColumns.put("failed_at", "String");
    clickHouseResourceManager.createTable(dlqTableName, dlqColumns, null, null);

    // Act
    LaunchInfo info =
        launchTemplate(
            LaunchConfig.builder(testName, specPath)
                .addParameter("inputSubscription", subscription.toString())
                .addParameter("clickHouseUrl", getClickHouseHttpUrl())
                .addParameter("clickHouseDatabase", "default")
                .addParameter("clickHouseTable", tableName)
                .addParameter("clickHouseUsername", "default")
                .addParameter("clickHousePassword", "")
                .addParameter("clickHouseDeadLetterTable", dlqTableName)
                .addParameter("batchRowCount", "1"));
    assertThatPipeline(info).isRunning();

    for (int i = 0; i < MESSAGES_COUNT; i++) {
      pubsubResourceManager.publish(
          topic,
          ImmutableMap.of(),
          ByteString.copyFromUtf8(
              String.format(
                  "{\"id\": \"%d\", \"created_at\": \"2025-01-%02dT10:30:00Z\"}", i, i + 1)));
    }

    Result result =
        pipelineOperator()
            .waitForConditionAndCancel(
                createConfig(info),
                ClickHouseRowsCheck.builder(clickHouseResourceManager, tableName)
                    .setMinRows(MESSAGES_COUNT)
                    .build());

    // Assert — all rows landed in main table; none were routed to DLQ due to DateTime parse errors
    assertThatResult(result).meetsConditions();
    assertThat(clickHouseResourceManager.count(tableName)).isAtLeast(MESSAGES_COUNT);
    assertThat(clickHouseResourceManager.count(dlqTableName)).isEqualTo(0);
  }

  /**
   * Tests time-only windowing mode: when only {@code --windowSeconds} is set (no {@code
   * --batchRowCount}), the pipeline should flush rows to ClickHouse after the window duration
   * elapses rather than on a row-count trigger.
   */
  @Test
  public void testPubSubToClickHouseWithTimeWindowMode() throws IOException {
    // Arrange
    TopicName topic = pubsubResourceManager.createTopic("input");
    SubscriptionName subscription = pubsubResourceManager.createSubscription(topic, "input-sub");

    String tableName = createJobName(testName).replace("-", "_");
    String dlqTableName = tableName + "_dlq";

    Map<String, String> columns = new LinkedHashMap<>();
    columns.put("id", "String");
    columns.put("name", "String");
    clickHouseResourceManager.createTable(tableName, columns, null, null);

    Map<String, String> dlqColumns = new LinkedHashMap<>();
    dlqColumns.put("raw_message", "String");
    dlqColumns.put("error_message", "String");
    dlqColumns.put("stack_trace", "String");
    dlqColumns.put("failed_at", "String");
    clickHouseResourceManager.createTable(dlqTableName, dlqColumns, null, null);

    // Act — time-only mode: windowSeconds set, batchRowCount deliberately omitted
    LaunchInfo info =
        launchTemplate(
            LaunchConfig.builder(testName, specPath)
                .addParameter("inputSubscription", subscription.toString())
                .addParameter("clickHouseUrl", getClickHouseHttpUrl())
                .addParameter("clickHouseDatabase", "default")
                .addParameter("clickHouseTable", tableName)
                .addParameter("clickHouseUsername", "default")
                .addParameter("clickHousePassword", "")
                .addParameter("clickHouseDeadLetterTable", dlqTableName)
                .addParameter("windowSeconds", "10"));
    assertThatPipeline(info).isRunning();

    List<Map<String, Object>> expectedRows = new ArrayList<>();
    for (int i = 0; i < MESSAGES_COUNT; i++) {
      String name = testName + i;
      pubsubResourceManager.publish(
          topic,
          ImmutableMap.of(),
          ByteString.copyFromUtf8(String.format("{\"id\": \"%d\", \"name\": \"%s\"}", i, name)));
      expectedRows.add(Map.of("id", String.valueOf(i), "name", name));
    }

    // Rows are held until the 30-second window fires, so the wait condition allows for that delay
    Result result =
        pipelineOperator()
            .waitForConditionAndCancel(
                createConfig(info),
                ClickHouseRowsCheck.builder(clickHouseResourceManager, tableName)
                    .setMinRows(MESSAGES_COUNT)
                    .build());

    // Assert
    assertThatResult(result).meetsConditions();
    assertThat(clickHouseResourceManager.count(tableName)).isAtLeast(MESSAGES_COUNT);
    assertThat(clickHouseResourceManager.count(dlqTableName)).isEqualTo(0);
    assertThatRecords(clickHouseResourceManager.fetchAll(tableName))
        .hasRecordsUnordered(expectedRows);
  }

  /**
   * Derives the ClickHouse HTTP URL from the JDBC connection string exposed by the resource
   * manager. The JDBC format is {@code jdbc:clickhouse://HOST:PORT/default}; the template requires
   * {@code http://HOST:PORT}. Would need to be updated once we move the Resource Manager to use the New ClickHouse Java V2 client
   */
  private String getClickHouseHttpUrl() {
    String jdbcUrl = clickHouseResourceManager.getJdbcConnectionString();
    return "http://" + jdbcUrl.replace("jdbc:clickhouse://", "").replaceAll("/.*", "");
  }
}
