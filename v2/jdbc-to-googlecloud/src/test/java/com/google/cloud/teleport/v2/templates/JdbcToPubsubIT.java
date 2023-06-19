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

import static com.google.cloud.teleport.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static com.google.cloud.teleport.it.truthmatchers.PipelineAsserts.assertThatRecords;
import static com.google.cloud.teleport.it.truthmatchers.PipelineAsserts.assertThatResult;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

import com.google.cloud.teleport.it.common.PipelineLauncher.LaunchConfig;
import com.google.cloud.teleport.it.common.PipelineLauncher.LaunchInfo;
import com.google.cloud.teleport.it.common.PipelineOperator.Result;
import com.google.cloud.teleport.it.common.utils.ResourceManagerUtils;
import com.google.cloud.teleport.it.gcp.JDBCBaseIT;
import com.google.cloud.teleport.it.gcp.artifacts.utils.JsonTestUtil;
import com.google.cloud.teleport.it.gcp.pubsub.PubsubResourceManager;
import com.google.cloud.teleport.it.gcp.pubsub.conditions.PubsubMessagesCheck;
import com.google.cloud.teleport.it.jdbc.JDBCResourceManager;
import com.google.cloud.teleport.it.jdbc.MySQLResourceManager;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.pubsub.v1.SubscriptionName;
import com.google.pubsub.v1.TopicName;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Integration test for {@link JdbcToPubsub} template. */
@Category(TemplateIntegrationTest.class)
@TemplateIntegrationTest(JdbcToPubsub.class)
@RunWith(JUnit4.class)
public class JdbcToPubsubIT extends JDBCBaseIT {
  private PubsubResourceManager pubsubResourceManager;
  private MySQLResourceManager mysqlResourceManager;

  private static final String ROW_ID = "row_id";
  private static final String NAME = "name";
  private static final String AGE = "age";
  private static final String MEMBER = "member";
  private static final String ENTRY_ADDED = "entry_added";

  @Before
  public void setUp() throws IOException {
    pubsubResourceManager =
        PubsubResourceManager.builder(testName, PROJECT)
            .credentialsProvider(credentialsProvider)
            .build();
  }

  @After
  public void tearDown() {
    ResourceManagerUtils.cleanResources(mysqlResourceManager, pubsubResourceManager);
  }

  @Test
  public void testJdbcToPubsub() throws IOException {
    // Arrange
    mysqlResourceManager = MySQLResourceManager.builder(testName).build();

    // Arrange MySQL-compatible schema
    HashMap<String, String> columns = new HashMap<>();
    columns.put(ROW_ID, "NUMERIC NOT NULL");
    columns.put(NAME, "VARCHAR(200)");
    columns.put(AGE, "NUMERIC");
    columns.put(MEMBER, "VARCHAR(200)");
    columns.put(ENTRY_ADDED, "VARCHAR(200)");
    JDBCResourceManager.JDBCSchema schema = new JDBCResourceManager.JDBCSchema(columns, ROW_ID);
    mysqlResourceManager.createTable(testName, schema);
    List<Map<String, Object>> generatedData = generateData();
    mysqlResourceManager.write(testName, generatedData);

    TopicName outputTopic = pubsubResourceManager.createTopic("topic");
    SubscriptionName outputSubscription =
        pubsubResourceManager.createSubscription(outputTopic, "output-subscription");
    LaunchConfig.Builder options =
        LaunchConfig.builder(testName, specPath)
            .addParameter("connectionUrl", mysqlResourceManager.getUri())
            .addParameter("driverClassName", MYSQL_DRIVER)
            .addParameter("driverJars", mySqlDriverGCSPath())
            .addParameter("query", "SELECT * FROM " + testName)
            .addParameter("username", mysqlResourceManager.getUsername())
            .addParameter("password", mysqlResourceManager.getPassword())
            .addParameter("outputTopic", outputTopic.toString());

    // Act
    LaunchInfo info = launchTemplate(options);
    assertThatPipeline(info).isRunning();

    Result result = pipelineOperator().waitUntilDone(createConfig(info));
    assertThatResult(result).isLaunchFinished();

    PubsubMessagesCheck pubsubCheck =
        PubsubMessagesCheck.builder(pubsubResourceManager, outputSubscription)
            .setMinMessages(generatedData.size())
            .build();

    // Poll checker, to avoid timing issues on DirectRunner
    await("Check if messages got to Pub/Sub on time")
        .atMost(Duration.ofMinutes(2))
        .pollInterval(Duration.ofSeconds(5))
        .until(pubsubCheck::get);

    assertThatRecords(
            pubsubCheck.getReceivedMessageList().stream()
                .map(
                    receivedMessage -> {
                      try {
                        return JsonTestUtil.readRecord(
                            receivedMessage.getMessage().getData().toByteArray());
                      } catch (IOException e) {
                        throw new RuntimeException(e);
                      }
                    })
                .collect(Collectors.toList()))
        .hasRecordsUnordered(generatedData);
  }

  /**
   * Helper function for generating data according to the schema for this IT.
   *
   * @return A map containing the rows of data to be stored in the JDBC table.
   */
  private List<Map<String, Object>> generateData() {
    List<Map<String, Object>> data = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      Map<String, Object> values = new HashMap<>();
      values.put(ROW_ID, i);
      values.put(NAME, RandomStringUtils.randomAlphabetic(10));
      values.put(AGE, new Random().nextInt(100));
      values.put(MEMBER, i % 2 == 0 ? "Y" : "N");
      values.put(ENTRY_ADDED, Instant.now().toString());
      data.add(values);
    }

    return data;
  }
}
