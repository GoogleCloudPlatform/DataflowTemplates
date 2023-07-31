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

import static org.apache.beam.it.common.utils.PipelineUtils.createJobName;
import static org.apache.beam.it.gcp.bigquery.matchers.BigQueryAsserts.assertThatBigQueryRecords;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.SubscriptionName;
import com.google.pubsub.v1.TopicName;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.beam.it.common.PipelineLauncher.LaunchConfig;
import org.apache.beam.it.common.PipelineLauncher.LaunchInfo;
import org.apache.beam.it.common.PipelineOperator.Result;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.TemplateTestBase;
import org.apache.beam.it.gcp.bigquery.BigQueryResourceManager;
import org.apache.beam.it.gcp.bigquery.conditions.BigQueryRowsCheck;
import org.apache.beam.it.gcp.pubsub.PubsubResourceManager;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Integration test for {@link PubSubToBigQuery} PubSub Subscription to Bigquery. */
@Category(TemplateIntegrationTest.class)
@TemplateIntegrationTest(
    value = PubSubToBigQuery.class,
    template = "PubSub_Subscription_to_BigQuery")
@RunWith(JUnit4.class)
public class PubSubSubscriptionToBigQueryIT extends TemplateTestBase {
  private PubsubResourceManager pubsubResourceManager;
  private BigQueryResourceManager bigQueryResourceManager;

  @Before
  public void setUp() throws IOException {
    pubsubResourceManager =
        PubsubResourceManager.builder(testName, PROJECT)
            .credentialsProvider(credentialsProvider)
            .build();
    bigQueryResourceManager =
        BigQueryResourceManager.builder(testName, PROJECT).setCredentials(credentials).build();
  }

  @After
  public void cleanUp() {
    ResourceManagerUtils.cleanResources(pubsubResourceManager, bigQueryResourceManager);
  }

  @Test
  public void testSubscriptionToBigQuery() throws IOException {
    // Arrange
    String jobName = createJobName(testName);
    String bqTable = testName;
    List<Map<String, Object>> messages =
        List.of(
            Map.of("job", jobName, "msgNumber", 1),
            Map.of("job", jobName, "msgNumber", 2),
            Map.of("job", jobName, "msgNumber", 3));
    List<Field> bqSchemaFields =
        Arrays.asList(
            Field.of("job", StandardSQLTypeName.STRING),
            Field.of("msgNumber", StandardSQLTypeName.INT64));
    Schema bqSchema = Schema.of(bqSchemaFields);

    TopicName topic = pubsubResourceManager.createTopic("input-topic");
    SubscriptionName subscription =
        pubsubResourceManager.createSubscription(topic, "input-subscription");
    publishMessages(topic, messages);

    bigQueryResourceManager.createDataset(REGION);
    TableId table = bigQueryResourceManager.createTable(bqTable, bqSchema);

    LaunchConfig.Builder options =
        LaunchConfig.builder(testName, specPath)
            .addParameter("inputSubscription", subscription.toString())
            .addParameter("outputTableSpec", toTableSpecLegacy(table));

    // Act
    LaunchInfo info = launchTemplate(options);
    assertThatPipeline(info).isRunning();

    Result result =
        pipelineOperator()
            .waitForConditionAndFinish(
                createConfig(info),
                BigQueryRowsCheck.builder(bigQueryResourceManager, table)
                    .setMinRows(messages.size())
                    .build());

    // Assert
    assertThatResult(result).meetsConditions();
    assertThatBigQueryRecords(bigQueryResourceManager.readTable(table)).hasRecords(messages);
  }

  private void publishMessages(TopicName topic, List<Map<String, Object>> messages) {
    for (Map<String, Object> message : messages) {
      // Publishing message as a JSON string
      ByteString messageData = ByteString.copyFromUtf8(new JSONObject(message).toString());
      pubsubResourceManager.publish(topic, ImmutableMap.of(), messageData);
    }
  }
}
