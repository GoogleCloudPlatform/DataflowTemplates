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
import static com.google.cloud.teleport.it.matchers.TemplateAsserts.assertThatRecords;
import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.teleport.it.TemplateTestBase;
import com.google.cloud.teleport.it.bigquery.BigQueryResourceManager;
import com.google.cloud.teleport.it.bigquery.DefaultBigQueryResourceManager;
import com.google.cloud.teleport.it.dataflow.DataflowOperator;
import com.google.cloud.teleport.it.dataflow.DataflowOperator.Result;
import com.google.cloud.teleport.it.dataflow.DataflowTemplateClient.JobInfo;
import com.google.cloud.teleport.it.dataflow.DataflowTemplateClient.JobState;
import com.google.cloud.teleport.it.dataflow.DataflowTemplateClient.LaunchConfig;
import com.google.cloud.teleport.it.pubsub.DefaultPubsubResourceManager;
import com.google.cloud.teleport.it.pubsub.PubsubResourceManager;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.SubscriptionName;
import com.google.pubsub.v1.TopicName;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
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
  private static PubsubResourceManager pubsubResourceManager;
  private static BigQueryResourceManager bigQueryResourceManager;

  @Before
  public void setUp() throws IOException {
    pubsubResourceManager =
        DefaultPubsubResourceManager.builder(testName.getMethodName(), PROJECT)
            .credentialsProvider(credentialsProvider)
            .build();
    bigQueryResourceManager =
        DefaultBigQueryResourceManager.builder(testName.getMethodName(), PROJECT)
            .setCredentials(credentials)
            .build();
  }

  @After
  public void cleanUp() {
    pubsubResourceManager.cleanupAll();
    bigQueryResourceManager.cleanupAll();
  }

  @Test
  public void testSubscriptionToBigQuery() throws IOException {
    // Arrange
    String jobName = createJobName(testName.getMethodName());
    String bqTable = testName.getMethodName();
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
    bigQueryResourceManager.createTable(bqTable, bqSchema);
    String tableSpec = PROJECT + ":" + bigQueryResourceManager.getDatasetId() + "." + bqTable;

    LaunchConfig.Builder options =
        LaunchConfig.builder(jobName, specPath)
            .addParameter("inputSubscription", subscription.toString())
            .addParameter("outputTableSpec", tableSpec);

    // Act
    JobInfo info = launchTemplate(options);
    assertThat(info.state()).isIn(JobState.ACTIVE_STATES);

    AtomicReference<TableResult> records = new AtomicReference<>();
    Result result =
        new DataflowOperator(getDataflowClient())
            .waitForConditionAndFinish(
                createConfig(info),
                () -> {
                  TableResult values = bigQueryResourceManager.readTable(bqTable);
                  records.set(values);
                  return values.getTotalRows() >= messages.size();
                });

    // Assert
    assertThat(result).isEqualTo(Result.CONDITION_MET);
    assertThatRecords(records.get()).hasRecords(messages);
  }

  private void publishMessages(TopicName topic, List<Map<String, Object>> messages) {
    for (Map<String, Object> message : messages) {
      // Publishing message as a JSON string
      ByteString messageData = ByteString.copyFromUtf8(new JSONObject(message).toString());
      pubsubResourceManager.publish(topic, ImmutableMap.of(), messageData);
    }
  }
}
