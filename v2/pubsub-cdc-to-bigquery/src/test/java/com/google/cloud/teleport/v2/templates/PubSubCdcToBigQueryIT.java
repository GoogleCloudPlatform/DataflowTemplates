/*
 * Copyright (C) 2024 Google LLC
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
import static org.apache.beam.it.gcp.bigquery.matchers.BigQueryAsserts.assertThatBigQueryRecords;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.SubscriptionName;
import com.google.pubsub.v1.TopicName;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineLauncher.LaunchConfig;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.TemplateTestBase;
import org.apache.beam.it.gcp.bigquery.BigQueryResourceManager;
import org.apache.beam.it.gcp.bigquery.conditions.BigQueryRowsCheck;
import org.apache.beam.it.gcp.pubsub.PubsubResourceManager;
import org.apache.commons.lang3.RandomStringUtils;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@Category(TemplateIntegrationTest.class)
@TemplateIntegrationTest(PubSubCdcToBigQuery.class)
@RunWith(JUnit4.class)
public class PubSubCdcToBigQueryIT extends TemplateTestBase {

  private static final int MESSAGES_COUNT = 10;
  private static final int BAD_MESSAGES_COUNT = 3;

  private static final Schema BIG_QUERY_DLQ_SCHEMA = getDlqSchema();

  private PubsubResourceManager pubsubResourceManager;
  private BigQueryResourceManager bigQueryResourceManager;

  @Before
  public void setUp() throws IOException {
    pubsubResourceManager =
        PubsubResourceManager.builder(testName, PROJECT, credentialsProvider).build();
    bigQueryResourceManager =
        BigQueryResourceManager.builder(testName, PROJECT, credentials).build();

    gcsClient.createArtifact(
        "udf.js",
        "function uppercaseName(value) {\n"
            + "  const data = JSON.parse(value);\n"
            + "  data.name = data.name.toUpperCase();\n"
            + "  return JSON.stringify(data);\n"
            + "}");
    gcsClient.createArtifact(
        "cdc_schema.json",
        "["
            + "  {"
            + "    \"type\": \"INTEGER\","
            + "    \"name\": \"id\","
            + "    \"mode\": \"NULLABLE\""
            + "  }, "
            + "  {"
            + "    \"type\": \"STRING\","
            + "    \"name\":\"job\","
            + "    \"mode\":\"NULLABLE\""
            + "  }, "
            + "  {"
            + "    \"type\": \"String\","
            + "    \"name\": \"name\","
            + "    \"mode\": \"NULLABLE\""
            + "  }"
            + "]");
  }

  @After
  public void cleanUp() {
    ResourceManagerUtils.cleanResources(pubsubResourceManager, bigQueryResourceManager);
  }

  @Test
  public void testSubscriptionToBigQuery() throws IOException {
    subscriptionToBigQueryBase(false);
  }

  @Test
  public void testSubscriptionToBigQueryWithUDf() throws IOException {
    subscriptionToBigQueryBase(true);
  }

  public void subscriptionToBigQueryBase(boolean useUdf) throws IOException {
    // Arrange
    // Omit name column to ensure table column mapping works properly
    List<Field> bqSchemaFields =
        Arrays.asList(
            Field.of("id", StandardSQLTypeName.INT64), Field.of("job", StandardSQLTypeName.STRING));
    Schema bqSchema = Schema.of(bqSchemaFields);

    TopicName topic = pubsubResourceManager.createTopic("input");
    SubscriptionName subscription = pubsubResourceManager.createSubscription(topic, "input-sub-1");
    bigQueryResourceManager.createDataset(REGION);
    TableId table = bigQueryResourceManager.createTable(testName, bqSchema);

    TableId dlqTable =
        bigQueryResourceManager.createTable(
            table.getTable() + PubSubCdcToBigQuery.DEFAULT_DEADLETTER_TABLE_SUFFIX,
            BIG_QUERY_DLQ_SCHEMA);

    // Act
    LaunchConfig.Builder launchConfigBuilder =
        LaunchConfig.builder(testName, specPath)
            .addParameter("inputSubscription", subscription.toString())
            .addParameter("schemaFilePath", getGcsPath("cdc_schema.json"))
            .addParameter("outputDatasetTemplate", table.getDataset())
            .addParameter("outputTableNameTemplate", table.getTable())
            .addParameter("outputDeadletterTable", toTableSpecLegacy(dlqTable));
    if (useUdf) {
      launchConfigBuilder
          .addParameter("javascriptTextTransformGcsPath", getGcsPath("udf.js"))
          .addParameter("javascriptTextTransformFunctionName", "uppercaseName");
    }
    PipelineLauncher.LaunchInfo info = launchTemplate(launchConfigBuilder);
    assertThatPipeline(info).isRunning();

    List<Map<String, Object>> expectedMessages = new ArrayList<>();
    for (int i = 1; i <= MESSAGES_COUNT; i++) {
      Map<String, Object> message =
          new HashMap<>(
              Map.of(
                  "id",
                  i,
                  "job",
                  testName,
                  "name",
                  RandomStringUtils.randomAlphabetic(1, 20).toLowerCase()));
      ByteString messageData = ByteString.copyFromUtf8(new JSONObject(message).toString());
      pubsubResourceManager.publish(topic, ImmutableMap.of(), messageData);
      if (useUdf) {
        message.put("name", message.get("name").toString().toUpperCase());
      }
      expectedMessages.add(message);
    }

    for (int i = 1; i <= BAD_MESSAGES_COUNT; i++) {
      ByteString messageData = ByteString.copyFromUtf8("bad id " + i);
      pubsubResourceManager.publish(topic, ImmutableMap.of(), messageData);
    }

    PipelineOperator.Result result =
        pipelineOperator()
            .waitForConditionsAndFinish(
                createConfig(info),
                BigQueryRowsCheck.builder(bigQueryResourceManager, table)
                    .setMinRows(MESSAGES_COUNT)
                    .build(),
                BigQueryRowsCheck.builder(bigQueryResourceManager, dlqTable)
                    .setMinRows(BAD_MESSAGES_COUNT)
                    .build());

    // Assert
    assertThatResult(result).meetsConditions();
    TableResult records = bigQueryResourceManager.readTable(table);

    // Make sure record can be read and UDF changed name to uppercase
    assertThatBigQueryRecords(records).hasRecordsUnordered(expectedMessages);

    TableResult dlqRecords = bigQueryResourceManager.readTable(dlqTable);
    if (useUdf) {
      assertThat(dlqRecords.getValues().iterator().next().toString())
          .contains("Expected json literal but found");
    } else {
      assertThat(dlqRecords.getValues().iterator().next().toString())
          .contains("Failed to serialize json to table row");
    }
  }

  private static Schema getDlqSchema() {
    return Schema.of(
        Arrays.asList(
            Field.newBuilder("timestamp", StandardSQLTypeName.TIMESTAMP)
                .setMode(Field.Mode.REQUIRED)
                .build(),
            Field.newBuilder("payloadString", StandardSQLTypeName.STRING)
                .setMode(Field.Mode.REQUIRED)
                .build(),
            Field.newBuilder("payloadBytes", StandardSQLTypeName.BYTES)
                .setMode(Field.Mode.REQUIRED)
                .build(),
            Field.newBuilder(
                    "attributes",
                    LegacySQLTypeName.RECORD,
                    Field.newBuilder("key", StandardSQLTypeName.STRING)
                        .setMode(Field.Mode.NULLABLE)
                        .build(),
                    Field.newBuilder("value", StandardSQLTypeName.STRING)
                        .setMode(Field.Mode.NULLABLE)
                        .build())
                .setMode(Field.Mode.REPEATED)
                .build(),
            Field.newBuilder("errorMessage", StandardSQLTypeName.STRING)
                .setMode(Field.Mode.NULLABLE)
                .build(),
            Field.newBuilder("stacktrace", StandardSQLTypeName.STRING)
                .setMode(Field.Mode.NULLABLE)
                .build()));
  }
}
