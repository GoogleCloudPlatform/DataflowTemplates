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
package com.google.cloud.teleport.v2.templates;

import static com.google.common.truth.Truth.assertThat;
import static org.apache.beam.it.gcp.bigquery.matchers.BigQueryAsserts.assertThatBigQueryRecords;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.bigquery.Field;
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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
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

/** Integration test for {@link PubSubToBigQuery} Flex template. */
@Category(TemplateIntegrationTest.class)
@TemplateIntegrationTest(PubSubToBigQuery.class)
@RunWith(JUnit4.class)
public class PubSubToBigQueryIT extends TemplateTestBase {

  private PubsubResourceManager pubsubResourceManager;
  private BigQueryResourceManager bigQueryResourceManager;

  private static final int MESSAGES_COUNT = 10;
  private static final int BAD_MESSAGES_COUNT = 3;

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
  }

  @After
  public void cleanUp() {
    ResourceManagerUtils.cleanResources(pubsubResourceManager, bigQueryResourceManager);
  }

  @Test
  public void testPubsubToBigQuery() throws IOException, InterruptedException {
    basePubsubToBigQuery(Function.identity()); // no extra parameters
  }

  @Test
  public void testPubsubToBigQueryWithStorageApi() throws IOException, InterruptedException {
    basePubsubToBigQuery(
        b ->
            b.addParameter("useStorageWriteApi", "true")
                .addParameter("numStorageWriteApiStreams", "1")
                .addParameter("storageWriteApiTriggeringFrequencySec", "5"));
  }

  @Test
  public void testPubsubToBigQueryWithReload() throws IOException, InterruptedException {
    basePubsubToBigQueryWithReload(Function.identity());
  }

  private void basePubsubToBigQuery(
      Function<LaunchConfig.Builder, LaunchConfig.Builder> paramsAdder)
      throws IOException, InterruptedException {
    // Arrange
    List<Field> bqSchemaFields =
        Arrays.asList(
            Field.of("id", StandardSQLTypeName.INT64),
            Field.of("job", StandardSQLTypeName.STRING),
            Field.of("name", StandardSQLTypeName.STRING));
    Schema bqSchema = Schema.of(bqSchemaFields);

    TopicName topic = pubsubResourceManager.createTopic("input");
    bigQueryResourceManager.createDataset(REGION);
    SubscriptionName subscription = pubsubResourceManager.createSubscription(topic, "sub-1");
    TableId table = bigQueryResourceManager.createTable(testName, bqSchema);
    TableId dlqTable =
        TableId.of(
            PROJECT,
            table.getDataset(),
            table.getTable() + PubSubToBigQuery.DEFAULT_DEADLETTER_TABLE_SUFFIX);

    LaunchConfig.Builder options =
        paramsAdder.apply(
            LaunchConfig.builder(testName, specPath)
                .addParameter("inputSubscription", subscription.toString())
                .addParameter("outputTableSpec", toTableSpecLegacy(table))
                .addParameter("javascriptTextTransformGcsPath", getGcsPath("udf.js"))
                .addParameter("javascriptTextTransformFunctionName", "uppercaseName"));

    // Act
    LaunchInfo info = launchTemplate(options);
    assertThatPipeline(info).isRunning();

    for (int i = 1; i <= MESSAGES_COUNT; i++) {
      Map<String, Object> message = Map.of("id", i, "job", testName, "name", "message");
      ByteString messageData = ByteString.copyFromUtf8(new JSONObject(message).toString());
      pubsubResourceManager.publish(topic, ImmutableMap.of(), messageData);
    }

    for (int i = 1; i <= BAD_MESSAGES_COUNT; i++) {
      ByteString messageData = ByteString.copyFromUtf8("bad id " + i);
      pubsubResourceManager.publish(topic, ImmutableMap.of(), messageData);
    }

    Result result =
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
    assertThatBigQueryRecords(records)
        .hasRecordsUnordered(List.of(Map.of("id", 1, "job", testName, "name", "MESSAGE")));

    TableResult dlqRecords = bigQueryResourceManager.readTable(dlqTable);
    assertThat(dlqRecords.getValues().iterator().next().toString())
        .contains("Expected json literal but found");
  }

  private void basePubsubToBigQueryWithReload(
      Function<LaunchConfig.Builder, LaunchConfig.Builder> paramsAdder)
      throws IOException, InterruptedException {
    // Arrange
    List<Field> bqSchemaFields =
        Arrays.asList(
            Field.of("id", StandardSQLTypeName.INT64),
            Field.of("job", StandardSQLTypeName.STRING),
            Field.of("name", StandardSQLTypeName.STRING));
    Schema bqSchema = Schema.of(bqSchemaFields);

    TopicName topic = pubsubResourceManager.createTopic("input");
    bigQueryResourceManager.createDataset(REGION);
    SubscriptionName subscription = pubsubResourceManager.createSubscription(topic, "sub-1");
    TableId table = bigQueryResourceManager.createTable(testName, bqSchema);
    TableId dlqTable =
        TableId.of(
            PROJECT,
            table.getDataset(),
            table.getTable() + PubSubToBigQuery.DEFAULT_DEADLETTER_TABLE_SUFFIX);

    LaunchConfig.Builder options =
        paramsAdder.apply(
            LaunchConfig.builder(testName, specPath)
                .addParameter("inputSubscription", subscription.toString())
                .addParameter("outputTableSpec", toTableSpecLegacy(table))
                .addParameter("javascriptTextTransformGcsPath", getGcsPath("udf.js"))
                .addParameter("javascriptTextTransformFunctionReload", "true")
                .addParameter("javascriptTextTransformReloadIntervalMinutes", "1")
                .addParameter("javascriptTextTransformFunctionName", "uppercaseName"));

    // Act
    LaunchInfo info = launchTemplate(options);
    assertThatPipeline(info).isRunning();

    for (int i = 1; i <= MESSAGES_COUNT; i++) {
      Map<String, Object> message = Map.of("id", i, "job", testName, "name", "upper: message");
      ByteString messageData = ByteString.copyFromUtf8(new JSONObject(message).toString());
      pubsubResourceManager.publish(topic, ImmutableMap.of(), messageData);
    }

    for (int i = 1; i <= BAD_MESSAGES_COUNT; i++) {
      ByteString messageData = ByteString.copyFromUtf8("bad id " + i);
      pubsubResourceManager.publish(topic, ImmutableMap.of(), messageData);
    }
    BigQueryRowsCheck bigQueryRowsCheck =
        BigQueryRowsCheck.builder(bigQueryResourceManager, table)
            .setMinRows(MESSAGES_COUNT)
            .build();
    BigQueryRowsCheck bigQueryRowsCheckDLQ =
        BigQueryRowsCheck.builder(bigQueryResourceManager, dlqTable)
            .setMinRows(BAD_MESSAGES_COUNT)
            .build();
    Result result =
        pipelineOperator()
            .waitForCondition(createConfig(info), bigQueryRowsCheck, bigQueryRowsCheckDLQ);

    // Assert
    assertThatResult(result).meetsConditions();

    TableResult records = bigQueryResourceManager.readTable(table);

    // Make sure record can be read and UDF changed name to uppercase
    assertThatBigQueryRecords(records)
        .hasRecordsUnordered(List.of(Map.of("id", 1, "job", testName, "name", "UPPER: MESSAGE")));

    TableResult dlqRecords = bigQueryResourceManager.readTable(dlqTable);
    assertThat(dlqRecords.getValues().iterator().next().toString())
        .contains("Expected json literal but found");

    // modify UDF to test reloading
    gcsClient.createArtifact(
        "udf.js",
        "function uppercaseName(value) {\n"
            + "  const data = JSON.parse(value);\n"
            + "  data.name = data.name.toLowerCase();\n"
            + "  return JSON.stringify(data);\n"
            + "}");

    // wait to ensure the reload will take effect.
    TimeUnit.MINUTES.sleep(2);
    for (int i = MESSAGES_COUNT + 1; i <= MESSAGES_COUNT * 2; i++) {
      Map<String, Object> message = Map.of("id", i, "job", testName, "name", "LOWER: message");
      ByteString messageData = ByteString.copyFromUtf8(new JSONObject(message).toString());
      pubsubResourceManager.publish(topic, ImmutableMap.of(), messageData);
    }

    Result reloadedResult =
        pipelineOperator()
            .waitForConditionsAndFinish(
                createConfig(info),
                BigQueryRowsCheck.builder(bigQueryResourceManager, table)
                    .setMinRows(MESSAGES_COUNT * 2)
                    .build());

    // Assert
    assertThatResult(reloadedResult).meetsConditions();

    TableResult reloadedRecords = bigQueryResourceManager.readTable(table);

    // Make sure record can be read and UDF changed name to uppercase
    assertThatBigQueryRecords(reloadedRecords)
        .hasRecordsUnordered(List.of(Map.of("id", 11, "job", testName, "name", "lower: message")));
  }
}
