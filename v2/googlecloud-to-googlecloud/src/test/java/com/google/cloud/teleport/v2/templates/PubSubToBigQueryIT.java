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

import static com.google.cloud.teleport.it.common.matchers.TemplateAsserts.assertThatPipeline;
import static com.google.cloud.teleport.it.common.matchers.TemplateAsserts.assertThatResult;
import static com.google.cloud.teleport.it.gcp.bigquery.matchers.BigQueryAsserts.assertThatBigQueryRecords;
import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.teleport.it.common.PipelineLauncher.LaunchConfig;
import com.google.cloud.teleport.it.common.PipelineLauncher.LaunchInfo;
import com.google.cloud.teleport.it.common.PipelineOperator.Result;
import com.google.cloud.teleport.it.common.utils.ResourceManagerUtils;
import com.google.cloud.teleport.it.gcp.TemplateTestBase;
import com.google.cloud.teleport.it.gcp.bigquery.DefaultBigQueryResourceManager;
import com.google.cloud.teleport.it.gcp.bigquery.conditions.BigQueryRowsCheck;
import com.google.cloud.teleport.it.gcp.pubsub.DefaultPubsubResourceManager;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.SubscriptionName;
import com.google.pubsub.v1.TopicName;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
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
public final class PubSubToBigQueryIT extends TemplateTestBase {

  private DefaultPubsubResourceManager pubsubResourceManager;
  private DefaultBigQueryResourceManager bigQueryResourceManager;

  private static final int MESSAGES_COUNT = 10;
  private static final int BAD_MESSAGES_COUNT = 3;

  @Before
  public void setUp() throws IOException {
    pubsubResourceManager =
        DefaultPubsubResourceManager.builder(testName, PROJECT)
            .credentialsProvider(credentialsProvider)
            .build();
    bigQueryResourceManager =
        DefaultBigQueryResourceManager.builder(testName, PROJECT)
            .setCredentials(credentials)
            .build();

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
  public void testPubsubToBigQuery() throws IOException {
    basePubsubToBigQuery(Function.identity()); // no extra parameters
  }

  @Test
  public void testPubsubToBigQueryWithStorageApi() throws IOException {
    basePubsubToBigQuery(
        b ->
            b.addParameter("useStorageWriteApi", "true")
                .addParameter("numStorageWriteApiStreams", "1")
                .addParameter("storageWriteApiTriggeringFrequencySec", "5"));
  }

  private void basePubsubToBigQuery(
      Function<LaunchConfig.Builder, LaunchConfig.Builder> paramsAdder) throws IOException {
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
}
