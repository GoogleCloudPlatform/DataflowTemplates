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

import static org.apache.beam.it.gcp.bigquery.matchers.BigQueryAsserts.assertThatBigQueryRecords;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;
import static org.apache.commons.lang3.RandomStringUtils.randomAlphabetic;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.teleport.metadata.DirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.cloud.teleport.v2.proto.testing.Address;
import com.google.cloud.teleport.v2.proto.testing.Tier;
import com.google.cloud.teleport.v2.proto.testing.User;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.SubscriptionName;
import com.google.pubsub.v1.TopicName;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.beam.it.common.PipelineLauncher.LaunchConfig;
import org.apache.beam.it.common.PipelineLauncher.LaunchInfo;
import org.apache.beam.it.common.PipelineOperator.Result;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.conditions.ConditionCheck;
import org.apache.beam.it.gcp.TemplateTestBase;
import org.apache.beam.it.gcp.bigquery.BigQueryResourceManager;
import org.apache.beam.it.gcp.bigquery.conditions.BigQueryRowsCheck;
import org.apache.beam.it.gcp.pubsub.PubsubResourceManager;
import org.apache.beam.it.gcp.pubsub.conditions.PubsubMessagesCheck;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Integration test for {@link PubsubProtoToBigQuery} Flex template. */
@Category(TemplateIntegrationTest.class)
@TemplateIntegrationTest(PubsubProtoToBigQuery.class)
@RunWith(JUnit4.class)
public final class PubsubProtoToBigQueryIT extends TemplateTestBase {

  private static final String TEST_DIR_PREFIX = "PubSubProtoToBigQueryIT/";
  private static final String PROTO_DIR_PREFIX = "generated-test-sources/protobuf/schema/";
  private static final String PROTO_SCHEMA = "schema.pb";
  private static final String BIGQUERY_SCHEMA = "BigQuerySchema.json";
  private static final String BIGQUERY_PRESERVED_SCHEMA = "BigQuerySchemaPreserved.json";

  private PubsubResourceManager pubsubResourceManager;
  private BigQueryResourceManager bigQueryResourceManager;

  private static final int GOOD_MESSAGES_COUNT = 10;
  private static final int BAD_PUB_SUB_MESSAGES_COUNT = 3;
  private static final int BAD_UDF_MESSAGES_COUNT = 3;

  @Before
  public void setUp() throws IOException {
    pubsubResourceManager =
        PubsubResourceManager.builder(testName, PROJECT, credentialsProvider).build();
    bigQueryResourceManager =
        BigQueryResourceManager.builder(testName, PROJECT, credentials).build();

    gcsClient.uploadArtifact(PROTO_SCHEMA, Paths.get("target", PROTO_DIR_PREFIX + PROTO_SCHEMA));

    gcsClient.createArtifact(
        "udf.js",
        "function uppercaseName(value) {\n"
            + "  const data = JSON.parse(value);\n"
            + "  if (data.name == \"INVALID\") {\n"
            + "    throw 'Invalid name!';\n"
            + "  }\n"
            + "  data.name = data.name.toUpperCase();\n"
            + "  return JSON.stringify(data);\n"
            + "}");

    gcsClient.createArtifact(
        "pyudf.py",
        "import json\n"
            + "def uppercaseName(value):\n"
            + "  data = json.loads(value)\n"
            + "  if data['name'] == \"INVALID\":\n"
            + "    raise RuntimeError(\"Invalid name!\")\n"
            + "  data['name'] = data['name'].upper()\n"
            + "  return json.dumps(data)");
  }

  @After
  public void cleanUp() {
    ResourceManagerUtils.cleanResources(pubsubResourceManager, bigQueryResourceManager);
  }

  @Test
  @TemplateIntegrationTest(
      value = PubsubProtoToBigQuery.class,
      template = "PubSub_Proto_to_BigQuery_Flex")
  @Category(DirectRunnerTest.class)
  public void pubSubProtoToBigQueryInferredSchema() throws IOException {
    basePubSubProtoToBigQuery(
        b ->
            b.addParameter("javascriptTextTransformGcsPath", getGcsPath("udf.js"))
                .addParameter("javascriptTextTransformFunctionName", "uppercaseName"),
        false);
  }

  @Test
  @TemplateIntegrationTest(
      value = PubsubProtoToBigQuery.class,
      template = "PubSub_Proto_to_BigQuery_Flex")
  @Category(DirectRunnerTest.class)
  public void pubSubProtoToBigQueryPreserveProtoNames() throws IOException {
    gcsClient.uploadArtifact(
        BIGQUERY_PRESERVED_SCHEMA,
        Resources.getResource(TEST_DIR_PREFIX + BIGQUERY_PRESERVED_SCHEMA).getPath());

    basePubSubProtoToBigQuery(
        b ->
            b.addParameter("preserveProtoFieldNames", "true")
                .addParameter("bigQueryTableSchemaPath", getGcsPath(BIGQUERY_PRESERVED_SCHEMA))
                .addParameter("javascriptTextTransformGcsPath", getGcsPath("udf.js"))
                .addParameter("javascriptTextTransformFunctionName", "uppercaseName"),
        true);
  }

  @Test
  @TemplateIntegrationTest(
      value = PubsubProtoToBigQuery.class,
      template = "PubSub_Proto_to_BigQuery_Flex")
  public void pubSubProtoToBigQueryUdfCustomOutputTopic() throws IOException {
    gcsClient.uploadArtifact(
        BIGQUERY_SCHEMA, Resources.getResource(TEST_DIR_PREFIX + BIGQUERY_SCHEMA).getPath());

    basePubSubProtoToBigQuery(
        b ->
            b.addParameter("bigQueryTableSchemaPath", getGcsPath(BIGQUERY_SCHEMA))
                .addParameter("javascriptTextTransformGcsPath", getGcsPath("udf.js"))
                .addParameter("javascriptTextTransformFunctionName", "uppercaseName"),
        false);
  }

  @Test
  @TemplateIntegrationTest(
      value = PubsubProtoToBigQuery.class,
      template = "PubSub_Proto_to_BigQuery_Xlang")
  public void pubSubProtoToBigQueryWithPythonUdf() throws IOException {
    basePubSubProtoToBigQuery(
        b ->
            b.addParameter("pythonExternalTextTransformGcsPath", getGcsPath("pyudf.py"))
                .addParameter("pythonExternalTextTransformFunctionName", "uppercaseName"),
        false);
  }

  private void basePubSubProtoToBigQuery(
      Function<LaunchConfig.Builder, LaunchConfig.Builder> paramsAdder, boolean preserved)
      throws IOException {

    // Arrange
    // Create BigQuery dataset and table
    bigQueryResourceManager.createDataset(REGION);
    Schema bqSchema = generateBigQuerySchema(preserved);
    TableId table = bigQueryResourceManager.createTable(testName, bqSchema);

    // Create PubSub topics and subscriptions
    TopicName inputTopic = pubsubResourceManager.createTopic("input");
    TopicName badProtoOutputTopic = pubsubResourceManager.createTopic("proto-output");
    TopicName badUdfOutputTopic = pubsubResourceManager.createTopic("udf-output");
    SubscriptionName inputSubscription =
        pubsubResourceManager.createSubscription(inputTopic, "sub-1");
    SubscriptionName badProtoOutputSubscription =
        pubsubResourceManager.createSubscription(badProtoOutputTopic, "sub-2");
    SubscriptionName badUdfOutputSubscription =
        pubsubResourceManager.createSubscription(badUdfOutputTopic, "sub-3");

    // Act
    LaunchInfo info =
        launchTemplate(
            paramsAdder.apply(
                LaunchConfig.builder(testName, specPath)
                    .addParameter("protoSchemaPath", getGcsPath(PROTO_SCHEMA))
                    .addParameter("fullMessageName", User.class.getName())
                    .addParameter("inputSubscription", inputSubscription.toString())
                    .addParameter("outputTopic", badProtoOutputTopic.toString())
                    .addParameter("udfOutputTopic", badUdfOutputTopic.toString())
                    .addParameter("outputTableSpec", toTableSpecLegacy(table))));
    assertThatPipeline(info).isRunning();

    // Generate good proto messages
    List<Map<String, Object>> expectedMessages = new ArrayList<>();
    for (User user : generateUserMessages(GOOD_MESSAGES_COUNT)) {
      pubsubResourceManager.publish(inputTopic, ImmutableMap.of(), user.toByteString());
      Map<String, Object> expectedMessage =
          new HashMap<>(
              Map.of(
                  "name",
                  user.getName().toUpperCase(),
                  "tier",
                  user.getTier().name(),
                  "email",
                  user.getEmail(),
                  "address",
                  Map.of(
                      "street",
                      user.getAddress().getStreet(),
                      preserved ? "apartment_number" : "apartmentNumber",
                      user.getAddress().getApartmentNumber(),
                      "city",
                      user.getAddress().getCity(),
                      "state",
                      user.getAddress().getState(),
                      preserved ? "zip_code" : "zipCode",
                      user.getAddress().getZipCode())));
      expectedMessage.put(preserved ? "phone_number" : "phoneNumber", null);
      expectedMessages.add(expectedMessage);
    }

    // Generate proto messages that cannot be parsed
    for (int i = 0; i < BAD_PUB_SUB_MESSAGES_COUNT; i++) {
      pubsubResourceManager.publish(
          inputTopic, ImmutableMap.of(), ByteString.copyFromUtf8("bad id " + i));
    }

    // Generate proto messages that will fail at UDF step
    for (User user : generateUserMessages(BAD_UDF_MESSAGES_COUNT)) {
      user = user.toBuilder().setName("INVALID").build();
      pubsubResourceManager.publish(inputTopic, ImmutableMap.of(), user.toByteString());
    }

    // Checks for BigQuery output table
    ConditionCheck bigQueryTableCheck =
        BigQueryRowsCheck.builder(bigQueryResourceManager, table)
            .setMinRows(GOOD_MESSAGES_COUNT)
            .build();

    // Check for PubSub bad proto format output topic
    ConditionCheck pubSubBadProtoTopicCheck =
        PubsubMessagesCheck.builder(pubsubResourceManager, badProtoOutputSubscription)
            .setMinMessages(BAD_PUB_SUB_MESSAGES_COUNT)
            .build();

    // Check for PubSub bad UDF custom output topic
    ConditionCheck pubSubBadUdfTopicCheck =
        PubsubMessagesCheck.builder(pubsubResourceManager, badUdfOutputSubscription)
            .setMinMessages(BAD_UDF_MESSAGES_COUNT)
            .build();

    Result result =
        pipelineOperator()
            .waitForConditionsAndFinish(
                createConfig(info),
                bigQueryTableCheck,
                pubSubBadProtoTopicCheck,
                pubSubBadUdfTopicCheck);

    // Assert
    assertThatResult(result).meetsConditions();

    TableResult records = bigQueryResourceManager.readTable(table);

    // Make sure record can be read and UDF changed name to uppercase
    assertThatBigQueryRecords(records).hasRecordsUnordered(expectedMessages);
  }

  private static List<User> generateUserMessages(int numMessages) {
    List<User> users = new ArrayList<>();
    for (int i = 1; i <= numMessages; i++) {
      User user =
          User.newBuilder()
              .setName(randomAlphabetic(8, 20).toLowerCase())
              .setTier(i % 2 == 0 ? Tier.FREE_TIER : Tier.PREMIUM_TIER)
              .setEmail(randomAlphabetic(8, 20))
              .setAddress(
                  Address.newBuilder()
                      .setStreet(randomAlphabetic(8, 20))
                      .setApartmentNumber(i)
                      .setCity(randomAlphabetic(8, 20))
                      .setState(randomAlphabetic(2))
                      .setZipCode(i)
                      .build())
              .build();
      users.add(user);
    }
    return users;
  }

  private static Schema generateBigQuerySchema(boolean preserved) {
    return Schema.of(
        Arrays.asList(
            Field.of("name", StandardSQLTypeName.STRING),
            Field.of("tier", StandardSQLTypeName.STRING),
            Field.newBuilder("email", StandardSQLTypeName.STRING)
                .setMode(Field.Mode.NULLABLE)
                .build(),
            Field.newBuilder(preserved ? "phone_number" : "phoneNumber", StandardSQLTypeName.STRING)
                .setMode(Field.Mode.NULLABLE)
                .build(),
            Field.newBuilder(
                    "address",
                    LegacySQLTypeName.RECORD,
                    Field.newBuilder("street", StandardSQLTypeName.STRING)
                        .setMode(Field.Mode.NULLABLE)
                        .build(),
                    Field.newBuilder(
                            preserved ? "apartment_number" : "apartmentNumber",
                            StandardSQLTypeName.INT64)
                        .setMode(Field.Mode.NULLABLE)
                        .build(),
                    Field.newBuilder("city", StandardSQLTypeName.STRING)
                        .setMode(Field.Mode.NULLABLE)
                        .build(),
                    Field.newBuilder("state", StandardSQLTypeName.STRING)
                        .setMode(Field.Mode.NULLABLE)
                        .build(),
                    Field.newBuilder(preserved ? "zip_code" : "zipCode", StandardSQLTypeName.INT64)
                        .setMode(Field.Mode.NULLABLE)
                        .build())
                .build()));
  }
}
