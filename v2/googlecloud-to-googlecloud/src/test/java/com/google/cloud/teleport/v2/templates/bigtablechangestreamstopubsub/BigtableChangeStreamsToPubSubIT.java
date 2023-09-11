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
package com.google.cloud.teleport.v2.templates.bigtablechangestreamstopubsub;

import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.TextNode;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import com.google.cloud.teleport.bigtable.ChangelogEntryMessage;
import com.google.cloud.teleport.bigtable.ChangelogEntryMessageProto;
import com.google.cloud.teleport.bigtable.ChangelogEntryMessageProto.ChangelogEntryProto;
import com.google.cloud.teleport.bigtable.ChangelogEntryMessageText;
import com.google.cloud.teleport.bigtable.ChangelogEntryMessageText.ChangelogEntryText;
import com.google.cloud.teleport.bigtable.ChangelogEntryMessageText.ChangelogEntryText.ModType;
import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.Encoding;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.ReceivedMessage;
import com.google.pubsub.v1.Schema;
import com.google.pubsub.v1.SubscriptionName;
import com.google.pubsub.v1.TopicName;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.time.Duration;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.regex.Pattern;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.beam.it.common.PipelineLauncher.LaunchConfig;
import org.apache.beam.it.common.PipelineLauncher.LaunchInfo;
import org.apache.beam.it.common.PipelineOperator.Config;
import org.apache.beam.it.common.PipelineOperator.Result;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.TemplateTestBase;
import org.apache.beam.it.gcp.artifacts.Artifact;
import org.apache.beam.it.gcp.bigtable.BigtableResourceManager;
import org.apache.beam.it.gcp.bigtable.BigtableResourceManagerUtils;
import org.apache.beam.it.gcp.bigtable.BigtableTableSpec;
import org.apache.beam.it.gcp.pubsub.PubsubResourceManager;
import org.apache.beam.it.gcp.pubsub.conditions.PubsubMessagesCheck;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Before;
import org.junit.ComparisonFailure;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Integration test for {@link BigtableChangeStreamsToPubSub}. */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(BigtableChangeStreamsToPubSub.class)
@RunWith(Parameterized.class)
public final class BigtableChangeStreamsToPubSubIT extends TemplateTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(BigtableChangeStreamsToPubSubIT.class);

  public static final String SOURCE_COLUMN_FAMILY = "cf";
  private static final Duration EXPECTED_REPLICATION_MAX_WAIT_TIME = Duration.ofMinutes(10);
  private BigtableResourceManager bigtableResourceManager;
  private PubsubResourceManager pubsubResourceManager;

  private String clusterName;
  private String appProfileId;
  private TopicName topicName;
  private SubscriptionName subscriptionName;
  private String srcTable;

  @Parameterized.Parameter public Boolean noDlqRetry;

  @Parameterized.Parameters(name = "no-dlq-retry-{0}")
  public static List<Boolean> testParameters() {
    return List.of(true, false);
  }

  @Before
  public void setup() throws IOException {
    pubsubResourceManager =
        PubsubResourceManager.builder(
                removeUnsafeCharacters(testName), PROJECT, credentialsProvider)
            .build();
    bigtableResourceManager =
        BigtableResourceManager.builder(
                removeUnsafeCharacters(testName), PROJECT, credentialsProvider)
            .maybeUseStaticInstance()
            .build();

    appProfileId = generateAppProfileId();

    srcTable = BigtableResourceManagerUtils.generateTableId(testName);

    BigtableTableSpec cdcTableSpec = new BigtableTableSpec();
    cdcTableSpec.setCdcEnabled(true);
    cdcTableSpec.setColumnFamilies(Lists.asList(SOURCE_COLUMN_FAMILY, new String[] {}));
    bigtableResourceManager.createTable(srcTable, cdcTableSpec);

    clusterName = bigtableResourceManager.getClusterNames().iterator().next();
    bigtableResourceManager.createAppProfile(
        appProfileId, true, Collections.singletonList(clusterName));

    topicName = pubsubResourceManager.createTopic("bigtable-cdc-topic");
    subscriptionName = pubsubResourceManager.createSubscription(topicName, "bigtable-cdc-sub");
  }

  @After
  public void tearDownClass() {
    ResourceManagerUtils.cleanResources(bigtableResourceManager, pubsubResourceManager);
  }

  @Test
  public void testJsonNoSchemaCharsetsAndBase64Values() throws Exception {
    LaunchInfo launchInfo =
        launchTemplate(
            LaunchConfig.builder(removeUnsafeCharacters(testName), specPath)
                .addParameter("bigtableReadTableId", srcTable)
                .addParameter("bigtableReadInstanceId", bigtableResourceManager.getInstanceId())
                .addParameter("bigtableChangeStreamAppProfile", appProfileId)
                .addParameter("messageFormat", "JSON")
                .addParameter("messageEncoding", "JSON")
                .addParameter("useBase64Values", "true")
                .addParameter("bigtableChangeStreamCharset", "KOI8-R")
                .addParameter("disableDlqRetries", Boolean.toString(noDlqRetry))
                .addParameter("pubSubTopic", this.topicName.getTopic()));

    assertThatPipeline(launchInfo).isRunning();

    String rowkey = UUID.randomUUID().toString();

    // Russian letter B in KOI8-R
    byte[] columnBytes = new byte[] {(byte) 0xc2};

    String value = UUID.randomUUID().toString();
    long timestamp = 12000L;

    RowMutation rowMutation =
        RowMutation.create(srcTable, rowkey)
            .setCell(
                SOURCE_COLUMN_FAMILY,
                ByteString.copyFrom(columnBytes),
                timestamp,
                ByteString.copyFrom(value, Charset.defaultCharset()));

    ChangelogEntryText expected =
        ChangelogEntryMessageText.ChangelogEntryText.newBuilder()
            .setColumn(new String(columnBytes, Charset.forName("KOI8-R")))
            .setColumnFamily(SOURCE_COLUMN_FAMILY)
            .setIsGC(false)
            .setModType(ModType.SET_CELL)
            .setCommitTimestamp(System.currentTimeMillis() * 1000)
            .setRowKey(rowkey)
            .setSourceInstance(bigtableResourceManager.getInstanceId())
            .setSourceCluster(clusterName)
            .setTieBreaker(1)
            .setTimestamp(timestamp)
            .setSourceTable(srcTable)
            .setValue(Base64.getEncoder().encodeToString(value.getBytes()))
            .build();

    bigtableResourceManager.write(rowMutation);

    List<ReceivedMessage> receivedMessages = getAtLeastOneMessage(launchInfo);
    for (ReceivedMessage message : receivedMessages) {
      validateJsonMessageData(expected, message.getMessage().getData().toString("UTF-8"));
    }
  }

  @Test
  public void testDeadLetterQueueDelivery() throws Exception {
    LaunchInfo launchInfo =
        launchTemplate(
            LaunchConfig.builder(removeUnsafeCharacters(testName), specPath)
                .addParameter("bigtableReadTableId", srcTable)
                .addParameter("bigtableReadInstanceId", bigtableResourceManager.getInstanceId())
                .addParameter("bigtableChangeStreamAppProfile", appProfileId)
                .addParameter("messageFormat", "JSON")
                .addParameter("messageEncoding", "JSON")
                .addParameter("dlqDirectory", getGcsPath("dlq"))
                .addParameter("dlqMaxRetries", "1")
                .addParameter("dlqRetryMinutes", "1")
                .addParameter("disableDlqRetries", Boolean.toString(noDlqRetry))
                .addParameter("pubSubTopic", topicName.getTopic()));

    assertThatPipeline(launchInfo).isRunning();

    String rowkey = UUID.randomUUID().toString();
    String column = "test_column";

    String goodValue = UUID.randomUUID().toString();
    // Making some 15MB value
    String tooBigValue =
        StringUtils.repeat(UUID.randomUUID().toString(), 15 * 1024 * 1024 / goodValue.length());

    long timestamp = 12000L;

    RowMutation tooLargeMutation =
        RowMutation.create(srcTable, rowkey)
            .setCell(SOURCE_COLUMN_FAMILY, column, timestamp, tooBigValue);
    bigtableResourceManager.write(tooLargeMutation);

    RowMutation smallMutation =
        RowMutation.create(srcTable, rowkey)
            .setCell(SOURCE_COLUMN_FAMILY, column, timestamp, goodValue);
    bigtableResourceManager.write(smallMutation);

    ChangelogEntryText expected =
        ChangelogEntryMessageText.ChangelogEntryText.newBuilder()
            .setColumn(column)
            .setColumnFamily(SOURCE_COLUMN_FAMILY)
            .setIsGC(false)
            .setModType(ModType.SET_CELL)
            .setCommitTimestamp(System.currentTimeMillis() * 1000)
            .setRowKey(rowkey)
            .setSourceInstance(bigtableResourceManager.getInstanceId())
            .setSourceCluster(clusterName)
            .setTieBreaker(1)
            .setTimestamp(timestamp)
            .setSourceTable(srcTable)
            .setValue(goodValue)
            .build();

    List<ReceivedMessage> receivedMessages = getAtLeastOneMessage(launchInfo);
    for (ReceivedMessage message : receivedMessages) {
      validateJsonMessageData(expected, message.getMessage().getData().toString("UTF-8"));
    }

    LOG.info("Looking for files in DLQ");

    Result result =
        pipelineOperator()
            .waitForConditionAndFinish(
                createConfig(launchInfo),
                () -> {
                  List<Artifact> artifacts = gcsClient.listArtifacts("dlq", Pattern.compile(".*"));
                  for (Artifact artifact : artifacts) {
                    try {
                      ObjectMapper om = new ObjectMapper();
                      JsonNode severeError = om.readTree(artifact.contents());
                      assertNotNull(severeError);
                      JsonNode errorMessageNode = severeError.get("error_message");
                      assertNotNull(errorMessageNode);
                      assertTrue(errorMessageNode instanceof TextNode);
                      String messageText = errorMessageNode.asText();
                      assertTrue(
                          "Unexpected message text: " + messageText,
                          StringUtils.contains(
                              messageText, "Request payload size exceeds the limit"));
                      return true;
                    } catch (Exception e) {
                      throw new RuntimeException(e);
                    }
                  }
                  return false;
                });

    assertThatResult(result).meetsConditions();
  }

  @Test
  public void testJsonNoSchemaB64RkAndColNoVal() throws Exception {
    LaunchInfo launchInfo =
        launchTemplate(
            LaunchConfig.builder(removeUnsafeCharacters(testName), specPath)
                .addParameter("bigtableReadTableId", srcTable)
                .addParameter("bigtableReadInstanceId", bigtableResourceManager.getInstanceId())
                .addParameter("bigtableChangeStreamAppProfile", appProfileId)
                .addParameter("messageFormat", "JSON")
                .addParameter("messageEncoding", "JSON")
                .addParameter("useBase64Rowkeys", "true")
                .addParameter("useBase64ColumnQualifiers", "true")
                .addParameter("stripValues", "true")
                .addParameter("disableDlqRetries", Boolean.toString(noDlqRetry))
                .addParameter("pubSubTopic", topicName.getTopic()));

    assertThatPipeline(launchInfo).isRunning();

    String rowkey = UUID.randomUUID().toString();
    String column = UUID.randomUUID().toString();
    String value = UUID.randomUUID().toString();
    long timestamp = 12000L;

    RowMutation rowMutation =
        RowMutation.create(srcTable, rowkey)
            .setCell(SOURCE_COLUMN_FAMILY, column, timestamp, value);

    ChangelogEntryText expected =
        ChangelogEntryMessageText.ChangelogEntryText.newBuilder()
            .setColumn(Base64.getEncoder().encodeToString(column.getBytes()))
            .setColumnFamily(SOURCE_COLUMN_FAMILY)
            .setIsGC(false)
            .setModType(ModType.SET_CELL)
            .setCommitTimestamp(System.currentTimeMillis() * 1000)
            .setRowKey(Base64.getEncoder().encodeToString(rowkey.getBytes()))
            .setSourceInstance(bigtableResourceManager.getInstanceId())
            .setSourceCluster(clusterName)
            .setTimestamp(timestamp)
            .setTieBreaker(1)
            .setSourceTable(srcTable)
            .build();

    bigtableResourceManager.write(rowMutation);

    List<ReceivedMessage> receivedMessages = getAtLeastOneMessage(launchInfo);

    for (ReceivedMessage message : receivedMessages) {
      validateJsonMessageData(expected, message.getMessage().getData().toString("UTF-8"));
    }
  }

  private List<ReceivedMessage> getAtLeastOneMessage(LaunchInfo launchInfo) {
    LOG.info("Pulling 1 message from PubSub");
    PubsubMessagesCheck pubsubCheck =
        PubsubMessagesCheck.builder(pubsubResourceManager, subscriptionName)
            .setMinMessages(1)
            .build();

    Result result = pipelineOperator().waitForCondition(createConfig(launchInfo), pubsubCheck);
    assertThatResult(result).meetsConditions();

    List<ReceivedMessage> receivedMessages = pubsubCheck.getReceivedMessageList();
    LOG.info("Pulled messages: {}", receivedMessages);
    return receivedMessages;
  }

  @Test
  public void testJsonWithSchemaCharsetAndB64Val() throws Exception {
    pubsubResourceManager.createSchema(
        Schema.Type.PROTOCOL_BUFFER,
        "syntax = \"proto2\";\n"
            + "\n"
            + "package com.google.cloud.teleport.bigtable;\n"
            + "\n"
            + "option java_outer_classname = \"ChangelogEntryMessageText\";\n"
            + "\n"
            + "message ChangelogEntryText{\n"
            + "  required string rowKey = 1;\n"
            + "  enum ModType {\n"
            + "    SET_CELL = 0;\n"
            + "    DELETE_FAMILY = 1;\n"
            + "    DELETE_CELLS = 2;\n"
            + "    UNKNOWN = 3;\n"
            + "  }\n"
            + "  required ModType modType = 2;\n"
            + "  required bool isGC = 3;\n"
            + "  required int32 tieBreaker = 4;\n"
            + "  required int64 commitTimestamp = 5;\n"
            + "  required string columnFamily = 6;\n"
            + "  optional string column = 7;\n"
            + "  optional int64 timestamp = 8;\n"
            + "  optional int64 timestampFrom = 9;\n"
            + "  optional int64 timestampTo = 10;\n"
            + "  optional string value = 11;\n"
            + "  required string sourceInstance = 12;\n"
            + "  required string sourceCluster = 13;\n"
            + "  required string sourceTable = 14;\n"
            + "}",
        Encoding.JSON,
        topicName);

    LaunchInfo launchInfo =
        launchTemplate(
            LaunchConfig.builder(removeUnsafeCharacters(testName), specPath)
                .addParameter("bigtableReadTableId", srcTable)
                .addParameter("bigtableReadInstanceId", bigtableResourceManager.getInstanceId())
                .addParameter("bigtableChangeStreamAppProfile", appProfileId)
                .addParameter("messageFormat", "JSON")
                .addParameter("messageEncoding", "JSON")
                .addParameter("useBase64Values", "true")
                .addParameter("bigtableChangeStreamCharset", "KOI8-R")
                .addParameter("disableDlqRetries", Boolean.toString(noDlqRetry))
                .addParameter("pubSubTopic", topicName.getTopic()));

    assertThatPipeline(launchInfo).isRunning();

    String rowkey = UUID.randomUUID().toString();

    // Russian letter B in KOI8-R
    byte[] columnBytes = new byte[] {(byte) 0xc2};

    String value = UUID.randomUUID().toString();
    long timestamp = 12000L;

    RowMutation rowMutation =
        RowMutation.create(srcTable, rowkey)
            .setCell(
                SOURCE_COLUMN_FAMILY,
                ByteString.copyFrom(columnBytes),
                timestamp,
                ByteString.copyFrom(value, Charset.defaultCharset()));

    ChangelogEntryText expected =
        ChangelogEntryMessageText.ChangelogEntryText.newBuilder()
            .setColumn(new String(columnBytes, Charset.forName("KOI8-R")))
            .setColumnFamily(SOURCE_COLUMN_FAMILY)
            .setIsGC(false)
            .setModType(ModType.SET_CELL)
            .setCommitTimestamp(System.currentTimeMillis() * 1000)
            .setRowKey(rowkey)
            .setSourceInstance(bigtableResourceManager.getInstanceId())
            .setSourceCluster(clusterName)
            .setTieBreaker(1)
            .setTimestamp(timestamp)
            .setSourceTable(srcTable)
            .setValue(Base64.getEncoder().encodeToString(value.getBytes()))
            .build();

    bigtableResourceManager.write(rowMutation);

    List<ReceivedMessage> receivedMessages = getAtLeastOneMessage(launchInfo);

    for (ReceivedMessage message : receivedMessages) {
      validateJsonMessageData(expected, message.getMessage().getData().toString("UTF-8"));
    }
  }

  @Test
  public void testProtoWithSchema() throws Exception {
    pubsubResourceManager.createSchema(
        Schema.Type.PROTOCOL_BUFFER,
        "syntax = \"proto2\";\n"
            + "\n"
            + "package com.google.cloud.teleport.bigtable;\n"
            + "\n"
            + "option java_outer_classname = \"ChangelogEntryMessageProto\";\n"
            + "\n"
            + "message ChangelogEntryProto{\n"
            + "  required bytes rowKey = 1;\n"
            + "  enum ModType {\n"
            + "    SET_CELL = 0;\n"
            + "    DELETE_FAMILY = 1;\n"
            + "    DELETE_CELLS = 2;\n"
            + "    UNKNOWN = 3;\n"
            + "  }\n"
            + "  required ModType modType = 2;\n"
            + "  required bool isGC = 3;\n"
            + "  required int32 tieBreaker = 4;\n"
            + "  required int64 commitTimestamp = 5;\n"
            + "  required string columnFamily = 6;\n"
            + "  optional bytes column = 7;\n"
            + "  optional int64 timestamp = 8;\n"
            + "  optional int64 timestampFrom = 9;\n"
            + "  optional int64 timestampTo = 10;\n"
            + "  optional bytes value = 11;\n"
            + "  required string sourceInstance = 12;\n"
            + "  required string sourceCluster = 13;\n"
            + "  required string sourceTable = 14;\n"
            + "}",
        Encoding.BINARY,
        topicName);

    LaunchInfo launchInfo =
        launchTemplate(
            LaunchConfig.builder(removeUnsafeCharacters(testName), specPath)
                .addParameter("bigtableReadTableId", srcTable)
                .addParameter("bigtableReadInstanceId", bigtableResourceManager.getInstanceId())
                .addParameter("bigtableChangeStreamAppProfile", appProfileId)
                .addParameter("disableDlqRetries", Boolean.toString(noDlqRetry))
                .addParameter("pubSubTopic", topicName.getTopic()));

    assertThatPipeline(launchInfo).isRunning();

    String rowkey = UUID.randomUUID().toString();
    String column = UUID.randomUUID().toString();
    String value = UUID.randomUUID().toString();
    long timestamp = 12000L;

    RowMutation rowMutation =
        RowMutation.create(srcTable, rowkey)
            .setCell(SOURCE_COLUMN_FAMILY, column, timestamp, value);

    ChangelogEntryProto expected =
        ChangelogEntryMessageProto.ChangelogEntryProto.newBuilder()
            .setColumn(ByteString.copyFrom(column, Charset.defaultCharset()))
            .setColumnFamily(SOURCE_COLUMN_FAMILY)
            .setIsGC(false)
            .setModType(ChangelogEntryProto.ModType.SET_CELL)
            .setCommitTimestamp(System.currentTimeMillis() * 1000)
            .setRowKey(ByteString.copyFrom(rowkey, Charset.defaultCharset()))
            .setSourceInstance(bigtableResourceManager.getInstanceId())
            .setSourceCluster(clusterName)
            .setTieBreaker(1)
            .setTimestamp(timestamp)
            .setSourceTable(srcTable)
            .setValue(ByteString.copyFrom(value, Charset.defaultCharset()))
            .build();

    bigtableResourceManager.write(rowMutation);

    List<ReceivedMessage> receivedMessages = getAtLeastOneMessage(launchInfo);

    for (ReceivedMessage message : receivedMessages) {
      validateProtoMessageData(expected, message.getMessage().getData());
    }
  }

  @Test
  public void testProtoNoSchemaNoVal() throws Exception {
    LaunchInfo launchInfo =
        launchTemplate(
            LaunchConfig.builder(removeUnsafeCharacters(testName), specPath)
                .addParameter("bigtableReadTableId", srcTable)
                .addParameter("bigtableReadInstanceId", bigtableResourceManager.getInstanceId())
                .addParameter("bigtableChangeStreamAppProfile", appProfileId)
                .addParameter("messageFormat", "PROTOCOL_BUFFERS")
                .addParameter("messageEncoding", "BINARY")
                .addParameter("stripValues", "true")
                .addParameter("disableDlqRetries", Boolean.toString(noDlqRetry))
                .addParameter("pubSubTopic", topicName.getTopic()));

    assertThatPipeline(launchInfo).isRunning();

    String rowkey = UUID.randomUUID().toString();
    String column = UUID.randomUUID().toString();
    String value = UUID.randomUUID().toString();
    long timestamp = 12000L;

    RowMutation rowMutation =
        RowMutation.create(srcTable, rowkey)
            .setCell(SOURCE_COLUMN_FAMILY, column, timestamp, value);

    ChangelogEntryProto expected =
        ChangelogEntryMessageProto.ChangelogEntryProto.newBuilder()
            .setColumn(ByteString.copyFrom(column, Charset.defaultCharset()))
            .setColumnFamily(SOURCE_COLUMN_FAMILY)
            .setIsGC(false)
            .setModType(ChangelogEntryProto.ModType.SET_CELL)
            .setCommitTimestamp(System.currentTimeMillis() * 1000)
            .setRowKey(ByteString.copyFrom(rowkey, Charset.defaultCharset()))
            .setSourceInstance(bigtableResourceManager.getInstanceId())
            .setSourceCluster(clusterName)
            .setTieBreaker(1)
            .setTimestamp(timestamp)
            .setSourceTable(srcTable)
            .build();

    bigtableResourceManager.write(rowMutation);

    List<ReceivedMessage> receivedMessages = getAtLeastOneMessage(launchInfo);

    for (ReceivedMessage message : receivedMessages) {
      validateProtoMessageData(expected, message.getMessage().getData());
    }
  }

  @Test
  public void testAvroWithSchema() throws Exception {
    pubsubResourceManager.createSchema(
        Schema.Type.AVRO,
        "{\n"
            + "    \"name\" : \"ChangelogEntryMessage\",\n"
            + "    \"type\" : \"record\",\n"
            + "    \"namespace\" : \"com.google.cloud.teleport.bigtable\",\n"
            + "    \"fields\" : [\n"
            + "      { \"name\" : \"rowKey\", \"type\" : \"bytes\"},\n"
            + "      {\n"
            + "        \"name\" : \"modType\",\n"
            + "        \"type\" : {\n"
            + "          \"name\": \"ModType\",\n"
            + "          \"type\": \"enum\",\n"
            + "          \"symbols\": [\"SET_CELL\", \"DELETE_FAMILY\", \"DELETE_CELLS\", \"UNKNOWN\"]}\n"
            + "      },\n"
            + "      { \"name\": \"isGC\", \"type\": \"boolean\" },\n"
            + "      { \"name\": \"tieBreaker\", \"type\": \"int\"},\n"
            + "      { \"name\": \"columnFamily\", \"type\": \"string\"},\n"
            + "      { \"name\": \"commitTimestamp\", \"type\" : \"long\"},\n"
            + "      { \"name\" : \"sourceInstance\", \"type\" : \"string\"},\n"
            + "      { \"name\" : \"sourceCluster\", \"type\" : \"string\"},\n"
            + "      { \"name\" : \"sourceTable\", \"type\" : \"string\"},\n"
            + "      { \"name\": \"column\", \"type\" : [\"null\",\"bytes\"]},\n"
            + "      { \"name\": \"timestamp\", \"type\" : [\"null\", \"long\"]},\n"
            + "      { \"name\": \"timestampFrom\", \"type\" : [\"null\", \"long\"]},\n"
            + "      { \"name\": \"timestampTo\", \"type\" : [\"null\", \"long\"]},\n"
            + "      { \"name\" : \"value\", \"type\" : [\"null\", \"bytes\"]}\n"
            + "   ]\n"
            + "}",
        Encoding.BINARY,
        topicName);

    LaunchInfo launchInfo =
        launchTemplate(
            LaunchConfig.builder(removeUnsafeCharacters(testName), specPath)
                .addParameter("bigtableReadTableId", srcTable)
                .addParameter("bigtableReadInstanceId", bigtableResourceManager.getInstanceId())
                .addParameter("bigtableChangeStreamAppProfile", appProfileId)
                .addParameter("disableDlqRetries", Boolean.toString(noDlqRetry))
                .addParameter("pubSubTopic", topicName.getTopic()));

    assertThatPipeline(launchInfo).isRunning();

    String rowkey = UUID.randomUUID().toString();
    String column = UUID.randomUUID().toString();
    String value = UUID.randomUUID().toString();
    long timestamp = 12000L;

    RowMutation rowMutation =
        RowMutation.create(srcTable, rowkey)
            .setCell(SOURCE_COLUMN_FAMILY, column, timestamp, value);

    ChangelogEntryMessage expected =
        ChangelogEntryMessage.newBuilder()
            .setColumn(ByteBuffer.wrap(column.getBytes(Charset.defaultCharset())))
            .setColumnFamily(SOURCE_COLUMN_FAMILY)
            .setIsGC(false)
            .setModType(com.google.cloud.teleport.bigtable.ModType.SET_CELL)
            .setCommitTimestamp(System.currentTimeMillis() * 1000)
            .setRowKey(ByteBuffer.wrap(rowkey.getBytes(Charset.defaultCharset())))
            .setSourceInstance(bigtableResourceManager.getInstanceId())
            .setSourceCluster(clusterName)
            .setTieBreaker(1)
            .setTimestamp(timestamp)
            .setTimestampFrom(null)
            .setTimestampTo(null)
            .setSourceTable(srcTable)
            .setValue(ByteBuffer.wrap(value.getBytes(Charset.defaultCharset())))
            .build();

    bigtableResourceManager.write(rowMutation);

    List<ReceivedMessage> receivedMessages = getAtLeastOneMessage(launchInfo);

    for (ReceivedMessage message : receivedMessages) {
      validateAvroMessageData(expected, message.getMessage());
    }
  }

  @Test
  public void testAvroWithNoSchemaNoVal() throws Exception {
    String rowkey = UUID.randomUUID().toString();
    String column = UUID.randomUUID().toString();
    String value = UUID.randomUUID().toString();
    long timestamp = 12000L;

    ChangelogEntryMessage expected =
        ChangelogEntryMessage.newBuilder()
            .setColumn(ByteBuffer.wrap(column.getBytes(Charset.defaultCharset())))
            .setColumnFamily(SOURCE_COLUMN_FAMILY)
            .setIsGC(false)
            .setModType(com.google.cloud.teleport.bigtable.ModType.SET_CELL)
            .setCommitTimestamp(System.currentTimeMillis() * 1000)
            .setRowKey(ByteBuffer.wrap(rowkey.getBytes(Charset.defaultCharset())))
            .setSourceInstance(bigtableResourceManager.getInstanceId())
            .setSourceCluster(clusterName)
            .setTieBreaker(1)
            .setTimestamp(timestamp)
            .setTimestampFrom(null)
            .setTimestampTo(null)
            .setSourceTable(srcTable)
            .setValue(null)
            .build();

    LaunchInfo launchInfo =
        launchTemplate(
            LaunchConfig.builder(removeUnsafeCharacters(testName), specPath)
                .addParameter("bigtableReadTableId", srcTable)
                .addParameter("bigtableReadInstanceId", bigtableResourceManager.getInstanceId())
                .addParameter("bigtableChangeStreamAppProfile", appProfileId)
                .addParameter("messageFormat", "AVRO")
                .addParameter("messageEncoding", "BINARY")
                .addParameter("stripValues", "true")
                .addParameter("disableDlqRetries", Boolean.toString(noDlqRetry))
                .addParameter("pubSubTopic", topicName.getTopic()));

    assertThatPipeline(launchInfo).isRunning();

    RowMutation rowMutation =
        RowMutation.create(srcTable, rowkey)
            .setCell(SOURCE_COLUMN_FAMILY, column, timestamp, value);

    bigtableResourceManager.write(rowMutation);

    List<ReceivedMessage> receivedMessages = getAtLeastOneMessage(launchInfo);

    for (ReceivedMessage message : receivedMessages) {
      validateAvroMessageData(expected, message.getMessage());
    }
  }

  private void validateAvroMessageData(ChangelogEntryMessage expected, PubsubMessage message)
      throws IOException {
    ByteArrayInputStream input = new ByteArrayInputStream(message.getData().toByteArray());
    Decoder decoder = DecoderFactory.get().directBinaryDecoder(input, /* reuse= */ null);

    SpecificDatumReader<ChangelogEntryMessage> reader =
        new SpecificDatumReader<>(ChangelogEntryMessage.getClassSchema());
    ChangelogEntryMessage received = reader.read(null, decoder);

    assertEquals(expected.getRowKey(), received.getRowKey());
    assertEquals(expected.getModType(), received.getModType());
    assertEquals(expected.getIsGC(), received.getIsGC());
    assertTrue(received.getTieBreaker() >= 0);
    assertTrue(expected.getCommitTimestamp() - 10000000L <= received.getCommitTimestamp());
    assertEqualCharSequences(expected.getColumnFamily(), received.getColumnFamily());
    assertEquals(expected.getColumn(), received.getColumn());
    assertEquals(expected.getTimestamp(), received.getTimestamp());
    assertEquals(expected.getValue(), received.getValue());
    assertEqualCharSequences(expected.getSourceInstance(), received.getSourceInstance());
    assertEqualCharSequences(expected.getSourceCluster(), received.getSourceCluster());
    assertEqualCharSequences(expected.getSourceTable(), received.getSourceTable());
  }

  private void assertEqualCharSequences(CharSequence expected, CharSequence actual) {
    if ((expected == null && actual != null) || (actual == null && expected != null)) {
      throw new ComparisonFailure(
          "Values are not the same", Objects.toString(expected), Objects.toString(actual));
    }
    if (expected != null) {
      assertEquals(expected.toString(), actual.toString());
    }
  }

  private void validateProtoMessageData(ChangelogEntryProto expected, ByteString data)
      throws IOException {
    ChangelogEntryProto received = ChangelogEntryMessageProto.ChangelogEntryProto.parseFrom(data);
    assertEquals(expected.getRowKey(), received.getRowKey());
    assertEquals(expected.getModType(), received.getModType());
    assertEquals(expected.getIsGC(), received.getIsGC());
    assertTrue(received.getTieBreaker() >= 0);
    assertTrue(expected.getCommitTimestamp() - 10000000L <= received.getCommitTimestamp());
    assertEquals(expected.getColumnFamily(), received.getColumnFamily());
    assertEquals(expected.getColumn(), received.getColumn());
    assertEquals(expected.getTimestamp(), received.getTimestamp());
    assertEquals(expected.getValue(), received.getValue());
    assertEquals(expected.getSourceInstance(), received.getSourceInstance());
    assertEquals(expected.getSourceCluster(), received.getSourceCluster());
    assertEquals(expected.getSourceTable(), received.getSourceTable());
  }

  private void validateJsonMessageData(ChangelogEntryText expected, String jsonString)
      throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    JsonNode jsonTree = mapper.readTree(jsonString);

    assertEquals(expected.getRowKey(), jsonTree.get("rowKey").asText());
    assertEquals(expected.getModType().name(), jsonTree.get("modType").asText());
    assertEquals(expected.getIsGC(), jsonTree.get("isGC").asBoolean());
    assertTrue(jsonTree.get("tieBreaker").asLong() >= 0);
    assertTrue(
        expected.getCommitTimestamp() - 10000000L
            <= Long.parseLong(jsonTree.get("commitTimestamp").asText()));
    assertEquals(expected.getColumnFamily(), jsonTree.get("columnFamily").asText());
    assertEquals(expected.getColumn(), jsonTree.get("column").asText());
    assertEquals(expected.getTimestamp(), Long.parseLong(jsonTree.get("timestamp").asText()));
    JsonNode valueNode = jsonTree.get("value");
    if (expected.hasValue()) {
      assertEquals(expected.getValue(), valueNode.asText());
    } else {
      assertNull(valueNode);
    }
    assertEquals(expected.getSourceInstance(), jsonTree.get("sourceInstance").asText());
    assertEquals(expected.getSourceCluster(), jsonTree.get("sourceCluster").asText());
    assertEquals(expected.getSourceTable(), jsonTree.get("sourceTable").asText());
  }

  @NotNull
  private static String generateAppProfileId() {
    return "cdc_app_profile_" + System.nanoTime();
  }

  private String removeUnsafeCharacters(String testName) {
    return testName.replaceAll("[\\[\\]]", "-");
  }

  @Override
  protected Config.Builder wrapConfiguration(Config.Builder builder) {

    // For DirectRunner tests, reduce the max time and the interval, as there is no worker required
    if (System.getProperty("directRunnerTest") != null) {
      builder =
          builder
              .setTimeoutAfter(EXPECTED_REPLICATION_MAX_WAIT_TIME.minus(Duration.ofMinutes(3)))
              .setCheckAfter(Duration.ofSeconds(5));
    } else {
      builder = builder.setTimeoutAfter(EXPECTED_REPLICATION_MAX_WAIT_TIME);
    }

    return builder;
  }
}
