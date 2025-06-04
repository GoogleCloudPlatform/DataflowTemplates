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
package com.google.cloud.teleport.v2.templates;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;
import static org.apache.commons.lang3.RandomStringUtils.randomAlphanumeric;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.TextNode;
import com.google.cloud.bigtable.admin.v2.models.StorageType;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import com.google.cloud.bigtable.data.v2.models.TableId;
import com.google.cloud.teleport.bigtable.ChangelogEntryMessage;
import com.google.cloud.teleport.bigtable.ChangelogEntryMessageBase64;
import com.google.cloud.teleport.bigtable.ModType;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.cloud.teleport.v2.kafka.values.KafkaTemplateParameters;
import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineLauncher.LaunchInfo;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.PipelineOperator.Result;
import org.apache.beam.it.common.TestProperties;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.TemplateTestBase;
import org.apache.beam.it.gcp.artifacts.Artifact;
import org.apache.beam.it.gcp.bigtable.BigtableResourceManager;
import org.apache.beam.it.gcp.bigtable.BigtableResourceManagerCluster;
import org.apache.beam.it.gcp.bigtable.BigtableTableSpec;
import org.apache.beam.it.kafka.KafkaResourceManager;
import org.apache.beam.sdk.util.RowJsonUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Integration test for {@link BigtableChangeStreamsToKafka}. */
@TemplateIntegrationTest(BigtableChangeStreamsToKafka.class)
@Category({TemplateIntegrationTest.class})
@RunWith(Parameterized.class)
public class BigtableChangeStreamsToKafkaIT extends TemplateTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(BigtableChangeStreamsToKafkaIT.class);

  private static final int KAFKA_PARTITIONS = 1;
  private static final Duration KAFKA_POLL_TIMEOUT = Duration.ofMillis(1000);

  private static final String STANDARD_COLUMN_FAMILY = "cf";
  private static final String COLUMN_FAMILY1 = "cf1";
  private static final String COLUMN_FAMILY2 = "cf2";
  private static final String IGNORED_COLUMN_FAMILY = "cfIgnored";

  private static final Duration EXPECTED_REPLICATION_MAX_WAIT_TIME = Duration.ofMinutes(10);
  private static final String TEST_ZONE = "us-central1-b";
  private static final Base64.Encoder B64 = Base64.getEncoder();
  private BigtableResourceManager bigtableResourceManager;
  private KafkaResourceManager kafkaResourceManager;
  private SchemaRegistryResourceManager schemaRegistryResourceManager;
  private KafkaConsumer<byte[], byte[]> kafkaConsumer;

  private final String clusterName = "teleport-c1";
  private String appProfileId;
  private String topicName;
  private String srcTable;

  @Parameterized.Parameter public Boolean noDlqRetry;

  @Parameterized.Parameters(name = "no-dlq-retry-{0}")
  public static List<Boolean> testParameters() {
    return List.of(false, true);
  }

  @Test
  public void testJsonCustomCharset() throws Exception {
    String charsetName = "KOI8-R";
    LaunchInfo launchInfo = launchTemplate("JSON", charsetName);

    assertThatPipeline(launchInfo).isRunning();

    String rowkey = UUID.randomUUID().toString();

    // Russian letter B in KOI8-R
    Charset charset = Charset.forName(charsetName);
    byte[] columnBytes = new byte[] {(byte) 0xc2};

    String value = UUID.randomUUID().toString();
    long timestamp = 12000L;

    RowMutation rowMutation =
        RowMutation.create(TableId.of(srcTable), rowkey)
            .setCell(
                STANDARD_COLUMN_FAMILY,
                ByteString.copyFrom(columnBytes),
                timestamp,
                ByteString.copyFrom(value, Charset.defaultCharset()));

    ChangelogEntryMessageBase64 expected =
        ChangelogEntryMessageBase64.newBuilder()
            .setColumn(B64.encodeToString(columnBytes))
            .setColumnFamily(STANDARD_COLUMN_FAMILY)
            .setIsGC(false)
            .setModType(ModType.SET_CELL)
            .setCommitTimestamp(System.currentTimeMillis() * 1000)
            .setRowKey(B64.encodeToString(rowkey.getBytes(charset)))
            .setSourceInstance(bigtableResourceManager.getInstanceId())
            .setSourceCluster(clusterName)
            .setTieBreaker(1)
            .setTimestamp(timestamp)
            .setTimestampFrom(null)
            .setTimestampTo(null)
            .setSourceTable(srcTable)
            .setValue(B64.encodeToString(value.getBytes(charset)))
            .build();

    bigtableResourceManager.write(rowMutation);

    kafkaConsumer.subscribe(List.of(topicName));
    List<ConsumerRecord<byte[], byte[]>> receivedMessages =
        getAtLeastOneMessage(launchInfo, kafkaConsumer, topicName);
    assertEquals(1, receivedMessages.size());
    validateJsonMessageData(
        expected, new String(receivedMessages.get(0).value(), StandardCharsets.UTF_8));
    assertArrayEquals(rowkey.getBytes(StandardCharsets.UTF_8), receivedMessages.get(0).key());
  }

  @Test
  public void testAvro() throws Exception {
    String rowkey = UUID.randomUUID().toString();
    String column = UUID.randomUUID().toString();
    String value = UUID.randomUUID().toString();
    long timestamp = 12000L;

    ChangelogEntryMessage expected =
        ChangelogEntryMessage.newBuilder()
            .setColumn(ByteBuffer.wrap(column.getBytes(Charset.defaultCharset())))
            .setColumnFamily(STANDARD_COLUMN_FAMILY)
            .setIsGC(false)
            .setModType(ModType.SET_CELL)
            .setCommitTimestamp(System.currentTimeMillis() * 1000)
            .setRowKey(ByteBuffer.wrap(rowkey.getBytes(Charset.defaultCharset())))
            .setSourceInstance(bigtableResourceManager.getInstanceId())
            .setSourceCluster(clusterName)
            .setTieBreaker(1)
            .setTimestamp(timestamp)
            .setTimestampFrom(null)
            .setTimestampTo(null)
            .setSourceTable(srcTable)
            .setValue(ByteBuffer.wrap(value.getBytes(StandardCharsets.UTF_8)))
            .build();

    LaunchInfo launchInfo = launchTemplate("AVRO_BINARY_ENCODING", "UTF-8");

    assertThatPipeline(launchInfo).isRunning();

    RowMutation rowMutation =
        RowMutation.create(TableId.of(srcTable), rowkey)
            .setCell(STANDARD_COLUMN_FAMILY, column, timestamp, value);

    bigtableResourceManager.write(rowMutation);

    kafkaConsumer.subscribe(List.of(topicName));
    List<ConsumerRecord<byte[], byte[]>> receivedMessages =
        getAtLeastOneMessage(launchInfo, kafkaConsumer, topicName);
    assertEquals(1, receivedMessages.size());
    validateAvroMessageData(expected, receivedMessages.get(0).value());
    assertArrayEquals(rowkey.getBytes(StandardCharsets.UTF_8), receivedMessages.get(0).key());
  }

  @Test
  public void testAvroConfluent() throws Exception {
    schemaRegistryResourceManager =
        SchemaRegistryResourceManager.builder(testName)
            .withTestContainerKafkaBootstrapServers(
                Arrays.stream(kafkaResourceManager.getBootstrapServers().split(","))
                    .map(protocolHostPort -> protocolHostPort.split("://")[1].split(":")[1])
                    .map(Integer::parseInt)
                    .collect(Collectors.toSet()))
            .build();

    String schemaRegistryUrl = schemaRegistryResourceManager.getConnectionString();
    try (KafkaConsumer<byte[], Object> avroKafkaConsumer =
        kafkaResourceManager.buildConsumer(
            new ByteArrayDeserializer(),
            new KafkaAvroDeserializer(
                null, Map.of(SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl), false))) {

      String rowkey = UUID.randomUUID().toString();
      String column = UUID.randomUUID().toString();
      String value = UUID.randomUUID().toString();
      long timestamp = 12000L;

      ChangelogEntryMessage expected =
          ChangelogEntryMessage.newBuilder()
              .setColumn(ByteBuffer.wrap(column.getBytes(Charset.defaultCharset())))
              .setColumnFamily(STANDARD_COLUMN_FAMILY)
              .setIsGC(false)
              .setModType(ModType.SET_CELL)
              .setCommitTimestamp(System.currentTimeMillis() * 1000)
              .setRowKey(ByteBuffer.wrap(rowkey.getBytes(Charset.defaultCharset())))
              .setSourceInstance(bigtableResourceManager.getInstanceId())
              .setSourceCluster(clusterName)
              .setTieBreaker(1)
              .setTimestamp(timestamp)
              .setTimestampFrom(null)
              .setTimestampTo(null)
              .setSourceTable(srcTable)
              .setValue(ByteBuffer.wrap(value.getBytes(StandardCharsets.UTF_8)))
              .build();

      LaunchInfo launchInfo =
          launchTemplate(
              "AVRO_CONFLUENT_WIRE_FORMAT",
              "UTF-8",
              Map.of(
                  "schemaRegistryConnectionUrl",
                  schemaRegistryResourceManager.getConnectionString(),
                  "schemaFormat",
                  KafkaTemplateParameters.SchemaFormat.SCHEMA_REGISTRY));

      assertThatPipeline(launchInfo).isRunning();

      RowMutation rowMutation =
          RowMutation.create(TableId.of(srcTable), rowkey)
              .setCell(STANDARD_COLUMN_FAMILY, column, timestamp, value);

      bigtableResourceManager.write(rowMutation);

      avroKafkaConsumer.subscribe(Collections.singletonList(topicName));
      List<ConsumerRecord<byte[], Object>> receivedMessages =
          getAtLeastOneMessage(launchInfo, avroKafkaConsumer, topicName);
      assertEquals(1, receivedMessages.size());
      validateAvroMessageData(expected, receivedMessages.get(0).value());
      assertArrayEquals(rowkey.getBytes(StandardCharsets.UTF_8), receivedMessages.get(0).key());

      try (SchemaRegistryClient src = new CachedSchemaRegistryClient(schemaRegistryUrl, 10)) {
        ParsedSchema schema = src.getSchemaBySubjectAndId(topicName, 1);
        assertEquals(ChangelogEntryMessage.class.getName(), schema.name());
      }
    }
  }

  @Test
  public void testDeadLetterQueueDelivery() throws Exception {
    String dlqPrefix = "dlq";
    LaunchInfo launchInfo =
        launchTemplate(
            "JSON",
            "UTF-8",
            Map.of(
                "dlqDirectory", getGcsPath(dlqPrefix),
                "dlqMaxRetries", "1",
                "dlqRetryMinutes", "1"));

    assertThatPipeline(launchInfo).isRunning();

    String rowkey = UUID.randomUUID().toString();
    String column = "test_column";

    String goodValue = UUID.randomUUID().toString();
    // Making some 2MB value (the default max.request.size is 1048576 (1 MB)).
    String tooBigValue =
        StringUtils.repeat(UUID.randomUUID().toString(), 2 * 1024 * 1024 / goodValue.length());

    long timestamp = 12000L;

    RowMutation tooLargeMutation =
        RowMutation.create(TableId.of(srcTable), rowkey)
            .setCell(STANDARD_COLUMN_FAMILY, column, timestamp, tooBigValue);
    bigtableResourceManager.write(tooLargeMutation);

    RowMutation smallMutation =
        RowMutation.create(TableId.of(srcTable), rowkey)
            .setCell(STANDARD_COLUMN_FAMILY, column, timestamp, goodValue);
    bigtableResourceManager.write(smallMutation);

    ChangelogEntryMessageBase64 expected =
        ChangelogEntryMessageBase64.newBuilder()
            .setColumn(B64.encodeToString(column.getBytes(StandardCharsets.UTF_8)))
            .setColumnFamily(STANDARD_COLUMN_FAMILY)
            .setIsGC(false)
            .setModType(ModType.SET_CELL)
            .setCommitTimestamp(System.currentTimeMillis() * 1000)
            .setRowKey(B64.encodeToString(rowkey.getBytes(StandardCharsets.UTF_8)))
            .setSourceInstance(bigtableResourceManager.getInstanceId())
            .setSourceCluster(clusterName)
            .setTieBreaker(1)
            .setTimestamp(timestamp)
            .setTimestampFrom(null)
            .setTimestampTo(null)
            .setSourceTable(srcTable)
            .setValue(B64.encodeToString(goodValue.getBytes(StandardCharsets.UTF_8)))
            .build();

    kafkaConsumer.subscribe(Collections.singletonList(topicName));
    List<ConsumerRecord<byte[], byte[]>> receivedMessages =
        getAtLeastOneMessage(launchInfo, kafkaConsumer, topicName);
    assertEquals(1, receivedMessages.size());
    validateJsonMessageData(
        expected, new String(receivedMessages.get(0).value(), StandardCharsets.UTF_8));
    assertArrayEquals(rowkey.getBytes(StandardCharsets.UTF_8), receivedMessages.get(0).key());

    LOG.info("Looking for files in DLQ");

    Result result =
        pipelineOperator()
            .waitForConditionAndFinish(
                createConfig(launchInfo),
                () -> {
                  List<Artifact> artifacts =
                      gcsClient.listArtifacts(dlqPrefix, Pattern.compile(".*"));
                  RowJsonUtils.increaseDefaultStreamReadConstraints(100 * 1024 * 1024);
                  ObjectMapper om = new ObjectMapper();
                  for (Artifact artifact : artifacts) {
                    try {
                      JsonNode severeError = om.readTree(artifact.contents());
                      assertNotNull(severeError);
                      JsonNode errorMessageNode = severeError.get("error_message");
                      assertNotNull(errorMessageNode);
                      assertTrue(errorMessageNode instanceof TextNode);
                      String messageText = errorMessageNode.asText();
                      assertTrue(
                          "Unexpected message text: " + messageText,
                          StringUtils.contains(
                              messageText, RecordTooLargeException.class.getName()));
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
  public void testBigtableIgnoreColumnFamiliesAndQualifiers() throws IOException {
    String column1 = "q1";
    String column2 = "q2";

    String ignoredColumn = "qIgnored";
    String ignoredEmptyQualifier = ":";
    Map.Entry<String, String> ignoredColumnFamilyAndQualifier =
        new AbstractMap.SimpleImmutableEntry<>(COLUMN_FAMILY1, column1);

    String charset = "UTF-8";

    LaunchInfo launchInfo =
        launchTemplate(
            "JSON",
            charset,
            Map.of(
                "bigtableChangeStreamIgnoreColumnFamilies",
                IGNORED_COLUMN_FAMILY,
                "bigtableChangeStreamIgnoreColumns",
                String.join(
                    ",",
                    List.of(
                        ignoredColumn,
                        ignoredColumnFamilyAndQualifier.getKey()
                            + ":"
                            + ignoredColumnFamilyAndQualifier.getValue(),
                        ignoredEmptyQualifier))));

    assertThatPipeline(launchInfo).isRunning();

    String rowkey = UUID.randomUUID().toString();
    String value = UUID.randomUUID().toString();
    long timestamp = 12000L;

    RowMutation rowMutationContainingOnlyIgnoredCells =
        RowMutation.create(TableId.of(srcTable), rowkey)
            .setCell(IGNORED_COLUMN_FAMILY, column1, 2 * timestamp, value)
            .setCell(IGNORED_COLUMN_FAMILY, column2, 2 * timestamp, value)
            .setCell(IGNORED_COLUMN_FAMILY, ignoredColumn, 2 * timestamp, value)
            .setCell(COLUMN_FAMILY1, ignoredColumn, 2 * timestamp, value);

    RowMutation rowMutation =
        RowMutation.create(TableId.of(srcTable), rowkey)
            .setCell(IGNORED_COLUMN_FAMILY, column1, timestamp, value)
            .setCell(COLUMN_FAMILY1, ignoredColumn, timestamp, value)
            .setCell(
                ignoredColumnFamilyAndQualifier.getKey(),
                ignoredColumnFamilyAndQualifier.getValue(),
                timestamp,
                value)
            .setCell(COLUMN_FAMILY2, "", timestamp, value)
            .setCell(COLUMN_FAMILY2, column2, timestamp, value);

    ChangelogEntryMessageBase64 expected =
        ChangelogEntryMessageBase64.newBuilder()
            .setColumnFamily(COLUMN_FAMILY2)
            .setColumn(B64.encodeToString(column2.getBytes(charset)))
            .setIsGC(false)
            .setModType(ModType.SET_CELL)
            .setCommitTimestamp(System.currentTimeMillis() * 1000)
            .setRowKey(B64.encodeToString(rowkey.getBytes(charset)))
            .setSourceInstance(bigtableResourceManager.getInstanceId())
            .setSourceCluster(clusterName)
            .setTieBreaker(1)
            .setTimestamp(timestamp)
            .setTimestampFrom(null)
            .setTimestampTo(null)
            .setSourceTable(srcTable)
            .setValue(B64.encodeToString(value.getBytes(charset)))
            .build();

    bigtableResourceManager.write(rowMutationContainingOnlyIgnoredCells);
    bigtableResourceManager.write(rowMutation);

    kafkaConsumer.subscribe(List.of(topicName));
    List<ConsumerRecord<byte[], byte[]>> receivedMessages =
        getAtLeastOneMessage(launchInfo, kafkaConsumer, topicName);
    assertEquals(1, receivedMessages.size());
    validateJsonMessageData(
        expected, new String(receivedMessages.get(0).value(), StandardCharsets.UTF_8));
    assertArrayEquals(rowkey.getBytes(charset), receivedMessages.get(0).key());
  }

  private void validateJsonMessageData(ChangelogEntryMessageBase64 expected, String jsonString)
      throws IOException {
    RowJsonUtils.increaseDefaultStreamReadConstraints(100 * 1024 * 1024);
    ObjectMapper mapper = new ObjectMapper();
    JsonNode jsonTree = mapper.readTree(jsonString);

    assertEquals(expected.getModType().name(), jsonTree.get("modType").asText());
    assertEquals(expected.getIsGC(), jsonTree.get("isGC").asBoolean());
    assertTrue(jsonTree.get("tieBreaker").asLong() >= 0);
    assertTrue(
        Math.abs(
                Long.parseLong(jsonTree.get("commitTimestamp").asText())
                    - expected.getCommitTimestamp())
            <= 10000000L);
    assertEquals(expected.getColumnFamily(), jsonTree.get("columnFamily").asText());
    assertEquals(
        (Object) expected.getTimestamp(),
        Long.parseLong(jsonTree.get("timestamp").get("long").asText()));
    assertEquals(expected.getSourceInstance(), jsonTree.get("sourceInstance").asText());
    assertEquals(expected.getSourceCluster(), jsonTree.get("sourceCluster").asText());
    assertEquals(expected.getSourceTable(), jsonTree.get("sourceTable").asText());

    assertEquals(expected.getRowKey(), jsonTree.get("rowKey").asText());
    assertEquals(expected.getColumn(), jsonTree.get("column").get("string").asText());
    assertEquals(expected.getValue(), jsonTree.get("value").get("string").asText());
  }

  private void validateAvroMessageData(ChangelogEntryMessage expected, byte[] messageValue)
      throws IOException {
    ByteArrayInputStream input = new ByteArrayInputStream(messageValue);
    Decoder decoder = DecoderFactory.get().directBinaryDecoder(input, /* reuse= */ null);

    SpecificDatumReader<ChangelogEntryMessage> reader =
        new SpecificDatumReader<>(ChangelogEntryMessage.getClassSchema());
    ChangelogEntryMessage received = reader.read(null, decoder);

    assertEquals(expected.getRowKey(), received.getRowKey());
    assertEquals(expected.getModType(), received.getModType());
    assertEquals(expected.getIsGC(), received.getIsGC());
    assertTrue(received.getTieBreaker() >= 0);
    assertTrue(
        Math.abs(received.getCommitTimestamp() - expected.getCommitTimestamp()) <= 10000000L);
    assertEquals(
        nullSafeToString(expected.getColumnFamily()), nullSafeToString(received.getColumnFamily()));
    assertEquals(expected.getColumn(), received.getColumn());
    assertEquals(expected.getTimestamp(), received.getTimestamp());
    assertEquals(
        nullSafeToString(expected.getSourceInstance()),
        nullSafeToString(received.getSourceInstance()));
    assertEquals(
        nullSafeToString(expected.getSourceCluster()),
        nullSafeToString(received.getSourceCluster()));
    assertEquals(
        nullSafeToString(expected.getSourceTable()), nullSafeToString(received.getSourceTable()));
    assertArrayEquals(expected.getValue().array(), received.getValue().array());
  }

  private void validateAvroMessageData(ChangelogEntryMessage expected, Object message) {
    GenericRecord received = (GenericRecord) message;

    assertEquals(expected.getRowKey(), received.get("rowKey"));
    assertEquals(expected.getModType().toString(), received.get("modType").toString());
    assertEquals(expected.getIsGC(), received.get("isGC"));

    assertTrue(
        Math.abs((Long) received.get("commitTimestamp") - expected.getCommitTimestamp())
            <= 10000000L);
    assertEquals(
        nullSafeToString(expected.getColumnFamily()),
        nullSafeToString(received.get("columnFamily")));
    assertEquals(expected.getColumn(), received.get("column"));
    assertEquals(expected.getTimestamp(), received.get("timestamp"));
    assertEquals(
        nullSafeToString(expected.getSourceInstance()),
        nullSafeToString(received.get("sourceInstance")));
    assertEquals(
        nullSafeToString(expected.getSourceCluster()),
        nullSafeToString(received.get("sourceCluster")));
    assertEquals(
        nullSafeToString(expected.getSourceTable()), nullSafeToString(received.get("sourceTable")));
    assertArrayEquals(expected.getValue().array(), ((ByteBuffer) received.get("value")).array());
  }

  private static String nullSafeToString(Object o) {
    return Optional.ofNullable(o).map(Object::toString).orElse(null);
  }

  @NotNull
  private static String generateAppProfileId() {
    return "cdc_app_profile_" + randomAlphanumeric(8).toLowerCase() + "_" + System.nanoTime();
  }

  @Before
  public void setup() throws IOException {
    BigtableResourceManager.Builder rmBuilder =
        BigtableResourceManager.builder(
            removeUnsafeCharacters(testName), PROJECT, credentialsProvider);

    bigtableResourceManager = rmBuilder.maybeUseStaticInstance().build();

    appProfileId = generateAppProfileId();

    String suffix = randomAlphanumeric(8).toLowerCase() + "_" + System.nanoTime();
    String topicNameToCreate = "topic-" + suffix;
    srcTable = "src_" + suffix;

    List<BigtableResourceManagerCluster> clusters = new ArrayList<>();
    clusters.add(BigtableResourceManagerCluster.create(clusterName, TEST_ZONE, 1, StorageType.HDD));

    bigtableResourceManager.createInstance(clusters);

    bigtableResourceManager.createAppProfile(
        appProfileId, true, Lists.asList(clusterName, new String[] {}));

    BigtableTableSpec cdcTableSpec = new BigtableTableSpec();
    cdcTableSpec.setCdcEnabled(true);
    cdcTableSpec.setColumnFamilies(
        List.of(STANDARD_COLUMN_FAMILY, COLUMN_FAMILY1, COLUMN_FAMILY2, IGNORED_COLUMN_FAMILY));
    bigtableResourceManager.createTable(srcTable, cdcTableSpec);

    kafkaResourceManager =
        KafkaResourceManager.builder(testName).setHost(TestProperties.hostIp()).build();
    topicName = kafkaResourceManager.createTopic(topicNameToCreate, KAFKA_PARTITIONS);
    kafkaConsumer =
        kafkaResourceManager.buildConsumer(
            new ByteArrayDeserializer(), new ByteArrayDeserializer());
  }

  @After
  public void tearDownClass() {
    try {
      if (kafkaConsumer != null) {
        kafkaConsumer.close(Duration.ofSeconds(5));
      }
    } catch (Exception e) {
      LOG.warn("Couldn't close Kafka consumer.", e);
    }
    ResourceManagerUtils.cleanResources(
        bigtableResourceManager, schemaRegistryResourceManager, kafkaResourceManager);
  }

  private <KeyT, ValueT> List<ConsumerRecord<KeyT, ValueT>> getAtLeastOneMessage(
      LaunchInfo launchInfo, KafkaConsumer<KeyT, ValueT> consumer, String topicName) {
    AtomicReference<List<ConsumerRecord<KeyT, ValueT>>> output =
        new AtomicReference<>(new ArrayList<>());
    Result result =
        pipelineOperator()
            .waitForCondition(
                createConfig(launchInfo),
                () -> {
                  while (true) {
                    List<ConsumerRecord<KeyT, ValueT>> response =
                        Lists.newArrayList(consumer.poll(KAFKA_POLL_TIMEOUT).records(topicName));
                    if (response.isEmpty()) {
                      break;
                    }
                    output.getAndUpdate(
                        o -> {
                          o.addAll(response);
                          return o;
                        });
                  }
                  return !output.get().isEmpty();
                });
    assertThatResult(result).meetsConditions();

    return output.get();
  }

  private LaunchInfo launchTemplate(String messageFormat, String bigtableCharset)
      throws IOException {
    return launchTemplate(messageFormat, bigtableCharset, Collections.emptyMap());
  }

  private LaunchInfo launchTemplate(
      String messageFormat, String bigtableCharset, Map<String, String> additionalParameters)
      throws IOException {
    PipelineLauncher.LaunchConfig.Builder builder =
        PipelineLauncher.LaunchConfig.builder(removeUnsafeCharacters(testName), specPath)
            .addParameter("bigtableReadTableId", srcTable)
            .addParameter("bigtableReadInstanceId", bigtableResourceManager.getInstanceId())
            .addParameter("bigtableChangeStreamAppProfile", appProfileId)
            .addParameter("bigtableChangeStreamCharset", bigtableCharset)
            .addParameter("messageFormat", messageFormat)
            .addParameter(
                "writeBootstrapServerAndTopic",
                kafkaResourceManager.getBootstrapServers().replace("PLAINTEXT://", "")
                    + ";"
                    + topicName)
            .addParameter("kafkaWriteAuthenticationMethod", "NONE")
            .addParameter("disableDlqRetries", Boolean.toString(noDlqRetry));
    for (Map.Entry<String, String> additionalParameter : additionalParameters.entrySet()) {
      builder.addParameter(additionalParameter.getKey(), additionalParameter.getValue());
    }
    return super.launchTemplate(builder);
  }

  private String removeUnsafeCharacters(String testName) {
    return testName.replaceAll("[\\[\\]]", "-");
  }

  @Override
  protected PipelineOperator.Config.Builder wrapConfiguration(
      PipelineOperator.Config.Builder builder) {

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
