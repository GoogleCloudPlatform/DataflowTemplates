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

import static com.google.cloud.teleport.v2.templates.SpannerChangeStreamsToPubSub.run;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Options.RpcPriority;
import com.google.cloud.teleport.v2.options.SpannerChangeStreamsToPubSubOptions;
import com.google.cloud.teleport.v2.spanner.IntegrationTest;
import com.google.cloud.teleport.v2.spanner.SpannerServerResource;
import com.google.cloud.teleport.v2.transforms.FileFormatFactorySpannerChangeStreamsToPubSub;
import com.google.gson.Gson;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.Pipeline.PipelineExecutionException;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.io.gcp.pubsub.TestPubsubSignal;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.DataChangeRecord;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Supplier;
import org.apache.commons.lang3.RandomStringUtils;
import org.joda.time.Duration;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Test class for {@link SpannerChangeStreamsToPubSubTest}. */
@RunWith(JUnit4.class)
public final class SpannerChangeStreamsToPubSubTest {
  /** Logger for class. */
  private static final Logger LOG = LoggerFactory.getLogger(SpannerChangeStreamsToPubSubTest.class);

  /** Rule for exception testing. */
  @Rule public ExpectedException exception = ExpectedException.none();

  @Rule public final TemporaryFolder tmpDir = new TemporaryFolder();

  /** Rule for Spanner server resource. */
  @Rule public final SpannerServerResource spannerServer = new SpannerServerResource();

  /** Rule for pipeline testing. */
  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  @Rule public transient TestPubsubSignal signal = TestPubsubSignal.create();

  private static final String TEST_PROJECT = "span-cloud-testing";
  private static final String TEST_INSTANCE = "change-stream-test";
  private static final String TEST_DATABASE_PREFIX = "testdbchangestreams";
  private static final String TEST_TABLE = "Users";
  private static final String TEST_CHANGE_STREAM = "UsersStream";
  private static final int MAX_TABLE_NAME_LENGTH = 29;
  private static final int MAX_SPANNER_STRING_SIZE = 2621440;
  private static String outputTopicName;
  private static String outputTopic;
  private static String subscriptionPath;
  private static String subscriptionName;
  private static final String PUBSUBIO = "pubsubio";
  private static final String NATIVE_CLIENT = "native_client";
  private static final String AVRO = "AVRO";
  private static final String JSON = "JSON";

  @Before
  public void setUp() throws Exception {
    outputTopicName = "change-streams-to-pubsub-template-test";
    outputTopic = "projects/span-cloud-testing/topics/" + outputTopicName;
    subscriptionName = "change-streams-to-pubsub-template-test-sub";
    subscriptionPath = "projects/" + TEST_PROJECT + "/subscriptions/" + subscriptionName;
  }

  @SuppressWarnings("DefaultAnnotationParam")
  private static class VerifyDataChangeRecordAvro
      implements SerializableFunction<
          Iterable<com.google.cloud.teleport.v2.DataChangeRecord>, Void> {
    @Override
    public Void apply(Iterable<com.google.cloud.teleport.v2.DataChangeRecord> actualIter) {
      // Make sure actual is the right length, and is a
      // subset of expected.
      List<com.google.cloud.teleport.v2.DataChangeRecord> actual = new ArrayList<>();
      for (com.google.cloud.teleport.v2.DataChangeRecord s : actualIter) {
        actual.add(s);
        assertEquals(TEST_TABLE, s.getTableName());
        assertTrue(s.getCommitTimestamp() > 0);
        assertTrue(s.getPartitionToken() != null && s.getPartitionToken().length() > 0);
        assertTrue(s.getServerTransactionId() != null && s.getServerTransactionId().length() > 0);
        assertTrue(s.getRecordSequence() != null && s.getRecordSequence().length() > 0);
        assertTrue(!s.getRowType().isEmpty());
        assertTrue(
            s.getRowType().get(0).getType()
                != com.google.cloud.teleport.v2.TypeCode.TYPE_CODE_UNSPECIFIED);
        assertTrue(!s.getMods().isEmpty());
        assertTrue(s.getNumberOfRecordsInTransaction() > 0);
        assertTrue(s.getNumberOfPartitionsInTransaction() > 0);
        assertTrue(s.getMetadata() != null);
      }
      return null;
    }
  }

  @SuppressWarnings("DefaultAnnotationParam")
  private static class VerifyDataChangeRecordText
      implements SerializableFunction<Iterable<String>, Void> {
    @Override
    public Void apply(Iterable<String> actualIter) {
      // Make sure actual is the right length, and is a
      // subset of expected.
      List<DataChangeRecord> actual = new ArrayList<>();
      for (String dataChangeRecordString : actualIter) {
        DataChangeRecord s = new Gson().fromJson(dataChangeRecordString, DataChangeRecord.class);
        actual.add(s);
        assertEquals(TEST_TABLE, s.getTableName());
        assertTrue(s.getCommitTimestamp().getSeconds() > 0);
        assertTrue(s.getPartitionToken() != null && s.getPartitionToken().length() > 0);
        assertTrue(s.getServerTransactionId() != null && s.getServerTransactionId().length() > 0);
        assertTrue(s.getRecordSequence() != null && s.getRecordSequence().length() > 0);
        assertTrue(!s.getRowType().isEmpty());
        assertTrue(!s.getMods().isEmpty());
        assertTrue(s.getNumberOfRecordsInTransaction() > 0);
        assertTrue(s.getNumberOfPartitionsInTransaction() > 0);
        assertTrue(s.getMetadata() != null);
      }
      return null;
    }
  }

  private String generateDatabaseName() {
    return TEST_DATABASE_PREFIX
        + "_"
        + RandomStringUtils.randomNumeric(
            MAX_TABLE_NAME_LENGTH - 1 - TEST_DATABASE_PREFIX.length());
  }

  /**
   * Test whether {@link FileFormatFactory} maps the output file format to the transform to be
   * carried out. And throws illegal argument exception if invalid file format is passed.
   */
  @Test
  @Category(IntegrationTest.class)
  public void testFileFormatFactoryInvalid() {
    exception.expect(IllegalArgumentException.class);
    exception.expectMessage("Invalid output format:PARQUET. Supported output formats: JSON, AVRO");

    SpannerChangeStreamsToPubSubOptions options =
        PipelineOptionsFactory.create().as(SpannerChangeStreamsToPubSubOptions.class);
    options.setOutputDataFormat("PARQUET");
    options.setPubsubTopic(outputTopicName);
    Pipeline p = Pipeline.create(options);
    Timestamp startTimestamp = Timestamp.now();
    Timestamp endTimestamp = Timestamp.now();

    p
        // Reads from the change stream
        .apply(
            SpannerIO.readChangeStream()
                .withSpannerConfig(
                    SpannerConfig.create()
                        .withProjectId("cloud-spanner-backups-loadtest")
                        .withInstanceId("change-stream-load-test-3")
                        .withDatabaseId("load-test-change-stream-enable"))
                .withMetadataInstance("change-stream-load-test-3")
                .withMetadataDatabase("change-stream-metadata")
                .withChangeStreamName("changeStreamAll")
                .withInclusiveStartAt(startTimestamp)
                .withInclusiveEndAt(endTimestamp)
                .withRpcPriority(RpcPriority.HIGH))
        .apply(
            "Write To PubSub",
            FileFormatFactorySpannerChangeStreamsToPubSub.newBuilder().setOptions(options).build());
    p.run();
  }

  @Test
  @Category(IntegrationTest.class)
  // This test can only be run locally with the following command:
  // mvn -Dexcluded.spanner.tests="" -Dtest=SpannerChangeStreamsToPubSubTest test
  public void testWriteToPubSubAvroNativeClient() throws Exception {
    testWriteToPubSubAvro(NATIVE_CLIENT);
  }

  @Test
  @Category(IntegrationTest.class)
  // This test can only be run locally with the following command:
  // mvn -Dexcluded.spanner.tests="" -Dtest=SpannerChangeStreamsToPubSubTest test
  public void testWriteToPubSubAvroPubsubIO() throws Exception {
    testWriteToPubSubAvro(PUBSUBIO);
  }

  private void testWriteToPubSubAvro(String pubsubAPI) throws Exception {
    // Create a test database.
    String testDatabase = generateDatabaseName();

    spannerServer.dropDatabase(testDatabase);

    // Create a table.
    List<String> statements = new ArrayList<String>();
    final String createTable =
        "CREATE TABLE "
            + TEST_TABLE
            + " ("
            + "user_id INT64 NOT NULL,"
            + "name STRING(MAX) "
            + ") PRIMARY KEY(user_id)";
    final String createChangeStream = "CREATE CHANGE STREAM " + TEST_CHANGE_STREAM + " FOR Users";
    statements.add(createTable);
    statements.add(createChangeStream);
    spannerServer.createDatabase(testDatabase, statements);

    Timestamp startTimestamp = Timestamp.now();

    // Create a mutation for the table that will generate 1 data change record.
    List<Mutation> mutations = new ArrayList<>();
    mutations.add(
        Mutation.newInsertBuilder(TEST_TABLE).set("user_id").to(1).set("name").to("Name1").build());
    mutations.add(
        Mutation.newInsertBuilder(TEST_TABLE).set("user_id").to(2).set("name").to("Name2").build());

    spannerServer.getDbClient(testDatabase).write(mutations);

    Timestamp endTimestamp = Timestamp.now();

    SpannerChangeStreamsToPubSubOptions options =
        PipelineOptionsFactory.create().as(SpannerChangeStreamsToPubSubOptions.class);
    options.setSpannerProjectId(TEST_PROJECT);
    options.setSpannerInstanceId(TEST_INSTANCE);
    options.setSpannerDatabase(testDatabase);
    options.setSpannerMetadataInstanceId(TEST_INSTANCE);
    options.setSpannerMetadataDatabase(testDatabase);
    options.setSpannerChangeStreamName(TEST_CHANGE_STREAM);
    options.setPubsubTopic(outputTopicName);
    options.setStartTimestamp(startTimestamp.toString());
    options.setEndTimestamp(endTimestamp.toString());
    List<String> experiments = new ArrayList<String>();
    options.setExperiments(experiments);

    options.setOutputDataFormat(AVRO);
    options.setPubsubAPI(pubsubAPI);
    // Run the pipeline.
    PipelineResult result = run(options);
    result.waitUntilFinish();
    pipeline.getOptions().as(TestPipelineOptions.class).setBlockOnRun(false);
    // Read from the output PubsubMessage with data in Avro to assert that 1 data change record has
    // been generated.
    PCollection<PubsubMessage> receivedPubsubMessages =
        pipeline.apply(
            "Read From Pub/Sub Subscription",
            PubsubIO.readMessages().fromSubscription(subscriptionPath));
    PCollection<com.google.cloud.teleport.v2.DataChangeRecord> dataChangeRecords =
        receivedPubsubMessages.apply(
            ParDo.of(
                // Convert PubsubMessage to DataChangeRecord
                new DoFn<PubsubMessage, com.google.cloud.teleport.v2.DataChangeRecord>() {
                  @ProcessElement
                  public void processElement(ProcessContext context) {
                    PubsubMessage message = context.element();
                    AvroCoder<com.google.cloud.teleport.v2.DataChangeRecord> coder =
                        AvroCoder.of(com.google.cloud.teleport.v2.DataChangeRecord.class);
                    com.google.cloud.teleport.v2.DataChangeRecord record = null;
                    try {
                      record = CoderUtils.decodeFromByteArray(coder, message.getPayload());
                    } catch (CoderException exc) {
                      LOG.error("Fail to decode DataChangeRecord from PubsubMessage payload.");
                    }
                    context.output(record);
                  }
                }));
    dataChangeRecords.apply(
        "waitForAnyMessage",
        signal.signalSuccessWhen(dataChangeRecords.getCoder(), anyMessages -> true));
    Supplier<Void> start = signal.waitForStart(Duration.standardMinutes(1));
    pipeline.apply(signal.signalStart());

    PAssert.that(dataChangeRecords).satisfies(new VerifyDataChangeRecordAvro());
    PipelineResult job = pipeline.run();
    start.get();
    signal.waitForSuccess(Duration.standardMinutes(5));
    // A runner may not support cancel
    try {
      job.cancel();
      System.out.println("Called cancel");
    } catch (UnsupportedOperationException exc) {
      // noop
    }
    // Drop the database.
    spannerServer.dropDatabase(testDatabase);
  }

  @Test
  @Category(IntegrationTest.class)
  // This test can only be run locally with the following command:
  // mvn -Dexcluded.spanner.tests="" -Dtest=SpannerChangeStreamsToPubSubTest test
  public void testWriteToPubSubJsonNativeClient() throws Exception {
    testWriteToPubSubJson(NATIVE_CLIENT);
  }

  @Test
  @Category(IntegrationTest.class)
  // This test can only be run locally with the following command:
  // mvn -Dexcluded.spanner.tests="" -Dtest=SpannerChangeStreamsToPubSubTest test
  public void testWriteToPubSubJsonPubsubIO() throws Exception {
    testWriteToPubSubJson(PUBSUBIO);
  }

  // This test can only be run locally with the following command:
  // mvn -Dexcluded.spanner.tests="" -Dtest=SpannerChangeStreamsToPubSubTest test
  private void testWriteToPubSubJson(String pubsubAPI) throws Exception {
    // Create a test database.
    String testDatabase = generateDatabaseName();

    spannerServer.dropDatabase(testDatabase);

    // Create a table.
    List<String> statements = new ArrayList<String>();
    final String createTable =
        "CREATE TABLE "
            + TEST_TABLE
            + " ("
            + "user_id INT64 NOT NULL,"
            + "name STRING(MAX) "
            + ") PRIMARY KEY(user_id)";
    final String createChangeStream = "CREATE CHANGE STREAM " + TEST_CHANGE_STREAM + " FOR Users";
    statements.add(createTable);
    statements.add(createChangeStream);
    spannerServer.createDatabase(testDatabase, statements);

    Timestamp startTimestamp = Timestamp.now();

    // Create a mutation for the table that will generate 1 data change record.
    List<Mutation> mutations = new ArrayList<>();
    mutations.add(
        Mutation.newInsertBuilder(TEST_TABLE).set("user_id").to(1).set("name").to("Name1").build());
    mutations.add(
        Mutation.newInsertBuilder(TEST_TABLE).set("user_id").to(2).set("name").to("Name2").build());

    spannerServer.getDbClient(testDatabase).write(mutations);

    Timestamp endTimestamp = Timestamp.now();

    SpannerChangeStreamsToPubSubOptions options =
        PipelineOptionsFactory.create().as(SpannerChangeStreamsToPubSubOptions.class);
    options.setSpannerProjectId(TEST_PROJECT);
    options.setSpannerInstanceId(TEST_INSTANCE);
    options.setSpannerDatabase(testDatabase);
    options.setSpannerMetadataInstanceId(TEST_INSTANCE);
    options.setSpannerMetadataDatabase(testDatabase);
    options.setSpannerChangeStreamName(TEST_CHANGE_STREAM);
    options.setPubsubTopic(outputTopicName);
    options.setStartTimestamp(startTimestamp.toString());
    options.setEndTimestamp(endTimestamp.toString());
    List<String> experiments = new ArrayList<String>();
    options.setExperiments(experiments);

    options.setOutputDataFormat(JSON);
    options.setPubsubAPI(pubsubAPI);
    // Run the pipeline.
    PipelineResult result = run(options);
    result.waitUntilFinish();

    pipeline.getOptions().as(TestPipelineOptions.class).setBlockOnRun(false);
    PCollection<PubsubMessage> receivedPubsubMessages =
        pipeline.apply(
            "readFromPubSubSubscription",
            PubsubIO.readMessagesWithAttributes().fromSubscription(subscriptionPath));

    PCollection<String> dataChangeRecords =
        receivedPubsubMessages.apply(
            ParDo.of(
                // Convert PubsubMessage to String
                new DoFn<PubsubMessage, String>() {
                  @ProcessElement
                  public void processElement(ProcessContext context) {
                    PubsubMessage message = context.element();
                    byte[] payload = message.getPayload();
                    String recordStr = new String(payload);
                    context.output(recordStr);
                  }
                }));
    dataChangeRecords.apply(
        "waitForAnyMessage",
        signal.signalSuccessWhen(dataChangeRecords.getCoder(), anyMessages -> true));
    Supplier<Void> start = signal.waitForStart(Duration.standardMinutes(1));
    pipeline.apply(signal.signalStart());
    PAssert.that(dataChangeRecords).satisfies(new VerifyDataChangeRecordText());
    PipelineResult job = pipeline.run();
    start.get();
    signal.waitForSuccess(Duration.standardMinutes(5));
    // A runner may not support cancel
    try {
      job.cancel();
      System.out.println("Called cancel");
    } catch (UnsupportedOperationException exc) {
      // noop
    }
    // Drop the database.
    spannerServer.dropDatabase(testDatabase);
  }

  @Test
  @Category(IntegrationTest.class)
  // This test can only be run locally with the following command:
  // mvn -Dexcluded.spanner.tests="" -Dtest=SpannerChangeStreamsToPubSubTest test
  public void oversizedRecordTestPubsubIO() throws Exception {
    oversizedRecordTest(PUBSUBIO);
  }

  @Test
  @Category(IntegrationTest.class)
  // This test can only be run locally with the following command:
  // mvn -Dexcluded.spanner.tests="" -Dtest=SpannerChangeStreamsToPubSubTest test
  public void oversizedRecordTestNativeClient() throws Exception {
    oversizedRecordTest(NATIVE_CLIENT);
  }

  private void oversizedRecordTest(String pubsubAPI) throws Exception {
    exception.expect(PipelineExecutionException.class);
    // Create a test database.
    String testDatabase = generateDatabaseName();
    spannerServer.dropDatabase(testDatabase);

    // Create a table.
    List<String> statements = new ArrayList<String>();
    final String createTable =
        "CREATE TABLE "
            + TEST_TABLE
            + " ("
            + "user_id INT64 NOT NULL,"
            + "name STRING(MAX), "
            + "col1 STRING(MAX), "
            + "col2 STRING(MAX), "
            + "col3 STRING(MAX), "
            + "col4 STRING(MAX), "
            + "col5 STRING(MAX) "
            + ") PRIMARY KEY(user_id)";
    final String createChangeStream = "CREATE CHANGE STREAM " + TEST_CHANGE_STREAM + " FOR Users";
    statements.add(createTable);
    statements.add(createChangeStream);
    spannerServer.createDatabase(testDatabase, statements);

    Timestamp startTimestamp = Timestamp.now();

    List<Mutation> mutations = new ArrayList<>();
    String str = RandomStringUtils.randomNumeric(MAX_SPANNER_STRING_SIZE);
    Random rand = new Random();
    int id = rand.nextInt(Integer.MAX_VALUE);
    mutations.add(
        Mutation.newInsertBuilder(TEST_TABLE)
            .set("user_id")
            .to(id)
            .set("name")
            .to(str)
            .set("col1")
            .to(str)
            .set("col2")
            .to(str)
            .set("col3")
            .to(str)
            .set("col4")
            .to(str)
            .set("col5")
            .to(str)
            .build());
    spannerServer.getDbClient(testDatabase).write(mutations);

    Timestamp endTimestamp = Timestamp.now();

    SpannerChangeStreamsToPubSubOptions options =
        PipelineOptionsFactory.create().as(SpannerChangeStreamsToPubSubOptions.class);
    options.setSpannerProjectId(TEST_PROJECT);
    options.setSpannerInstanceId(TEST_INSTANCE);
    options.setSpannerDatabase(testDatabase);
    options.setSpannerMetadataInstanceId(TEST_INSTANCE);
    options.setSpannerMetadataDatabase(testDatabase);
    options.setSpannerChangeStreamName(TEST_CHANGE_STREAM);
    options.setPubsubTopic(outputTopicName);
    options.setStartTimestamp(startTimestamp.toString());
    options.setEndTimestamp(endTimestamp.toString());
    List<String> experiments = new ArrayList<String>();
    options.setExperiments(experiments);

    options.setOutputDataFormat(AVRO);
    options.setPubsubAPI(pubsubAPI);
    // Run the pipeline.
    PipelineResult result = run(options);
    result.waitUntilFinish();
    pipeline.getOptions().as(TestPipelineOptions.class).setBlockOnRun(false);
    // Read from the output PubsubMessage with data in Avro to assert that 1 data change record has
    // been generated.
    PCollection<PubsubMessage> receivedPubsubMessages =
        pipeline.apply(
            "Read From Pub/Sub Subscription",
            PubsubIO.readMessages().fromSubscription(subscriptionPath));
    PCollection<com.google.cloud.teleport.v2.DataChangeRecord> dataChangeRecords =
        receivedPubsubMessages.apply(
            ParDo.of(
                // Convert PubsubMessage to DataChangeRecord
                new DoFn<PubsubMessage, com.google.cloud.teleport.v2.DataChangeRecord>() {
                  @ProcessElement
                  public void processElement(ProcessContext context) {
                    PubsubMessage message = context.element();
                    AvroCoder<com.google.cloud.teleport.v2.DataChangeRecord> coder =
                        AvroCoder.of(com.google.cloud.teleport.v2.DataChangeRecord.class);
                    com.google.cloud.teleport.v2.DataChangeRecord record = null;
                    try {
                      record = CoderUtils.decodeFromByteArray(coder, message.getPayload());
                    } catch (CoderException exc) {
                      LOG.error("Fail to decode DataChangeRecord from PubsubMessage payload.");
                    }
                    context.output(record);
                  }
                }));
    dataChangeRecords.apply(
        "waitForAnyMessage",
        signal.signalSuccessWhen(dataChangeRecords.getCoder(), anyMessages -> true));
    Supplier<Void> start = signal.waitForStart(Duration.standardMinutes(1));
    pipeline.apply(signal.signalStart());

    PAssert.that(dataChangeRecords).satisfies(new VerifyDataChangeRecordAvro());
    PipelineResult job = null;
    try {
      job = pipeline.run();
    } catch (PipelineExecutionException e) {
      throw e;
    }
    start.get();
    signal.waitForSuccess(Duration.standardMinutes(5));
    // A runner may not support cancel
    try {
      job.cancel();
      System.out.println("Called cancel");
    } catch (UnsupportedOperationException exc) {
      // noop
    }
    // Drop the database.
    spannerServer.dropDatabase(testDatabase);
  }
}
