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

import static com.google.cloud.teleport.v2.templates.SpannerChangeStreamsToGcs.run;
import static org.junit.Assert.assertEquals;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Options.RpcPriority;
import com.google.cloud.teleport.v2.options.SpannerChangeStreamsToGcsOptions;
import com.google.cloud.teleport.v2.spanner.IntegrationTest;
import com.google.cloud.teleport.v2.spanner.SpannerServerResource;
import com.google.cloud.teleport.v2.transforms.FileFormatFactorySpannerChangeStreams;
import com.google.cloud.teleport.v2.utils.DurationUtils;
import com.google.cloud.teleport.v2.utils.WriteToGCSUtility.FileFormat;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.DataChangeRecord;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test class for {@link SpannerChangeStreamsToGcsTest}. */
@RunWith(JUnit4.class)
public final class SpannerChangeStreamsToGcsTest {

  /** Rule for exception testing. */
  @Rule public ExpectedException exception = ExpectedException.none();

  @Rule public final TemporaryFolder tmpDir = new TemporaryFolder();

  /** Rule for Spanner server resource. */
  @Rule public final SpannerServerResource spannerServer = new SpannerServerResource();

  /** Rule for pipeline testing. */
  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  private static final String AVRO_FILENAME_PREFIX = "avro-output-";
  private static final Integer NUM_SHARDS = 1;
  private static final String TEST_PROJECT = "span-cloud-testing";
  private static final String TEST_INSTANCE = "changestream";
  private static final String TEST_DATABASE_PREFIX = "testdbchangestreams";
  private static final String TEST_TABLE = "Users";
  private static final String TEST_CHANGE_STREAM = "UsersStream";
  private static final int MAX_TABLE_NAME_LENGTH = 29;

  private static String fakeDir;
  private static String fakeTempLocation;
  private static final String FILENAME_PREFIX = "filenamePrefix";

  @Before
  public void setup() throws Exception {
    fakeDir = tmpDir.newFolder("output").getAbsolutePath();
    fakeTempLocation = tmpDir.newFolder("temporaryLocation").getAbsolutePath();
  }

  @SuppressWarnings("DefaultAnnotationParam")
  private static class VerifyDataChangeRecordAvro
      implements SerializableFunction<Iterable<DataChangeRecord>, Void> {
    @Override
    public Void apply(Iterable<DataChangeRecord> actualIter) {
      // Make sure actual is the right length, and is a
      // subset of expected.
      List<DataChangeRecord> actual = new ArrayList<>();
      for (DataChangeRecord s : actualIter) {
        actual.add(s);
        assertEquals(TEST_TABLE, s.getTableName());
      }
      assertEquals(actual.size(), 1);
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
  public void testFileFormatFactoryInvalid() {

    exception.expect(IllegalArgumentException.class);
    exception.expectMessage("Invalid output format:PARQUET. Supported output formats: TEXT, AVRO");

    SpannerChangeStreamsToGcsOptions options =
        PipelineOptionsFactory.create().as(SpannerChangeStreamsToGcsOptions.class);
    options.setOutputFileFormat(FileFormat.PARQUET);
    options.setOutputDirectory(fakeDir);
    options.setOutputFilenamePrefix(FILENAME_PREFIX);
    options.setNumShards(NUM_SHARDS);
    options.setTempLocation(fakeTempLocation);

    Pipeline p = Pipeline.create(options);

    Timestamp startTimestamp = Timestamp.now();
    Timestamp endTimestamp = Timestamp.now();

    p
        // Reads from the change stream
        .apply(
            SpannerIO.readChangeStream()
                .withSpannerConfig(
                    SpannerConfig.create()
                        .withProjectId("project")
                        .withInstanceId("instance")
                        .withDatabaseId("db"))
                .withMetadataInstance("instance")
                .withMetadataDatabase("db")
                .withChangeStreamName("changestream")
                .withInclusiveStartAt(startTimestamp)
                .withInclusiveEndAt(endTimestamp)
                .withRpcPriority(RpcPriority.HIGH))
        .apply(
            "Creating " + options.getWindowDuration() + " Window",
            Window.into(FixedWindows.of(DurationUtils.parseDuration(options.getWindowDuration()))))
        .apply(
            "Write To GCS",
            FileFormatFactorySpannerChangeStreams.newBuilder().setOptions(options).build());

    p.run();
  }

  @Test
  public void testInvalidWindowDuration() {
    exception.expect(IllegalArgumentException.class);
    exception.expectMessage("The window duration must be greater than 0!");
    SpannerChangeStreamsToGcsOptions options =
        PipelineOptionsFactory.create().as(SpannerChangeStreamsToGcsOptions.class);
    options.setOutputFileFormat(FileFormat.AVRO);
    options.setOutputDirectory(fakeDir);
    options.setOutputFilenamePrefix(FILENAME_PREFIX);
    options.setNumShards(NUM_SHARDS);
    options.setTempLocation(fakeTempLocation);
    options.setWindowDuration("invalidWindowDuration");

    Pipeline p = Pipeline.create(options);

    Timestamp startTimestamp = Timestamp.now();
    Timestamp endTimestamp = Timestamp.now();

    p
        // Reads from the change stream
        .apply(
            SpannerIO.readChangeStream()
                .withSpannerConfig(
                    SpannerConfig.create()
                        .withProjectId("project")
                        .withInstanceId("instance")
                        .withDatabaseId("db"))
                .withMetadataInstance("instance")
                .withMetadataDatabase("db")
                .withChangeStreamName("changestream")
                .withInclusiveStartAt(startTimestamp)
                .withInclusiveEndAt(endTimestamp)
                .withRpcPriority(RpcPriority.HIGH))
        .apply(
            "Creating " + options.getWindowDuration() + " Window",
            Window.into(FixedWindows.of(DurationUtils.parseDuration(options.getWindowDuration()))))
        .apply(
            "Write To GCS",
            FileFormatFactorySpannerChangeStreams.newBuilder().setOptions(options).build());

    p.run();
  }

  @Test
  @Category(IntegrationTest.class)
  // This test can only be run locally with the following command:
  // mvn -Dexcluded.spanner.tests="" -Dtest=SpannerChangeStreamsToGcsTest test
  // TODO(nancyxu): Add an integration test for writing into GCS text when the connector can be
  // run in parallel testing. Should happen after this PR is submitted:
  // https://github.com/apache/beam/pull/17036
  public void testWriteToGCSAvro() throws Exception {
    // Create a test database.
    String testDatabase = generateDatabaseName();
    fakeDir = tmpDir.newFolder("output").getAbsolutePath();
    fakeTempLocation = tmpDir.newFolder("temporaryLocation").getAbsolutePath();

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

    SpannerChangeStreamsToGcsOptions options =
        PipelineOptionsFactory.create().as(SpannerChangeStreamsToGcsOptions.class);
    options.setSpannerProjectId(TEST_PROJECT);
    options.setSpannerInstanceId(TEST_INSTANCE);
    options.setSpannerDatabaseId(testDatabase);
    options.setSpannerMetadataInstanceId(TEST_INSTANCE);
    options.setSpannerMetadataDatabaseId(testDatabase);
    options.setSpannerChangeStreamName(TEST_CHANGE_STREAM);

    options.setStartTimestamp(startTimestamp.toString());
    options.setEndTimestamp(endTimestamp.toString());
    List<String> experiments = new ArrayList<String>();
    options.setExperiments(experiments);

    options.setOutputFileFormat(FileFormat.AVRO);
    options.setOutputDirectory(fakeDir);
    options.setOutputFilenamePrefix(AVRO_FILENAME_PREFIX);
    options.setNumShards(NUM_SHARDS);
    options.setTempLocation(fakeTempLocation);

    // Run the pipeline.
    PipelineResult result = run(options);
    result.waitUntilFinish();

    // Read from the output Avro file to assert that 1 data change record has been generated.
    PCollection<DataChangeRecord> dataChangeRecords =
        pipeline.apply(
            "readRecords",
            AvroIO.read(DataChangeRecord.class).from(fakeDir + "/avro-output-*.avro"));
    PAssert.that(dataChangeRecords).satisfies(new VerifyDataChangeRecordAvro());
    pipeline.run();

    // Drop the database.
    spannerServer.dropDatabase(testDatabase);
  }
}
