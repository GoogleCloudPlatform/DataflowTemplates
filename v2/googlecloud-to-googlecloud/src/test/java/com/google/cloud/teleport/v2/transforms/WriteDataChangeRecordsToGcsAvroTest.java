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
package com.google.cloud.teleport.v2.transforms;

import com.google.cloud.Timestamp;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.ColumnType;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.DataChangeRecord;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.Mod;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.ModType;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.TypeCode;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.ValueCaptureType;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test cases for the {@link WriteDataChangeRecordsToGcsAvro} class. */
@RunWith(JUnit4.class)
public class WriteDataChangeRecordsToGcsAvroTest {
  /** Rule for pipeline testing. */
  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  /** Rule for exception testing. */
  @Rule public ExpectedException expectedException = ExpectedException.none();

  /** Rule for temporary folder storing output records. */
  @Rule public final TemporaryFolder tmpDir = new TemporaryFolder();

  private static final String AVRO_FILENAME_PREFIX = "avro-output-";
  private static final Integer NUM_SHARDS = 1;
  private static String fakeDir;
  private static String fakeTempLocation;
  private PipelineOptions options;

  @Before
  public void setUp() throws InterruptedException, IOException {
    options = TestPipeline.testingPipelineOptions();
    fakeDir = tmpDir.newFolder("output").getAbsolutePath();
    fakeTempLocation = tmpDir.newFolder("temporaryLocation").getAbsolutePath();
  }

  /** Test the basic WriteDataChangeRecordsToGcsAvro transform. */
  @Test
  public void testBasicWrite() {
    // First run the transform in a separate pipeline.
    final DataChangeRecord dataChangeRecord = createTestDataChangeRecord();
    Pipeline p = Pipeline.create(options);
    p.apply("CreateInput", Create.of(dataChangeRecord))
        .apply(
            "WriteTextFile(s)",
            WriteDataChangeRecordsToGcsAvro.newBuilder()
                .withGcsOutputDirectory(fakeDir)
                .withOutputFilenamePrefix(AVRO_FILENAME_PREFIX)
                .setNumShards(NUM_SHARDS)
                .withTempLocation(fakeTempLocation)
                .build());
    p.run();

    // Then, read the records back from the output directory using AvrioIO.read.
    PCollection<com.google.cloud.teleport.v2.DataChangeRecord> dataChangeRecords =
        pipeline.apply(
            "readRecords",
            AvroIO.read(com.google.cloud.teleport.v2.DataChangeRecord.class)
                .from(fakeDir + "/avro-output-GlobalWindow-pane-0-last-00-of-01.avro"));
    PAssert.that(dataChangeRecords)
        .containsInAnyOrder(WriteDataChangeRecordsToAvro.dataChangeRecordToAvro(dataChangeRecord));
    pipeline.run();
  }

  /**
   * Test whether {@link WriteDataChangeRecordsToGcsAvro} throws an exception if no output directory
   * is provided.
   */
  @Test
  public void testWriteWithoutOutputDirectory() {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage(
        "withGcsOutputDirectory(gcsOutputDirectory) called with null input.");

    WriteDataChangeRecordsToGcsAvro.newBuilder()
        .withGcsOutputDirectory(null)
        .withOutputFilenamePrefix(AVRO_FILENAME_PREFIX)
        .setNumShards(NUM_SHARDS)
        .withTempLocation(fakeTempLocation)
        .build();
  }

  /**
   * Test whether {@link WriteDataChangeRecordsToGcsAvro} throws an exception if temporary directory
   * is not provided.
   */
  @Test
  public void testWriteWithoutTempLocation() {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("withTempLocation(tempLocation) called with null input.");

    WriteDataChangeRecordsToGcsAvro.newBuilder()
        .withGcsOutputDirectory(fakeDir)
        .withOutputFilenamePrefix(AVRO_FILENAME_PREFIX)
        .setNumShards(NUM_SHARDS)
        .withTempLocation(null)
        .build();
  }

  private DataChangeRecord createTestDataChangeRecord() {
    return new DataChangeRecord(
        "partitionToken",
        Timestamp.ofTimeSecondsAndNanos(10L, 20),
        "serverTransactionId",
        true,
        "1",
        "tableName",
        Arrays.asList(
            new ColumnType("column1", new TypeCode("type1"), true, 1L),
            new ColumnType("column2", new TypeCode("type2"), false, 2L)),
        Collections.singletonList(
            new Mod(
                "{\"column1\": \"value1\"}",
                "{\"column2\": \"oldValue2\"}",
                "{\"column2\": \"newValue2\"}")),
        ModType.UPDATE,
        ValueCaptureType.OLD_AND_NEW_VALUES,
        10L,
        2L,
        "transactionTag",
        /*isSystemTransaction*/ false,
        null);
  }
}
