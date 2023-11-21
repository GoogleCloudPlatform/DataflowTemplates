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
package com.google.cloud.teleport.v2.utils;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import com.google.cloud.teleport.v2.utils.DataplexJdbcPartitionUtils.PartitioningSchema;
import com.google.cloud.teleport.v2.utils.FileFormat.FileFormatOptions;
import com.google.cloud.teleport.v2.utils.JdbcIngestionWriteDisposition.WriteDispositionException;
import com.google.cloud.teleport.v2.utils.JdbcIngestionWriteDisposition.WriteDispositionOptions;
import java.io.File;
import java.io.IOException;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link DataplexJdbcIngestionFilter}. */
@RunWith(JUnit4.class)
public final class DataplexJdbcIngestionFilterTest {
  private static final String SERIALIZED_SCHEMA_TEMPLATE =
      "{"
          + "\"name\": \"Schema\","
          + "\"type\": \"record\","
          + "\"fields\": ["
          + "   {\"name\": \"x\", \"type\": \"boolean\"},"
          + "   {\"name\": \"date\", \"type\": {\"type\": \"%s\", \"logicalType\": \"%s\"}}"
          + "]"
          + "}";
  private static final String SERIALIZED_SCHEMA =
      String.format(SERIALIZED_SCHEMA_TEMPLATE, "long", "timestamp-millis");
  private static final Schema SCHEMA = new Parser().parse(SERIALIZED_SCHEMA);
  private static final String PARTITION_COLUMN_NAME = "date";
  private static final TupleTag<GenericRecord> FILTERED_RECORDS_OUT =
      new TupleTag<GenericRecord>() {};
  private static final TupleTag<String> EXISTING_TARGET_FILES_OUT = new TupleTag<String>() {};
  private Record record11;
  private Record record12;
  private Record record21;

  @Rule public final transient TemporaryFolder temporaryFolder = new TemporaryFolder();
  @Rule public final transient TestPipeline mainPipeline = TestPipeline.create();

  @Before
  public void setUp() throws IOException {
    record11 = new Record(SCHEMA);
    record11.put("x", true);
    record11.put("date", dateToEpochMillis(2010, 1));
    record12 = new Record(SCHEMA);
    record12.put("x", false);
    record12.put("date", dateToEpochMillis(2010, 1));
    record21 = new Record(SCHEMA);
    record21.put("x", true);
    record21.put("date", dateToEpochMillis(2010, 2));
    File outputDir1 = temporaryFolder.newFolder("year=2010", "month=1");
    File outputFile1 = new File(outputDir1.getAbsolutePath() + "/" + "output-00000-of-00001.avro");
    outputFile1.createNewFile();
  }

  @Test
  public void testRecordOverwriteIfTargetFileExists() {
    String targetRootPath = temporaryFolder.getRoot().getAbsolutePath();
    PCollectionTuple result =
        mainPipeline
            .apply(
                Create.<GenericRecord>of(record11, record12, record21)
                    .withCoder(AvroCoder.of(SCHEMA)))
            .apply(
                new DataplexJdbcIngestionFilter(
                    targetRootPath,
                    SERIALIZED_SCHEMA,
                    PARTITION_COLUMN_NAME,
                    PartitioningSchema.MONTHLY,
                    FileFormatOptions.AVRO.getFileSuffix(),
                    WriteDispositionOptions.WRITE_TRUNCATE,
                    GCSUtils.getFilesInDirectory(targetRootPath),
                    FILTERED_RECORDS_OUT,
                    EXISTING_TARGET_FILES_OUT));
    PAssert.that(result.get(FILTERED_RECORDS_OUT)).containsInAnyOrder(record11, record12, record21);
    // Contains the same filename two times since 2 records map to the same partition whose file
    // exists
    PAssert.that(result.get(EXISTING_TARGET_FILES_OUT))
        .containsInAnyOrder(
            "year=2010/month=1/output-00000-of-00001.avro",
            "year=2010/month=1/output-00000-of-00001.avro");
    mainPipeline.run();
  }

  @Test
  public void testRecordSkippedIfTargetFileExists() {
    String targetRootPath = temporaryFolder.getRoot().getAbsolutePath();
    PCollectionTuple result =
        mainPipeline
            .apply(
                Create.<GenericRecord>of(record11, record12, record21)
                    .withCoder(AvroCoder.of(SCHEMA)))
            .apply(
                new DataplexJdbcIngestionFilter(
                    targetRootPath,
                    SERIALIZED_SCHEMA,
                    PARTITION_COLUMN_NAME,
                    PartitioningSchema.MONTHLY,
                    FileFormatOptions.AVRO.getFileSuffix(),
                    WriteDispositionOptions.SKIP,
                    GCSUtils.getFilesInDirectory(targetRootPath),
                    FILTERED_RECORDS_OUT,
                    EXISTING_TARGET_FILES_OUT));
    PAssert.that(result.get(FILTERED_RECORDS_OUT)).containsInAnyOrder(record21);
    // Contains the same filename two times since 2 records map to the same partition whose file
    // exists
    PAssert.that(result.get(EXISTING_TARGET_FILES_OUT))
        .containsInAnyOrder(
            "year=2010/month=1/output-00000-of-00001.avro",
            "year=2010/month=1/output-00000-of-00001.avro");
    mainPipeline.run();
  }

  @Test
  public void testFailIfTargetFileExists() {
    String targetRootPath = temporaryFolder.getRoot().getAbsolutePath();
    PCollectionTuple result =
        mainPipeline
            .apply(
                Create.<GenericRecord>of(record11, record12, record21)
                    .withCoder(AvroCoder.of(SCHEMA)))
            .apply(
                new DataplexJdbcIngestionFilter(
                    targetRootPath,
                    SERIALIZED_SCHEMA,
                    PARTITION_COLUMN_NAME,
                    PartitioningSchema.MONTHLY,
                    FileFormatOptions.AVRO.getFileSuffix(),
                    WriteDispositionOptions.WRITE_EMPTY,
                    GCSUtils.getFilesInDirectory(targetRootPath),
                    FILTERED_RECORDS_OUT,
                    EXISTING_TARGET_FILES_OUT));
    try {
      mainPipeline.run();
      fail("Expected a WriteDispositionException.");
    } catch (Exception e) {
      assertThat(e).hasCauseThat().isInstanceOf(WriteDispositionException.class);
    }
  }

  private static long dateToEpochMillis(int year, int month) {
    return ZonedDateTime.of(year, month, 1, 1, 42, 42, 42, ZoneOffset.UTC)
        .toInstant()
        .toEpochMilli();
  }
}
