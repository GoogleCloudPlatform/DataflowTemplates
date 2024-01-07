/*
 * Copyright (C) 2021 Google LLC
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

import com.google.cloud.teleport.v2.utils.DataplexJdbcPartitionUtils.PartitioningSchema;
import com.google.cloud.teleport.v2.utils.FileFormat.FileFormatOptions;
import com.google.cloud.teleport.v2.values.DataplexPartitionMetadata;
import com.google.common.collect.ImmutableList;
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
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** Test cases for the {@link GenericRecordsToGcsPartitioned} class. */
public class GenericRecordsToGcsPartitionedTest {

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

  @Rule public final transient TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule public final transient TestPipeline mainPipeline = TestPipeline.create();
  @Rule public final transient TestPipeline readPipeline = TestPipeline.create();

  @Test
  public void testMonthlyPartitioning() {
    Record record11 = new Record(SCHEMA);
    record11.put("x", true);
    record11.put("date", dateToEpochMillis(2010, 1));
    Record record12 = new Record(SCHEMA);
    record12.put("x", false);
    record12.put("date", dateToEpochMillis(2010, 1));
    Record record21 = new Record(SCHEMA);
    record21.put("x", true);
    record21.put("date", dateToEpochMillis(2010, 2));

    String tmpRootPath = temporaryFolder.getRoot().getAbsolutePath();

    PCollection<DataplexPartitionMetadata> result =
        mainPipeline
            .apply(
                Create.<GenericRecord>of(record11, record12, record21)
                    .withCoder(AvroCoder.of(SCHEMA)))
            .apply(
                "GenericRecordsToGCS",
                new GenericRecordsToGcsPartitioned(
                    tmpRootPath,
                    SERIALIZED_SCHEMA,
                    "date",
                    PartitioningSchema.MONTHLY,
                    FileFormatOptions.AVRO));

    PAssert.that(result)
        .containsInAnyOrder(
            DataplexPartitionMetadata.builder()
                .setLocation(tmpRootPath + "/year=2010/month=1")
                .setValues(ImmutableList.of("2010", "1"))
                .build(),
            DataplexPartitionMetadata.builder()
                .setLocation(tmpRootPath + "/year=2010/month=2")
                .setValues(ImmutableList.of("2010", "2"))
                .build());

    mainPipeline.run();

    verifyRecordsExists(tmpRootPath + "/year=2010/month=1/*", record11, record12);
    verifyRecordsExists(tmpRootPath + "/year=2010/month=2/*", record21);
  }

  @Test
  public void testDailyPartitioning() {
    Record record11 = new Record(SCHEMA);
    record11.put("x", true);
    record11.put("date", dateToEpochMillis(2010, 1, 1));
    Record record12 = new Record(SCHEMA);
    record12.put("x", false);
    record12.put("date", dateToEpochMillis(2010, 1, 1));
    Record record21 = new Record(SCHEMA);
    record21.put("x", true);
    record21.put("date", dateToEpochMillis(2010, 1, 2));
    Record record31 = new Record(SCHEMA);
    record31.put("x", true);
    record31.put("date", dateToEpochMillis(2010, 2, 1));

    String tmpRootPath = temporaryFolder.getRoot().getAbsolutePath();

    PCollection<DataplexPartitionMetadata> result =
        mainPipeline
            .apply(
                Create.<GenericRecord>of(record11, record12, record21, record31)
                    .withCoder(AvroCoder.of(SCHEMA)))
            .apply(
                "GenericRecordsToGCS",
                new GenericRecordsToGcsPartitioned(
                    tmpRootPath,
                    SERIALIZED_SCHEMA,
                    "date",
                    PartitioningSchema.DAILY,
                    FileFormatOptions.AVRO));

    PAssert.that(result)
        .containsInAnyOrder(
            DataplexPartitionMetadata.builder()
                .setLocation(tmpRootPath + "/year=2010/month=1/day=1")
                .setValues(ImmutableList.of("2010", "1", "1"))
                .build(),
            DataplexPartitionMetadata.builder()
                .setLocation(tmpRootPath + "/year=2010/month=1/day=2")
                .setValues(ImmutableList.of("2010", "1", "2"))
                .build(),
            DataplexPartitionMetadata.builder()
                .setLocation(tmpRootPath + "/year=2010/month=2/day=1")
                .setValues(ImmutableList.of("2010", "2", "1"))
                .build());

    mainPipeline.run();

    verifyRecordsExists(tmpRootPath + "/year=2010/month=1/day=1/*", record11, record12);
    verifyRecordsExists(tmpRootPath + "/year=2010/month=1/day=2/*", record21);
    verifyRecordsExists(tmpRootPath + "/year=2010/month=2/day=1/*", record31);
  }

  @Test
  public void testHourlyPartitioning() {
    Record record11 = new Record(SCHEMA);
    record11.put("x", true);
    record11.put("date", dateToEpochMillis(2010, 1, 1, 1));
    Record record12 = new Record(SCHEMA);
    record12.put("x", false);
    record12.put("date", dateToEpochMillis(2010, 1, 1, 1));
    Record record21 = new Record(SCHEMA);
    record21.put("x", true);
    record21.put("date", dateToEpochMillis(2010, 1, 1, 2));
    Record record31 = new Record(SCHEMA);
    record31.put("x", true);
    record31.put("date", dateToEpochMillis(2010, 1, 2, 1));

    String tmpRootPath = temporaryFolder.getRoot().getAbsolutePath();

    PCollection<DataplexPartitionMetadata> result =
        mainPipeline
            .apply(
                Create.<GenericRecord>of(record11, record12, record21, record31)
                    .withCoder(AvroCoder.of(SCHEMA)))
            .apply(
                "GenericRecordsToGCS",
                new GenericRecordsToGcsPartitioned(
                    tmpRootPath,
                    SERIALIZED_SCHEMA,
                    "date",
                    PartitioningSchema.HOURLY,
                    FileFormatOptions.AVRO));

    PAssert.that(result)
        .containsInAnyOrder(
            DataplexPartitionMetadata.builder()
                .setLocation(tmpRootPath + "/year=2010/month=1/day=1/hour=1")
                .setValues(ImmutableList.of("2010", "1", "1", "1"))
                .build(),
            DataplexPartitionMetadata.builder()
                .setLocation(tmpRootPath + "/year=2010/month=1/day=1/hour=2")
                .setValues(ImmutableList.of("2010", "1", "1", "2"))
                .build(),
            DataplexPartitionMetadata.builder()
                .setLocation(tmpRootPath + "/year=2010/month=1/day=2/hour=1")
                .setValues(ImmutableList.of("2010", "1", "2", "1"))
                .build());

    mainPipeline.run();

    verifyRecordsExists(tmpRootPath + "/year=2010/month=1/day=1/hour=1/*", record11, record12);
    verifyRecordsExists(tmpRootPath + "/year=2010/month=1/day=1/hour=2/*", record21);
    verifyRecordsExists(tmpRootPath + "/year=2010/month=1/day=2/hour=1/*", record31);
  }

  @Test
  public void testParquet() {
    Record record11 = new Record(SCHEMA);
    record11.put("x", true);
    record11.put("date", dateToEpochMillis(2010, 1));
    Record record12 = new Record(SCHEMA);
    record12.put("x", false);
    record12.put("date", dateToEpochMillis(2010, 1));

    String tmpRootPath = temporaryFolder.getRoot().getAbsolutePath();

    mainPipeline
        .apply(Create.<GenericRecord>of(record11, record12).withCoder(AvroCoder.of(SCHEMA)))
        .apply(
            "GenericRecordsToGCS",
            new GenericRecordsToGcsPartitioned(
                tmpRootPath,
                SERIALIZED_SCHEMA,
                "date",
                PartitioningSchema.MONTHLY,
                FileFormatOptions.PARQUET));

    mainPipeline.run();

    PCollection<GenericRecord> readAvroFile =
        readPipeline.apply(
            "ReadAvroFile",
            ParquetConverters.ReadParquetFile.newBuilder()
                .withInputFileSpec(tmpRootPath + "/year=2010/month=1/*")
                .withSerializedSchema(SERIALIZED_SCHEMA)
                .build());

    PAssert.that(readAvroFile).containsInAnyOrder(record11, record12);
    readPipeline.run();
  }

  @Test
  public void testNoPartitioning() {
    Record record11 = new Record(SCHEMA);
    record11.put("x", true);
    record11.put("date", dateToEpochMillis(2010, 1));
    Record record12 = new Record(SCHEMA);
    record12.put("x", false);
    record12.put("date", dateToEpochMillis(2010, 1));
    Record record21 = new Record(SCHEMA);
    record21.put("x", true);
    record21.put("date", dateToEpochMillis(2010, 2));

    String tmpRootPath = temporaryFolder.getRoot().getAbsolutePath();

    mainPipeline
        .apply(
            Create.<GenericRecord>of(record11, record12, record21).withCoder(AvroCoder.of(SCHEMA)))
        .apply(
            "GenericRecordsToGCS",
            new GenericRecordsToGcsPartitioned(
                tmpRootPath, SERIALIZED_SCHEMA, null, null, FileFormatOptions.AVRO));

    mainPipeline.run();

    verifyRecordsExists(tmpRootPath + "/*", record11, record12, record21);
  }

  private void verifyRecordsExists(String path, GenericRecord... expectedRecords) {
    PCollection<GenericRecord> readAvroFile =
        readPipeline.apply(
            "ReadAvroFile",
            AvroConverters.ReadAvroFile.newBuilder()
                .withInputFileSpec(path)
                .withSerializedSchema(SERIALIZED_SCHEMA)
                .build());

    PAssert.that(readAvroFile).containsInAnyOrder(expectedRecords);
    readPipeline.run();
  }

  private static long dateToEpochMillis(int year, int month) {
    return dateToEpochMillis(year, month, 1);
  }

  private static long dateToEpochMillis(int year, int month, int day) {
    return dateToEpochMillis(year, month, day, 1);
  }

  private static long dateToEpochMillis(int year, int month, int day, int hour) {
    return ZonedDateTime.of(year, month, day, hour, 42, 42, 42, ZoneOffset.UTC)
        .toInstant()
        .toEpochMilli();
  }
}
