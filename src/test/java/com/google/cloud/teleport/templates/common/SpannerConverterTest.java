/*
 * Copyright (C) 2018 Google LLC
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
package com.google.cloud.teleport.templates.common;

import static com.google.cloud.teleport.templates.common.SpannerConverters.getTimestampBound;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.PartitionOptions;
import com.google.cloud.spanner.ReadOnlyTransaction;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.TimestampBound;
import com.google.cloud.spanner.Value;
import java.io.IOException;
import java.io.Serializable;
import java.nio.channels.ReadableByteChannel;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.UUID;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.gcp.spanner.ReadOperation;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link SpannerConverters}. */
@RunWith(JUnit4.class)
public class SpannerConverterTest implements Serializable {

  private static final String TABLE = "table";
  private static final String COLUMN_NAME = "id";
  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  private SpannerConverters.StructCsvPrinter structCsvPrinter =
      new SpannerConverters.StructCsvPrinter();

  /** Unit test for export transform. */
  @Test
  @Category(NeedsRunner.class)
  public void testSchemaSave() throws IOException {

    ValueProvider<String> table = ValueProvider.StaticValueProvider.of(TABLE);
    SpannerConfig spannerConfig = SpannerConfig.create();
    DatabaseClient databaseClient = mock(DatabaseClient.class, withSettings().serializable());
    ReadOnlyTransaction readOnlyTransaction =
        mock(ReadOnlyTransaction.class, withSettings().serializable());
    ResultSet resultSet = mock(ResultSet.class, withSettings().serializable());
    Struct struct = mock(Struct.class, withSettings().serializable());

    /*
     * Get a second earlier than current time to avoid tests failing due to time mismatch across
     * machines. A future timestamp is regarded as illegal when creating a timestamp bounded
     * transaction.
     */
    String instant = Instant.now().minus(1, ChronoUnit.SECONDS).toString();

    ValueProvider.StaticValueProvider<String> timestamp =
        ValueProvider.StaticValueProvider.of(instant);
    TimestampBound tsbound = getTimestampBound(instant);

    when(databaseClient.readOnlyTransaction(tsbound)).thenReturn(readOnlyTransaction);
    when(readOnlyTransaction.executeQuery(any(Statement.class))).thenReturn(resultSet);
    when(resultSet.next()).thenReturn(true).thenReturn(false);
    when(resultSet.getCurrentRowAsStruct()).thenReturn(struct);
    when(struct.getString(0)).thenReturn(COLUMN_NAME);
    when(struct.getString(1)).thenReturn("INT64");

    String schemaPath = "/tmp/" + UUID.randomUUID().toString();
    ValueProvider<String> textWritePrefix = ValueProvider.StaticValueProvider.of(schemaPath);
    SpannerConverters.ExportTransform exportTransform =
        SpannerConverters.ExportTransformFactory.create(
            table, spannerConfig, textWritePrefix, timestamp);
    exportTransform.setDatabaseClient(databaseClient);

    PCollection<ReadOperation> results = pipeline.apply("Create", exportTransform);
    ReadOperation readOperation =
        ReadOperation.create()
            .withQuery("SELECT `id` FROM `table`")
            .withPartitionOptions(PartitionOptions.newBuilder().setMaxPartitions(1000).build());
    PAssert.that(results).containsInAnyOrder(readOperation);
    pipeline.run();
    ReadableByteChannel channel =
        FileSystems.open(
            FileSystems.matchNewResource(
                schemaPath + SpannerConverters.ExportTransform.ExportFn.SCHEMA_SUFFIX, false));
    java.util.Scanner scanner = new java.util.Scanner(channel).useDelimiter("\\A");
    assertEquals("{\"id\":\"INT64\"}", scanner.next());
  }

  @Test
  public void testTwoFields() {
    assertEquals(
        "\"true\",\"test\"",
        structCsvPrinter.print(
            Struct.newBuilder()
                .set("bool")
                .to(Value.bool(true))
                .set("col")
                .to(Value.string("test"))
                .build()));
  }

  @Test
  public void testBoolean() {
    assertEquals(
        "\"true\"",
        structCsvPrinter.print(Struct.newBuilder().set("col").to(Value.bool(true)).build()));
  }

  @Test
  public void testString() {
    assertEquals(
        "\"test\"",
        structCsvPrinter.print(Struct.newBuilder().set("col").to(Value.string("test")).build()));
  }

  @Test
  public void testNull() {
    assertEquals(
        "", structCsvPrinter.print(Struct.newBuilder().set("col").to(Value.string(null)).build()));
  }

  @Test
  public void testEscapingString() {
    assertEquals(
        "\"\"\" ,;\"",
        structCsvPrinter.print(Struct.newBuilder().set("col").to(Value.string("\" ,;")).build()));
  }

  @Test
  public void testInt64() {
    assertEquals(
        "\"1\"", structCsvPrinter.print(Struct.newBuilder().set("col").to(Value.int64(1)).build()));
  }

  @Test
  public void testFloat() {
    assertEquals(
        "\"0.1\"",
        structCsvPrinter.print(Struct.newBuilder().set("col").to(Value.float64(0.1)).build()));
  }

  @Test
  public void testBytes() {
    assertEquals(
        "\"dGVzdA==\"",
        structCsvPrinter.print(
            Struct.newBuilder().set("col").to(Value.bytes(ByteArray.copyFrom("test"))).build()));
  }

  @Test
  public void testDate() {
    assertEquals(
        "\"2018-03-26\"",
        structCsvPrinter.print(
            Struct.newBuilder()
                .set("col")
                .to(Value.date(Date.fromYearMonthDay(2018, 3, 26)))
                .build()));
  }

  @Test
  public void testTimestamp() {
    assertEquals(
        "\"1970-01-01T00:00:00Z\"",
        structCsvPrinter.print(
            Struct.newBuilder()
                .set("col")
                .to(Value.timestamp(Timestamp.ofTimeMicroseconds(0)))
                .build()));
  }

  @Test
  public void testBooleanArray() {
    assertEquals(
        "\"[true]\"",
        structCsvPrinter.print(
            Struct.newBuilder().set("col").to(Value.boolArray(new boolean[] {true})).build()));
  }

  @Test
  public void testStringArray() {
    assertEquals(
        "\"[\"\"test\"\"]\"",
        structCsvPrinter.print(
            Struct.newBuilder()
                .set("col")
                .to(Value.stringArray(Collections.singletonList("test")))
                .build()));
  }

  @Test
  public void testNullArray() {
    assertEquals(
        "",
        structCsvPrinter.print(Struct.newBuilder().set("col").to(Value.stringArray(null)).build()));
  }

  @Test
  public void testStringArrayWithNull() {
    assertEquals(
        "\"[null]\"",
        structCsvPrinter.print(
            Struct.newBuilder()
                .set("col")
                .to(Value.stringArray(Collections.singletonList(null)))
                .build()));
  }

  @Test
  public void testStrintArrayWithEmptyString() {
    assertEquals(
        "\"[\"\"\"\"]\"",
        structCsvPrinter.print(
            Struct.newBuilder()
                .set("col")
                .to(Value.stringArray(Collections.singletonList("")))
                .build()));
  }

  @Test
  public void testInt64Array() {
    assertEquals(
        "\"[1]\"",
        structCsvPrinter.print(
            Struct.newBuilder().set("col").to(Value.int64Array(new long[] {1})).build()));
  }

  @Test
  public void testFloatArray() {
    assertEquals(
        "\"[0.1]\"",
        structCsvPrinter.print(
            Struct.newBuilder().set("col").to(Value.float64Array(new double[] {0.1})).build()));
  }

  @Test
  public void testDateArray() {
    assertEquals(
        "\"[\"\"2018-03-26\"\"]\"",
        structCsvPrinter.print(
            Struct.newBuilder()
                .set("col")
                .to(Value.dateArray(Collections.singletonList(Date.fromYearMonthDay(2018, 3, 26))))
                .build()));
  }

  @Test
  public void testTimestampArray() {
    assertEquals(
        "\"[\"\"1970-01-01T00:00:00Z\"\"]\"",
        structCsvPrinter.print(
            Struct.newBuilder()
                .set("col")
                .to(
                    Value.timestampArray(
                        Collections.singletonList(Timestamp.ofTimeMicroseconds(0))))
                .build()));
  }

  @Test
  public void testBytesArray() {
    assertEquals(
        "\"[\"\"dGVzdA==\"\"]\"",
        structCsvPrinter.print(
            Struct.newBuilder()
                .set("col")
                .to(Value.bytesArray(Collections.singletonList(ByteArray.copyFrom("test"))))
                .build()));
  }
}
