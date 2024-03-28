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
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.PartitionOptions;
import com.google.cloud.spanner.ReadOnlyTransaction;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.TimestampBound;
import com.google.cloud.spanner.Value;
import com.google.cloud.teleport.templates.common.SpannerConverters.StructValidator;
import com.google.cloud.teleport.templates.common.SpannerConverters.VectorSearchStructValidator;
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
  private static final Dialect POSTGRESQL = Dialect.POSTGRESQL;
  private static final Dialect GOOGLE_STANDARD_SQL = Dialect.GOOGLE_STANDARD_SQL;
  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  private SpannerConverters.StructCsvPrinter structCsvPrinter =
      new SpannerConverters.StructCsvPrinter();

  private final SpannerConverters.StructJSONPrinter structJSONPrinter =
      new SpannerConverters.StructJSONPrinter();

  private StructValidator vectorSearchStructValidator = new VectorSearchStructValidator();

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
    when(resultSet.next()).thenReturn(true).thenReturn(true).thenReturn(false);
    when(resultSet.getCurrentRowAsStruct()).thenReturn(struct);
    when(struct.getString(0)).thenReturn(COLUMN_NAME);
    when(struct.getString(1)).thenReturn("INT64");
    when(databaseClient.getDialect()).thenReturn(GOOGLE_STANDARD_SQL);

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
  @Category(NeedsRunner.class)
  public void testPgSchemaSave() throws IOException {

    ValueProvider<String> table = ValueProvider.StaticValueProvider.of(TABLE);
    SpannerConfig spannerConfig = SpannerConfig.create().withDatabaseId("test-db");
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
    when(resultSet.next()).thenReturn(true).thenReturn(true).thenReturn(false);
    when(resultSet.getCurrentRowAsStruct()).thenReturn(struct);
    when(struct.getString(0)).thenReturn(COLUMN_NAME);
    when(struct.getString(1)).thenReturn("bigint");
    when(databaseClient.getDialect()).thenReturn(POSTGRESQL);

    String schemaPath = "/tmp/" + UUID.randomUUID().toString();
    ValueProvider<String> textWritePrefix = ValueProvider.StaticValueProvider.of(schemaPath);
    SpannerConverters.ExportTransform exportTransform =
        SpannerConverters.ExportTransformFactory.create(
            table, spannerConfig, textWritePrefix, timestamp);
    exportTransform.setDatabaseClient(databaseClient);

    PCollection<ReadOperation> results = pipeline.apply("Create", exportTransform);
    ReadOperation readOperation =
        ReadOperation.create()
            .withQuery("SELECT \"id\" FROM \"table\";")
            .withPartitionOptions(PartitionOptions.newBuilder().setMaxPartitions(1000).build());
    PAssert.that(results).containsInAnyOrder(readOperation);
    pipeline.run();
    ReadableByteChannel channel =
        FileSystems.open(
            FileSystems.matchNewResource(
                schemaPath + SpannerConverters.ExportTransform.ExportFn.SCHEMA_SUFFIX, false));
    java.util.Scanner scanner = new java.util.Scanner(channel).useDelimiter("\\A");
    assertEquals("{\"id\":\"bigint\"}", scanner.next());
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
  public void testFloat32() {
    assertEquals(
        "\"0.1\"",
        structCsvPrinter.print(Struct.newBuilder().set("col").to(Value.float32(0.1f)).build()));
  }

  @Test
  public void testFloat64() {
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
  public void testPgNumeric() {
    assertEquals(
        "\"-25398514232141142.0014578090\"",
        structCsvPrinter.print(
            Struct.newBuilder()
                .set("col")
                .to(Value.pgNumeric("-25398514232141142.0014578090"))
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
  public void testStringArrayWithEmptyString() {
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
  public void testFloat32Array() {
    assertEquals(
        "\"[0.1]\"",
        structCsvPrinter.print(
            Struct.newBuilder().set("col").to(Value.float32Array(new float[] {0.1f})).build()));
  }

  @Test
  public void testFloat64Array() {
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

  @Test
  public void testPgNumericArray() {
    assertEquals(
        "\"[\"\"-25398514232141142.0014578090\"\"]\"",
        structCsvPrinter.print(
            Struct.newBuilder()
                .set("col")
                .to(
                    Value.pgNumericArray(
                        Collections.singletonList("-25398514232141142.0014578090")))
                .build()));
  }

  @Test
  public void testNullWithJSONPrinter() {
    assertEquals(
        "{}",
        structJSONPrinter.print(Struct.newBuilder().set("col").to(Value.string(null)).build()));
  }

  @Test
  public void testInt64WithJSONPrinter() {
    assertEquals(
        "{\"col\":\"1\"}",
        structJSONPrinter.print(Struct.newBuilder().set("col").to(Value.int64(1)).build()));
  }

  @Test
  public void testFloat32WithJSONPrinter() {
    assertEquals(
        "{\"col\":\"0.1\"}",
        structJSONPrinter.print(Struct.newBuilder().set("col").to(Value.float32(0.1f)).build()));
  }

  @Test
  public void testFloat64WithJSONPrinter() {
    assertEquals(
        "{\"col\":\"0.1\"}",
        structJSONPrinter.print(Struct.newBuilder().set("col").to(Value.float64(0.1)).build()));
  }

  @Test
  public void testBytesWithJSONPrinter() {
    assertEquals(
        "{\"col\":\"dGVzdA==\"}",
        structJSONPrinter.print(
            Struct.newBuilder().set("col").to(Value.bytes(ByteArray.copyFrom("test"))).build()));
  }

  @Test
  public void testJsonWithJSONPrinter() {
    assertEquals(
        "{\"col\":[{\"allow_list\": [\"even\"], \"namespace\": \"class\"}]}",
        structJSONPrinter.print(
            Struct.newBuilder()
                .set("col")
                .to(Value.json("[{\"allow_list\": [\"even\"], \"namespace\": \"class\"}]"))
                .build()));
  }

  @Test
  public void testDateWithJSONPrinter() {
    assertEquals(
        "{\"col\":\"2018-03-26\"}",
        structJSONPrinter.print(
            Struct.newBuilder()
                .set("col")
                .to(Value.date(Date.fromYearMonthDay(2018, 3, 26)))
                .build()));
  }

  @Test
  public void testTimestampWithJSONPrinter() {
    assertEquals(
        "{\"col\":\"1970-01-01T00:00:00Z\"}",
        structJSONPrinter.print(
            Struct.newBuilder()
                .set("col")
                .to(Value.timestamp(Timestamp.ofTimeMicroseconds(0)))
                .build()));
  }

  @Test
  public void testPgNumericWithJSONPrinter() {
    assertEquals(
        "{\"col\":\"-25398514232141142.0014578090\"}",
        structJSONPrinter.print(
            Struct.newBuilder()
                .set("col")
                .to(Value.pgNumeric("-25398514232141142.0014578090"))
                .build()));
  }

  @Test
  public void testBooleanArrayWithJSONPrinter() {
    assertEquals(
        "{\"col\":[\"true\"]}",
        structJSONPrinter.print(
            Struct.newBuilder().set("col").to(Value.boolArray(new boolean[] {true})).build()));
  }

  @Test
  public void testStringArrayWithJSONPrinter() {
    assertEquals(
        "{\"col\":[\"test\"]}",
        structJSONPrinter.print(
            Struct.newBuilder()
                .set("col")
                .to(Value.stringArray(Collections.singletonList("test")))
                .build()));
  }

  @Test
  public void testStringArrayWithNullWithJSONPrinter() {
    assertEquals(
        "{\"col\":[null]}",
        structJSONPrinter.print(
            Struct.newBuilder()
                .set("col")
                .to(Value.stringArray(Collections.singletonList(null)))
                .build()));
  }

  @Test
  public void testStringArrayWithEmptyStringWithJSONPrinter() {
    assertEquals(
        "{\"col\":[\"\"]}",
        structJSONPrinter.print(
            Struct.newBuilder()
                .set("col")
                .to(Value.stringArray(Collections.singletonList("")))
                .build()));
  }

  @Test
  public void testInt64ArrayWithJSONPrinter() {
    assertEquals(
        "{\"col\":[\"1\"]}",
        structJSONPrinter.print(
            Struct.newBuilder().set("col").to(Value.int64Array(new long[] {1})).build()));
  }

  @Test
  public void testFloat32ArrayWithJSONPrinter() {
    assertEquals(
        "{\"col\":[\"0.1\"]}",
        structJSONPrinter.print(
            Struct.newBuilder().set("col").to(Value.float32Array(new float[] {0.1f})).build()));
  }

  @Test
  public void testFloat64ArrayWithJSONPrinter() {
    assertEquals(
        "{\"col\":[\"0.1\"]}",
        structJSONPrinter.print(
            Struct.newBuilder().set("col").to(Value.float64Array(new double[] {0.1})).build()));
  }

  @Test
  public void testDateArrayWithJSONPrinter() {
    assertEquals(
        "{\"col\":[\"2018-03-26\"]}",
        structJSONPrinter.print(
            Struct.newBuilder()
                .set("col")
                .to(Value.dateArray(Collections.singletonList(Date.fromYearMonthDay(2018, 3, 26))))
                .build()));
  }

  @Test
  public void testTimestampArrayWithJSONPrinter() {
    assertEquals(
        "{\"col\":[\"1970-01-01T00:00:00Z\"]}",
        structJSONPrinter.print(
            Struct.newBuilder()
                .set("col")
                .to(
                    Value.timestampArray(
                        Collections.singletonList(Timestamp.ofTimeMicroseconds(0))))
                .build()));
  }

  @Test
  public void testBytesArrayWithJSONPrinter() {
    assertEquals(
        "{\"col\":[\"dGVzdA==\"]}",
        structJSONPrinter.print(
            Struct.newBuilder()
                .set("col")
                .to(Value.bytesArray(Collections.singletonList(ByteArray.copyFrom("test"))))
                .build()));
  }

  @Test
  public void testPgNumericArrayWithJSONPrinter() {
    assertEquals(
        "{\"col\":[\"-25398514232141142.0014578090\"]}",
        structJSONPrinter.print(
            Struct.newBuilder()
                .set("col")
                .to(
                    Value.pgNumericArray(
                        Collections.singletonList("-25398514232141142.0014578090")))
                .build()));
  }

  @Test
  public void testVectorSearchValidator() {
    vectorSearchStructValidator.validate(
        Struct.newBuilder()
            .set("id")
            .to(Value.int64(323232))
            .set("embedding")
            .to(Value.float64Array(new double[] {3223, 232}))
            .build());
  }

  @Test
  public void testVectorSearchValidatorWithRestricts() {
    vectorSearchStructValidator.validate(
        Struct.newBuilder()
            .set("id")
            .to(Value.int64(323232))
            .set("restricts")
            .to(Value.json("[{\"allow_list\": [\"even\"], \"namespace\": \"class\"}]"))
            .set("embedding")
            .to(Value.float64Array(new double[] {3223, 232}))
            .build());
  }

  @Test
  public void testVectorSearchValidatorIDError() {
    assertThrows(
        RuntimeException.class,
        () ->
            vectorSearchStructValidator.validate(
                Struct.newBuilder()
                    .set("embedding")
                    .to(Value.float64Array(new double[] {3223, 232}))
                    .build()));
  }

  @Test
  public void testVectorSearchValidatorEmbeddingError() {
    assertThrows(
        RuntimeException.class,
        () ->
            vectorSearchStructValidator.validate(
                Struct.newBuilder().set("id").to(Value.int64(323232)).build()));
  }

  @Test
  public void testPrepareColumnExpressionWithAlias() {
    assertEquals(
        "column1 AS alias1,column2 AS alias2,column3",
        SpannerConverters.prepareColumnExpression("column1:alias1,column2:alias2, column3"));
  }

  @Test
  public void testPrepareColumnExpression() {
    assertEquals(
        "column1,column2,column3",
        SpannerConverters.prepareColumnExpression("column1, column2 , column3"));
  }

  @Test
  public void testPrepareColumnExpressionError() {
    assertThrows(
        IllegalArgumentException.class,
        () -> SpannerConverters.prepareColumnExpression("column1,,column2"));
  }
}
