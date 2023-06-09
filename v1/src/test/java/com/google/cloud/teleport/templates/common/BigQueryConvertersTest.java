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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.teleport.coders.FailsafeElementCoder;
import com.google.cloud.teleport.templates.common.BigQueryConverters.AvroToEntity;
import com.google.cloud.teleport.templates.common.BigQueryConverters.FailsafeJsonToTableRow;
import com.google.cloud.teleport.values.FailsafeElement;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Value;
import com.google.protobuf.NullValue;
import com.google.protobuf.util.Timestamps;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.beam.sdk.coders.Coder.Context;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.bigquery.SchemaAndRecord;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessageWithAttributesCoder;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link BigQueryConverters}. */
@RunWith(JUnit4.class)
public class BigQueryConvertersTest {

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();
  @Rule public ExpectedException expectedException = ExpectedException.none();

  // Define the TupleTag's here otherwise the anonymous class will force the test method to
  // be serialized.
  private static final TupleTag<TableRow> TABLE_ROW_TAG = new TupleTag<TableRow>() {};

  private static final TupleTag<FailsafeElement<PubsubMessage, String>> FAILSAFE_ELM_TAG =
      new TupleTag<FailsafeElement<PubsubMessage, String>>() {};

  private ValueProvider<String> entityKind = StaticValueProvider.of("TestEntity");
  private ValueProvider<String> uniqueNameColumn = StaticValueProvider.of("id");
  private ValueProvider<String> namespace = StaticValueProvider.of("bq-to-ds-test");
  private AvroToEntity converter =
      AvroToEntity.newBuilder()
          .setEntityKind(entityKind)
          .setUniqueNameColumn(uniqueNameColumn)
          .setNamespace(namespace)
          .build();
  private String avroSchemaTemplate =
      new StringBuilder()
          .append("{")
          .append(" \"type\" : \"record\",")
          .append(" \"name\" : \"BigQueryTestData\",")
          .append(" \"namespace\" : \"\",")
          .append(" \"fields\" :")
          .append("  [%s],")
          .append(" \"doc:\" : \"A basic Avro schema for unit testing purposes\"")
          .append("}")
          .toString();
  private String avroFieldTemplate =
      new StringBuilder()
          .append("{")
          .append(" \"name\" : \"%s\",")
          .append(" \"type\" : \"%s\",")
          .append(" \"doc\"  : \"%s\"")
          .append("}")
          .toString();
  private String idField = "id";
  private String idFieldDesc = "Unique identifier";
  private int idFieldValueInt = 87234;
  private String idFieldValueStr = "87234";
  private String nullField = "comment";
  private String nullFieldDesc = "Comment";
  private String shortStringField = "author";
  private String shortStringFieldDesc = "Author name";
  private String shortStringFieldValue = "Morgan le Fay";
  private String longStringField = "excerpt";
  private String longStringFieldDesc = "Excerpt from the article";
  private String longStringFieldValue = Strings.repeat("dignissimos", 5000);
  private String integerField = "year";
  private String integerFieldDesc = "Publication year";
  private long integerFieldValue = 2013L;
  private String int64Field = "year_64";
  private String int64FieldDesc = "Publication year (64)";
  private long int64FieldValue = 2015L;
  private String floatField = "price";
  private String floatFieldDesc = "Price";
  private double floatFieldValue = 47.89;
  private String float64Field = "price_64";
  private String float64FieldDesc = "Price (64)";
  private double float64FieldValue = 173.45;
  private String booleanField = "available";
  private String booleanFieldDesc = "Available?";
  private boolean booleanFieldValue = true;
  private String boolField = "borrowable";
  private String boolFieldDesc = "Can be borrowed?";
  private boolean boolFieldValue = false;
  private String validTimestampField = "date_ts";
  private String validTimestampFieldDesc = "Publication date (ts)";
  private long validTimestampFieldValueMicros = 1376954900000567L;
  private long validTimestampFieldValueMillis = 1376954900000L;
  private String invalidTimestampField = "date_ts_invalid";
  private String invalidTimestampFieldDesc = "Expiration date (ts)";
  private long invalidTimestampFieldValueNanos = 1376954900000567000L;
  private String dateField = "date";
  private String dateFieldDesc = "Publication day";
  private String dateFieldValue = "2013-08-19";
  private String timeField = "time";
  private String timeFieldDesc = "Publication time";
  private String timeFieldValue = "23:28:20.000567";
  private String dateTimeField = "full_date";
  private String dateTimeFieldDesc = "Full publication date";
  private String dateTimeFieldValue = "2013-08-19 23:28:20.000567";

  /**
   * Tests {@link BigQueryConverters.JsonToTableRow} converts a valid Json TableRow to a TableRow.
   */
  @Test
  @Category(NeedsRunner.class)
  public void testJsonToTableRowGood() throws Exception {
    TableRow expectedRow = new TableRow();
    expectedRow.set("location", "nyc");
    expectedRow.set("name", "Adam");
    expectedRow.set("age", 30);
    expectedRow.set("color", "blue");
    expectedRow.set("coffee", "black");

    ByteArrayOutputStream jsonStream = new ByteArrayOutputStream();
    TableRowJsonCoder.of().encode(expectedRow, jsonStream, Context.OUTER);
    String expectedJson = new String(jsonStream.toByteArray(), StandardCharsets.UTF_8.name());

    PCollection<TableRow> transformedJson =
        pipeline
            .apply("Create", Create.of(expectedJson))
            .apply(BigQueryConverters.jsonToTableRow());

    PAssert.that(transformedJson).containsInAnyOrder(expectedRow);

    pipeline.run();
  }

  /** Tests the {@link BigQueryConverters.FailsafeJsonToTableRow} transform with good input. */
  @Test
  @Category(NeedsRunner.class)
  public void testFailsafeJsonToTableRowValidInput() {
    // Test input
    final String payload = "{\"ticker\": \"GOOGL\", \"price\": 1006.94}";
    final Map<String, String> attributes = ImmutableMap.of("id", "0xDb12", "type", "stock");
    final PubsubMessage message = new PubsubMessage(payload.getBytes(), attributes);

    final FailsafeElement<PubsubMessage, String> input = FailsafeElement.of(message, payload);

    // Expected Output
    TableRow expectedRow = new TableRow().set("ticker", "GOOGL").set("price", 1006.94);

    // Register the coder for the pipeline. This prevents having to invoke .setCoder() on
    // many transforms.
    FailsafeElementCoder<PubsubMessage, String> coder =
        FailsafeElementCoder.of(PubsubMessageWithAttributesCoder.of(), StringUtf8Coder.of());

    CoderRegistry coderRegistry = pipeline.getCoderRegistry();
    coderRegistry.registerCoderForType(coder.getEncodedTypeDescriptor(), coder);

    // Build the pipeline
    PCollectionTuple output =
        pipeline
            .apply("CreateInput", Create.of(input).withCoder(coder))
            .apply(
                "JsonToTableRow",
                FailsafeJsonToTableRow.<PubsubMessage>newBuilder()
                    .setSuccessTag(TABLE_ROW_TAG)
                    .setFailureTag(FAILSAFE_ELM_TAG)
                    .build());

    // Assert
    PAssert.that(output.get(TABLE_ROW_TAG)).containsInAnyOrder(expectedRow);
    PAssert.that(output.get(FAILSAFE_ELM_TAG)).empty();

    // Execute the test
    pipeline.run();
  }

  /**
   * Tests the {@link BigQueryConverters.FailsafeJsonToTableRow} transform with invalid JSON input.
   */
  @Test
  @Category(NeedsRunner.class)
  public void testFailsafeJsonToTableRowInvalidJSON() {
    // Test input
    final String payload = "{\"ticker\": \"GOOGL\", \"price\": 1006.94";
    final Map<String, String> attributes = ImmutableMap.of("id", "0xDb12", "type", "stock");
    final PubsubMessage message = new PubsubMessage(payload.getBytes(), attributes);

    final FailsafeElement<PubsubMessage, String> input = FailsafeElement.of(message, payload);

    // Register the coder for the pipeline. This prevents having to invoke .setCoder() on
    // many transforms.
    FailsafeElementCoder<PubsubMessage, String> coder =
        FailsafeElementCoder.of(PubsubMessageWithAttributesCoder.of(), StringUtf8Coder.of());

    CoderRegistry coderRegistry = pipeline.getCoderRegistry();
    coderRegistry.registerCoderForType(coder.getEncodedTypeDescriptor(), coder);

    // Build the pipeline
    PCollectionTuple output =
        pipeline
            .apply("CreateInput", Create.of(input).withCoder(coder))
            .apply(
                "JsonToTableRow",
                FailsafeJsonToTableRow.<PubsubMessage>newBuilder()
                    .setSuccessTag(TABLE_ROW_TAG)
                    .setFailureTag(FAILSAFE_ELM_TAG)
                    .build());

    // Assert
    PAssert.that(output.get(TABLE_ROW_TAG)).empty();
    PAssert.that(output.get(FAILSAFE_ELM_TAG))
        .satisfies(
            collection -> {
              final FailsafeElement<PubsubMessage, String> result = collection.iterator().next();
              // Check the individual elements of the PubsubMessage since the message above won't be
              // serializable.
              assertThat(
                  new String(result.getOriginalPayload().getPayload()), is(equalTo(payload)));
              assertThat(result.getOriginalPayload().getAttributeMap(), is(equalTo(attributes)));
              assertThat(result.getPayload(), is(equalTo(payload)));
              assertThat(result.getErrorMessage(), is(notNullValue()));
              assertThat(result.getStacktrace(), is(notNullValue()));
              return null;
            });

    // Execute the test
    pipeline.run();
  }

  /**
   * Tests that BigQueryConverters.validateKeyColumn() throws IllegalArgumentException when the
   * BigQuery column NULL.
   */
  @Test
  public void testValidateKeyColumnNull() {
    TableFieldSchema column = new TableFieldSchema().setName(nullField).setType("STRING");
    Record record = generateSingleFieldAvroRecord(nullField, "null", nullFieldDesc, null);
    boolean isThrown = false;
    String message = null;
    try {
      BigQueryConverters.validateKeyColumn(column, record.get(nullField));
    } catch (IllegalArgumentException e) {
      isThrown = true;
      message = e.getMessage();
    }
    assertTrue(isThrown);
    assertTrue(message != null);
    assertEquals(
        message,
        String.format("Column [%s] with NULL value cannot be set as Entity name.", nullField));
  }

  /**
   * Tests that BigQueryConverters.validateKeyColumn() throws IllegalArgumentException when the
   * BigQuery column is a STRING exceeding 1500 bytes.
   */
  @Test
  public void testValidateKeyColumnStringLong() {
    TableFieldSchema column = new TableFieldSchema().setName(longStringField).setType("STRING");
    Record record =
        generateSingleFieldAvroRecord(
            longStringField, "string", longStringFieldDesc, longStringFieldValue);
    boolean isThrown = false;
    String message = null;
    try {
      BigQueryConverters.validateKeyColumn(column, record.get(longStringField));
    } catch (IllegalArgumentException e) {
      isThrown = true;
      message = e.getMessage();
    }
    assertTrue(isThrown);
    assertTrue(message != null);
    assertEquals(
        message,
        String.format(
            "Column [%s] exceeding %d bytes cannot be set as Entity name.",
            longStringField, BigQueryConverters.MAX_STRING_SIZE_BYTES));
  }

  /**
   * Tests that BigQueryConverters.validateKeyColumn() throws IllegalArgumentException when the
   * BigQuery column is a RECORD.
   */
  @Test
  public void testValidateKeyColumnRecord() {
    TableFieldSchema column = generateNestedTableFieldSchema();
    Record record = generateNestedAvroRecord();
    boolean isThrown = false;
    String message = null;
    try {
      BigQueryConverters.validateKeyColumn(column, record.get("address"));
    } catch (IllegalArgumentException e) {
      isThrown = true;
      message = e.getMessage();
    }
    assertTrue(isThrown);
    assertTrue(message != null);
    assertEquals(message, "Column [address] of type RECORD cannot be set as Entity name.");
  }

  /**
   * Tests that BigQueryConverters.validateKeyColumn() does not throw IllegalArgumentException when
   * the BigQuery column is a STRING under 1500 bytes.
   */
  @Test
  public void testValidateKeyColumnStringShort() {
    TableFieldSchema column = new TableFieldSchema().setName(shortStringField).setType("STRING");
    Record record =
        generateSingleFieldAvroRecord(
            shortStringField, "string", shortStringFieldDesc, shortStringFieldValue);
    boolean isThrown = false;
    try {
      BigQueryConverters.validateKeyColumn(column, record.get(shortStringField));
    } catch (IllegalArgumentException e) {
      isThrown = true;
    }
    assertFalse(isThrown);
  }

  /**
   * Tests that BigQueryConverters.columnToValue() returns a null {@link Value} when the BigQuery
   * column is null.
   */
  @Test
  public void testColumnToValueNull() {
    TableFieldSchema column = new TableFieldSchema().setName(nullField).setType("STRING");
    Record record = generateSingleFieldAvroRecord(nullField, "null", nullFieldDesc, null);
    Value value = BigQueryConverters.columnToValue(column, record.get(nullField));
    assertEquals(NullValue.NULL_VALUE, value.getNullValue());
  }

  /**
   * Tests that BigQueryConverters.columnToValue() returns an indexed String {@link Value} when the
   * BigQuery column is a STRING of less than 1500 bytes.
   */
  @Test
  public void testColumnToValueStringShort() {
    TableFieldSchema column = new TableFieldSchema().setName(shortStringField).setType("STRING");
    Record record =
        generateSingleFieldAvroRecord(
            shortStringField, "string", shortStringFieldDesc, shortStringFieldValue);
    Value value = BigQueryConverters.columnToValue(column, record.get(shortStringField));
    assertEquals(shortStringFieldValue, value.getStringValue());
    assertFalse(value.getExcludeFromIndexes());
  }

  /**
   * Tests that BigQueryConverters.columnToValue() returns a non-indexed String {@link Value} when
   * the BigQuery column is a STRING longer than 1500 bytes.
   */
  @Test
  public void testColumnToValueStringLong() {
    TableFieldSchema column = new TableFieldSchema().setName(longStringField).setType("STRING");
    Record record =
        generateSingleFieldAvroRecord(
            longStringField, "string", longStringFieldDesc, longStringFieldValue);
    Value value = BigQueryConverters.columnToValue(column, record.get(longStringField));
    assertEquals(longStringFieldValue, value.getStringValue());
    assertTrue(value.getExcludeFromIndexes());
  }

  /**
   * Tests that BigQueryConverters.columnToValue() returns an Integer {@link Value} when the
   * BigQuery column is an INTEGER.
   */
  @Test
  public void testColumnToValueInteger() {
    TableFieldSchema column = new TableFieldSchema().setName(integerField).setType("INTEGER");
    Record record =
        generateSingleFieldAvroRecord(integerField, "int", integerFieldDesc, integerFieldValue);
    Value value = BigQueryConverters.columnToValue(column, record.get(integerField));
    assertEquals(integerFieldValue, value.getIntegerValue());
  }

  /**
   * Tests that BigQueryConverters.columnToValue() returns an Integer {@link Value} when the
   * BigQuery column is an INT64.
   */
  @Test
  public void testColumnToValueInt64() {
    TableFieldSchema column = new TableFieldSchema().setName(int64Field).setType("INT64");
    Record record =
        generateSingleFieldAvroRecord(int64Field, "int", int64FieldDesc, int64FieldValue);
    Value value = BigQueryConverters.columnToValue(column, record.get(int64Field));
    assertEquals(int64FieldValue, value.getIntegerValue());
  }

  /**
   * Tests that BigQueryConverters.columnToValue() returns a Double {@link Value} when the BigQuery
   * column is a FLOAT.
   */
  @Test
  public void testColumnToValueFloat() {
    TableFieldSchema column = new TableFieldSchema().setName(floatField).setType("FLOAT");
    Record record =
        generateSingleFieldAvroRecord(floatField, "float", floatFieldDesc, floatFieldValue);
    Value value = BigQueryConverters.columnToValue(column, record.get(floatField));
    assertEquals(floatFieldValue, value.getDoubleValue(), 0.001);
  }

  /**
   * Tests that BigQueryConverters.columnToValue() returns an Double {@link Value} when the BigQuery
   * column is a FLOAT64.
   */
  @Test
  public void testColumnToValueFloat64() {
    TableFieldSchema column = new TableFieldSchema().setName(float64Field).setType("FLOAT64");
    Record record =
        generateSingleFieldAvroRecord(float64Field, "float", float64FieldDesc, float64FieldValue);
    Value value = BigQueryConverters.columnToValue(column, record.get(float64Field));
    assertEquals(float64FieldValue, value.getDoubleValue(), 0.001);
  }

  /**
   * Tests that BigQueryConverters.columnToValue() returns a Boolean {@link Value} when the BigQuery
   * column is a BOOLEAN.
   */
  @Test
  public void testColumnToValueBoolean() {
    TableFieldSchema column = new TableFieldSchema().setName(booleanField).setType("BOOLEAN");
    Record record =
        generateSingleFieldAvroRecord(booleanField, "boolean", booleanFieldDesc, booleanFieldValue);
    Value value = BigQueryConverters.columnToValue(column, record.get(booleanField));
    assertEquals(booleanFieldValue, value.getBooleanValue());
  }

  /**
   * Tests that BigQueryConverters.columnToValue() returns an Boolean {@link Value} when the
   * BigQuery column is a BOOL.
   */
  @Test
  public void testColumnToValueBool() {
    TableFieldSchema column = new TableFieldSchema().setName(boolField).setType("BOOL");
    Record record =
        generateSingleFieldAvroRecord(boolField, "boolean", boolFieldDesc, boolFieldValue);
    Value value = BigQueryConverters.columnToValue(column, record.get(boolField));
    assertEquals(boolFieldValue, value.getBooleanValue());
  }

  /**
   * Tests that BigQueryConverters.columnToValue() returns a Timestamp {@link Value} when the
   * BigQuery column is a valid TIMESTAMP.
   */
  @Test
  public void testColumnToValueTimestampValid() {
    TableFieldSchema column =
        new TableFieldSchema().setName(validTimestampField).setType("TIMESTAMP");
    Record record =
        generateSingleFieldAvroRecord(
            validTimestampField, "long", validTimestampFieldDesc, validTimestampFieldValueMicros);
    Value value = BigQueryConverters.columnToValue(column, record.get(validTimestampField));
    assertEquals(Timestamps.fromMillis(validTimestampFieldValueMillis), value.getTimestampValue());
  }

  /**
   * Tests that BigQueryConverters.columnToValue() throws IllegalArgumentException when the BigQuery
   * column is an invalid TIMESTAMP.
   */
  @Test
  public void testColumnToValueTimestampInvalid() {
    TableFieldSchema column =
        new TableFieldSchema().setName(invalidTimestampField).setType("TIMESTAMP");
    Record record =
        generateSingleFieldAvroRecord(
            invalidTimestampField,
            "long",
            invalidTimestampFieldDesc,
            invalidTimestampFieldValueNanos);
    boolean isThrown = false;
    try {
      Value value = BigQueryConverters.columnToValue(column, record.get(invalidTimestampField));
    } catch (IllegalArgumentException e) {
      isThrown = true;
    }
    assertTrue(isThrown);
  }

  /**
   * Tests that BigQueryConverters.columnToValue() returns a String {@link Value} when the BigQuery
   * column is a DATE.
   */
  @Test
  public void testColumnToValueDate() {
    TableFieldSchema column = new TableFieldSchema().setName(dateField).setType("DATE");
    Record record =
        generateSingleFieldAvroRecord(dateField, "string", dateFieldDesc, dateFieldValue);
    Value value = BigQueryConverters.columnToValue(column, record.get(dateField));
    assertEquals(dateFieldValue, value.getStringValue());
  }

  /**
   * Tests that BigQueryConverters.columnToValue() returns a String {@link Value} when the BigQuery
   * column is a TIME.
   */
  @Test
  public void testColumnToValueTime() {
    TableFieldSchema column = new TableFieldSchema().setName(timeField).setType("TIME");
    Record record =
        generateSingleFieldAvroRecord(timeField, "string", timeFieldDesc, timeFieldValue);
    Value value = BigQueryConverters.columnToValue(column, record.get(timeField));
    assertEquals(timeFieldValue, value.getStringValue());
  }

  /**
   * Tests that BigQueryConverters.columnToValue() returns a String {@link Value} when the BigQuery
   * column is a DATETIME.
   */
  @Test
  public void testColumnToValueDatetime() {
    TableFieldSchema column = new TableFieldSchema().setName(dateTimeField).setType("DATETIME");
    Record record =
        generateSingleFieldAvroRecord(
            dateTimeField, "string", dateTimeFieldDesc, dateTimeFieldValue);
    Value value = BigQueryConverters.columnToValue(column, record.get(dateTimeField));
    assertEquals(dateTimeFieldValue, value.getStringValue());
  }

  /**
   * Tests that BigQueryConverters.columnToValue() returns a String {@link Value} when the BigQuery
   * column is a RECORD.
   */
  @Test
  public void testColumnToValueRecord() {
    TableFieldSchema column = generateNestedTableFieldSchema();
    Record record = generateNestedAvroRecord();
    boolean isThrown = false;
    String message = null;
    try {
      Value value = BigQueryConverters.columnToValue(column, record.get("address"));
    } catch (IllegalArgumentException e) {
      isThrown = true;
      message = e.getMessage();
    }
    assertTrue(isThrown);
    assertTrue(message != null);
    assertEquals(message, "Column [address] of type [RECORD] not supported.");
  }

  /**
   * Tests that {@link BigQueryConverters.AvroToEntity} creates an Entity without a key when the
   * unique name column is missing.
   */
  @Test
  public void testAvroToEntityNoIdColumn() throws Exception {
    // Create test data
    List<TableFieldSchema> fields = new ArrayList<>();
    fields.add(new TableFieldSchema().setName(shortStringField).setType("STRING"));
    TableSchema bqSchema = new TableSchema().setFields(fields);
    Record record =
        generateSingleFieldAvroRecord(
            shortStringField, "string", shortStringFieldDesc, shortStringFieldValue);
    SchemaAndRecord inputBqData = new SchemaAndRecord(record, bqSchema);
    // Run the test
    Entity outputEntity = converter.apply(inputBqData);
    assertTrue(!outputEntity.hasKey());
  }

  /**
   * Tests that {@link BigQueryConverters.AvroToEntity} creates an Entity without a key when the
   * unique name column is null.
   */
  @Test
  public void testAvroToEntityNullIdColumn() throws Exception {
    // Create test data
    List<TableFieldSchema> fields = new ArrayList<>();
    fields.add(new TableFieldSchema().setName(idField).setType("STRING"));
    fields.add(new TableFieldSchema().setName(shortStringField).setType("STRING"));
    TableSchema bqSchema = new TableSchema().setFields(fields);
    Schema avroSchema =
        new Schema.Parser()
            .parse(
                String.format(
                    avroSchemaTemplate,
                    new StringBuilder()
                        .append(String.format(avroFieldTemplate, idField, "null", idFieldDesc))
                        .append(",")
                        .append(generateShortStringField())
                        .toString()));
    GenericRecordBuilder builder = new GenericRecordBuilder(avroSchema);
    builder.set(idField, null);
    builder.set(shortStringField, shortStringFieldValue);
    Record record = builder.build();
    SchemaAndRecord inputBqData = new SchemaAndRecord(record, bqSchema);
    // Run the test
    Entity outputEntity = converter.apply(inputBqData);
    assertTrue(!outputEntity.hasKey());
  }

  /**
   * Tests that {@link BigQueryConverters.AvroToEntity} creates an Entity without a key when the
   * unique name column exceeds the maximum size allowed of 1500 bytes.
   */
  @Test
  public void testAvroToEntityTooLongIdColumn() throws Exception {
    // Create test data
    List<TableFieldSchema> fields = new ArrayList<>();
    fields.add(new TableFieldSchema().setName(idField).setType("STRING"));
    fields.add(new TableFieldSchema().setName(shortStringField).setType("STRING"));
    TableSchema bqSchema = new TableSchema().setFields(fields);
    Schema avroSchema =
        new Schema.Parser()
            .parse(
                String.format(
                    avroSchemaTemplate,
                    new StringBuilder()
                        .append(String.format(avroFieldTemplate, idField, "string", idFieldDesc))
                        .append(",")
                        .append(generateShortStringField())
                        .toString()));
    GenericRecordBuilder builder = new GenericRecordBuilder(avroSchema);
    builder.set(idField, longStringFieldValue);
    builder.set(shortStringField, shortStringFieldValue);
    Record record = builder.build();
    SchemaAndRecord inputBqData = new SchemaAndRecord(record, bqSchema);
    // Run the test
    Entity outputEntity = converter.apply(inputBqData);
    assertTrue(!outputEntity.hasKey());
  }

  /**
   * Tests that {@link BigQueryConverters.AvroToEntity} creates an Entity with a valid key when the
   * unique name column is integer.
   */
  @Test
  public void testAvroToEntityIntegerIdColumn() throws Exception {
    // Create test data
    List<TableFieldSchema> fields = new ArrayList<>();
    fields.add(new TableFieldSchema().setName(idField).setType("INTEGER"));
    fields.add(new TableFieldSchema().setName(shortStringField).setType("STRING"));
    TableSchema bqSchema = new TableSchema().setFields(fields);
    Schema avroSchema =
        new Schema.Parser()
            .parse(
                String.format(
                    avroSchemaTemplate,
                    new StringBuilder()
                        .append(String.format(avroFieldTemplate, idField, "int", idFieldDesc))
                        .append(",")
                        .append(generateShortStringField())
                        .toString()));
    GenericRecordBuilder builder = new GenericRecordBuilder(avroSchema);
    builder.set(idField, idFieldValueInt);
    builder.set(shortStringField, shortStringFieldValue);
    Record record = builder.build();
    SchemaAndRecord inputBqData = new SchemaAndRecord(record, bqSchema);
    // Run the test
    Entity outputEntity = converter.apply(inputBqData);
    assertTrue(outputEntity.hasKey());
    assertEquals(idFieldValueStr, outputEntity.getKey().getPath(0).getName());
    validateMetadata(outputEntity);
  }

  /**
   * Tests that {@link BigQueryConverters.AvroToEntity} creates an Entity with a valid key when the
   * unique name column is string.
   */
  @Test
  public void testAvroToEntityStringIdColumn() throws Exception {
    // Create test data
    List<TableFieldSchema> fields = new ArrayList<>();
    fields.add(new TableFieldSchema().setName(idField).setType("STRING"));
    fields.add(new TableFieldSchema().setName(shortStringField).setType("STRING"));
    TableSchema bqSchema = new TableSchema().setFields(fields);
    Schema avroSchema =
        new Schema.Parser()
            .parse(
                String.format(
                    avroSchemaTemplate,
                    new StringBuilder()
                        .append(String.format(avroFieldTemplate, idField, "string", idFieldDesc))
                        .append(",")
                        .append(generateShortStringField())
                        .toString()));
    GenericRecordBuilder builder = new GenericRecordBuilder(avroSchema);
    builder.set(idField, idFieldValueStr);
    builder.set(shortStringField, shortStringFieldValue);
    Record record = builder.build();
    SchemaAndRecord inputBqData = new SchemaAndRecord(record, bqSchema);
    // Run the test
    Entity outputEntity = converter.apply(inputBqData);
    assertTrue(outputEntity.hasKey());
    assertEquals(idFieldValueStr, outputEntity.getKey().getPath(0).getName());
    validateMetadata(outputEntity);
  }

  /**
   * Tests that {@link BigQueryConverters.AvroToEntity} creates an Entity with a default namespace
   * when the namespace is not specified.
   */
  @Test
  public void testAvroToEntityDefaultNamespace() throws Exception {
    // Create test data
    List<TableFieldSchema> fields = new ArrayList<>();
    fields.add(new TableFieldSchema().setName(idField).setType("STRING"));
    fields.add(new TableFieldSchema().setName(shortStringField).setType("STRING"));
    TableSchema bqSchema = new TableSchema().setFields(fields);
    Schema avroSchema =
        new Schema.Parser()
            .parse(
                String.format(
                    avroSchemaTemplate,
                    new StringBuilder()
                        .append(String.format(avroFieldTemplate, idField, "int", idFieldDesc))
                        .append(",")
                        .append(generateShortStringField())
                        .toString()));
    GenericRecordBuilder builder = new GenericRecordBuilder(avroSchema);
    builder.set(idField, 1);
    builder.set(shortStringField, shortStringFieldValue);
    Record record = builder.build();
    SchemaAndRecord inputBqData = new SchemaAndRecord(record, bqSchema);
    // Run the test
    AvroToEntity noNamespaceConverter =
        AvroToEntity.newBuilder()
            .setEntityKind(entityKind)
            .setUniqueNameColumn(uniqueNameColumn)
            .build();
    Entity outputEntity = noNamespaceConverter.apply(inputBqData);
    // Assess results
    assertTrue(outputEntity.hasKey());
    assertEquals("", outputEntity.getKey().getPartitionId().getNamespaceId());
  }

  /**
   * Tests that {@link BigQueryConverters.AvroToEntity} creates an Entity without a valid key when a
   * Timestamp field is invalid.
   */
  @Test
  public void testAvroToEntityInvalidTimestampField() throws Exception {
    // Create test data
    List<TableFieldSchema> fields = new ArrayList<>();
    fields.add(new TableFieldSchema().setName(idField).setType("STRING"));
    fields.add(new TableFieldSchema().setName(invalidTimestampField).setType("TIMESTAMP"));
    TableSchema bqSchema = new TableSchema().setFields(fields);
    Schema avroSchema =
        new Schema.Parser()
            .parse(
                String.format(
                    avroSchemaTemplate,
                    new StringBuilder()
                        .append(String.format(avroFieldTemplate, idField, "string", idFieldDesc))
                        .append(",")
                        .append(
                            String.format(
                                avroFieldTemplate,
                                invalidTimestampField,
                                "long",
                                invalidTimestampFieldDesc))
                        .toString()));
    GenericRecordBuilder builder = new GenericRecordBuilder(avroSchema);
    builder.set(idField, idFieldValueStr);
    builder.set(invalidTimestampField, invalidTimestampFieldValueNanos);
    Record record = builder.build();
    SchemaAndRecord inputBqData = new SchemaAndRecord(record, bqSchema);
    // Run the test
    Entity outputEntity = converter.apply(inputBqData);
    // Assess results
    assertTrue(!outputEntity.hasKey());
    assertTrue(
        outputEntity
            .getPropertiesMap()
            .get("cause")
            .getStringValue()
            .startsWith("Timestamp is not valid"));
    assertEquals(record.toString(), outputEntity.getPropertiesMap().get("row").getStringValue());
  }

  /**
   * Tests that {@link BigQueryConverters.AvroToEntity} creates an Entity without a valid key when a
   * field is of type Record.
   */
  @Test
  public void testAvroToEntityRecordField() throws Exception {
    // Create test data
    TableFieldSchema column = generateNestedTableFieldSchema();
    List<TableFieldSchema> fields = new ArrayList<>();
    fields.add(column);
    TableSchema bqSchema = new TableSchema().setFields(fields);
    Record record = generateNestedAvroRecord();
    SchemaAndRecord inputBqData = new SchemaAndRecord(record, bqSchema);
    // Run the test
    Entity outputEntity = converter.apply(inputBqData);
    // Assess results
    String expectedCauseMessage = String.format("Column [address] of type [RECORD] not supported.");
    assertTrue(!outputEntity.hasKey());
    assertEquals(
        expectedCauseMessage, outputEntity.getPropertiesMap().get("cause").getStringValue());
    assertEquals(record.toString(), outputEntity.getPropertiesMap().get("row").getStringValue());
  }

  /** Tests that {@link BigQueryConverters.AvroToEntity} creates an Entity with all field types. */
  @Test
  public void testAvroToEntityAllFieldTypes() throws Exception {
    // Create test data
    List<TableFieldSchema> fields = new ArrayList<>();
    fields.add(new TableFieldSchema().setName(idField).setType("STRING"));
    fields.add(new TableFieldSchema().setName(shortStringField).setType("STRING"));
    fields.add(new TableFieldSchema().setName(longStringField).setType("STRING"));
    fields.add(new TableFieldSchema().setName(integerField).setType("INTEGER"));
    fields.add(new TableFieldSchema().setName(int64Field).setType("INT64"));
    fields.add(new TableFieldSchema().setName(floatField).setType("FLOAT"));
    fields.add(new TableFieldSchema().setName(float64Field).setType("FLOAT64"));
    fields.add(new TableFieldSchema().setName(booleanField).setType("BOOLEAN"));
    fields.add(new TableFieldSchema().setName(boolField).setType("BOOL"));
    fields.add(new TableFieldSchema().setName(validTimestampField).setType("TIMESTAMP"));
    fields.add(new TableFieldSchema().setName(dateField).setType("DATE"));
    fields.add(new TableFieldSchema().setName(timeField).setType("TIME"));
    fields.add(new TableFieldSchema().setName(dateTimeField).setType("DATETIME"));
    fields.add(new TableFieldSchema().setName(nullField).setType("STRING"));
    TableSchema bqSchema = new TableSchema().setFields(fields);
    Schema avroSchema =
        new Schema.Parser()
            .parse(
                String.format(
                    avroSchemaTemplate,
                    new StringBuilder()
                        .append(String.format(avroFieldTemplate, idField, "string", idFieldDesc))
                        .append(",")
                        .append(generateShortStringField())
                        .append(",")
                        .append(generateLongStringField())
                        .append(",")
                        .append(
                            String.format(avroFieldTemplate, integerField, "int", integerFieldDesc))
                        .append(",")
                        .append(String.format(avroFieldTemplate, int64Field, "int", int64FieldDesc))
                        .append(",")
                        .append(
                            String.format(avroFieldTemplate, floatField, "float", floatFieldDesc))
                        .append(",")
                        .append(
                            String.format(
                                avroFieldTemplate, float64Field, "float", float64FieldDesc))
                        .append(",")
                        .append(
                            String.format(
                                avroFieldTemplate, booleanField, "boolean", booleanFieldDesc))
                        .append(",")
                        .append(
                            String.format(avroFieldTemplate, boolField, "boolean", boolFieldDesc))
                        .append(",")
                        .append(
                            String.format(
                                avroFieldTemplate,
                                validTimestampField,
                                "long",
                                validTimestampFieldDesc))
                        .append(",")
                        .append(
                            String.format(avroFieldTemplate, dateField, "string", dateFieldDesc))
                        .append(",")
                        .append(
                            String.format(avroFieldTemplate, timeField, "string", timeFieldDesc))
                        .append(",")
                        .append(
                            String.format(
                                avroFieldTemplate, dateTimeField, "string", dateTimeFieldDesc))
                        .append(",")
                        .append(String.format(avroFieldTemplate, nullField, "null", nullFieldDesc))
                        .toString()));
    GenericRecordBuilder builder = new GenericRecordBuilder(avroSchema);
    builder.set(idField, idFieldValueStr);
    builder.set(shortStringField, shortStringFieldValue);
    builder.set(longStringField, longStringFieldValue);
    builder.set(integerField, integerFieldValue);
    builder.set(int64Field, int64FieldValue);
    builder.set(floatField, floatFieldValue);
    builder.set(float64Field, float64FieldValue);
    builder.set(booleanField, booleanFieldValue);
    builder.set(boolField, boolFieldValue);
    builder.set(validTimestampField, validTimestampFieldValueMicros);
    builder.set(dateField, dateFieldValue);
    builder.set(timeField, timeFieldValue);
    builder.set(dateTimeField, dateTimeFieldValue);
    builder.set(nullField, null);
    Record record = builder.build();
    SchemaAndRecord inputBqData = new SchemaAndRecord(record, bqSchema);

    // Run the test
    Entity outputEntity = converter.apply(inputBqData);
    Map<String, Value> properties = outputEntity.getPropertiesMap();

    // Assess results
    assertTrue(outputEntity.hasKey());
    assertEquals(idFieldValueStr, outputEntity.getKey().getPath(0).getName());
    validateMetadata(outputEntity);

    assertTrue(outputEntity.containsProperties(shortStringField));
    assertEquals(shortStringFieldValue, properties.get(shortStringField).getStringValue());
    assertFalse(properties.get(shortStringField).getExcludeFromIndexes());

    assertTrue(outputEntity.containsProperties(longStringField));
    assertEquals(longStringFieldValue, properties.get(longStringField).getStringValue());
    assertTrue(properties.get(longStringField).getExcludeFromIndexes());

    assertTrue(outputEntity.containsProperties(integerField));
    assertEquals(integerFieldValue, properties.get(integerField).getIntegerValue());

    assertTrue(outputEntity.containsProperties(int64Field));
    assertEquals(int64FieldValue, properties.get(int64Field).getIntegerValue());

    assertTrue(outputEntity.containsProperties(floatField));
    assertEquals(floatFieldValue, properties.get(floatField).getDoubleValue(), 0.001);

    assertTrue(outputEntity.containsProperties(float64Field));
    assertEquals(float64FieldValue, properties.get(float64Field).getDoubleValue(), 0.001);

    assertTrue(outputEntity.containsProperties(booleanField));
    assertEquals(booleanFieldValue, properties.get(booleanField).getBooleanValue());

    assertTrue(outputEntity.containsProperties(boolField));
    assertEquals(boolFieldValue, properties.get(boolField).getBooleanValue());

    assertTrue(outputEntity.containsProperties(validTimestampField));
    assertEquals(
        Timestamps.fromMillis(validTimestampFieldValueMillis),
        properties.get(validTimestampField).getTimestampValue());

    assertTrue(outputEntity.containsProperties(dateField));
    assertEquals(dateFieldValue, properties.get(dateField).getStringValue());

    assertTrue(outputEntity.containsProperties(timeField));
    assertEquals(timeFieldValue, properties.get(timeField).getStringValue());

    assertTrue(outputEntity.containsProperties(dateTimeField));
    assertEquals(dateTimeFieldValue, properties.get(dateTimeField).getStringValue());

    assertTrue(outputEntity.containsProperties(nullField));
    assertEquals(NullValue.NULL_VALUE, properties.get(nullField).getNullValue());
  }

  /** Validate that {@link Entity} has the right kind and namespace. */
  private void validateMetadata(Entity outputEntity) {
    assertEquals(entityKind.get(), outputEntity.getKey().getPath(0).getKind());
    assertEquals(namespace.get(), outputEntity.getKey().getPartitionId().getNamespaceId());
  }

  /** Generates an Avro record with a single field. */
  private Record generateSingleFieldAvroRecord(
      String name, String type, String description, Object value) {
    Schema avroSchema =
        new Schema.Parser()
            .parse(
                String.format(
                    avroSchemaTemplate,
                    new StringBuilder()
                        .append(String.format(avroFieldTemplate, name, type, description))
                        .toString()));
    GenericRecordBuilder builder = new GenericRecordBuilder(avroSchema);
    builder.set(name, value);
    return builder.build();
  }

  /** Generates a short string Avro field. */
  private String generateShortStringField() {
    return String.format(avroFieldTemplate, shortStringField, "string", shortStringFieldDesc);
  }

  /** Generates a long string Avro field. */
  private String generateLongStringField() {
    return String.format(avroFieldTemplate, longStringField, "string", longStringFieldDesc);
  }

  /** Generate a BigQuery TableSchema with nested fields. */
  private TableFieldSchema generateNestedTableFieldSchema() {
    return new TableFieldSchema()
        .setName("address")
        .setType("RECORD")
        .setFields(
            Arrays.asList(
                new TableFieldSchema().setName("street_number").setType("INTEGER"),
                new TableFieldSchema().setName("street_name").setType("STRING")));
  }

  /** Generates an Avro record with a record field type. */
  private Record generateNestedAvroRecord() {
    String avroRecordFieldSchema =
        new StringBuilder()
            .append("{")
            .append("  \"name\" : \"address\",")
            .append("  \"type\" :")
            .append("  {")
            .append("    \"type\" : \"record\",")
            .append("    \"name\" : \"address\",")
            .append("    \"namespace\"  : \"nothing\",")
            .append("    \"fields\" : ")
            .append("    [")
            .append("      {\"name\" : \"street_number\", \"type\" : \"int\"},")
            .append("      {\"name\" : \"street_name\", \"type\" : \"string\"}")
            .append("    ]")
            .append("  }")
            .append("}")
            .toString();
    Schema avroSchema =
        new Schema.Parser().parse(String.format(avroSchemaTemplate, avroRecordFieldSchema));
    GenericRecordBuilder addressBuilder =
        new GenericRecordBuilder(avroSchema.getField("address").schema());
    addressBuilder.set("street_number", 12);
    addressBuilder.set("street_name", "Magnolia street");
    GenericRecordBuilder builder = new GenericRecordBuilder(avroSchema);
    builder.set("address", addressBuilder);
    return builder.build();
  }
}
