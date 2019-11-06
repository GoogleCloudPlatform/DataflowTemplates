/*
 * Copyright (C) 2018 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.teleport.v2.transforms;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.teleport.v2.coders.FailsafeElementCoder;
import com.google.cloud.teleport.v2.transforms.BigQueryConverters.FailsafeJsonToTableRow;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessageWithAttributesCoder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
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

  static final TableRow ROW =
      new TableRow().set("id", "007").set("state", "CA").set("price", 26.23);
  /** The tag for the main output of the json transformation. */

  static final TupleTag<FailsafeElement<TableRow, String>> TRANSFORM_OUT =
          new TupleTag<FailsafeElement<TableRow, String>>() {};
  /** The tag for the dead-letter output of the json to table row transform. */
  static final TupleTag<FailsafeElement<TableRow, String>> TRANSFORM_DEADLETTER_OUT =
          new TupleTag<FailsafeElement<TableRow, String>>() {};
  /** The tag for the main output of the json transformation. */
  static final TupleTag<FailsafeElement<TableRow, String>> UDF_OUT =
          new TupleTag<FailsafeElement<TableRow, String>>() {};
  /** The tag for the dead-letter output of the json to table row transform. */
  static final TupleTag<FailsafeElement<TableRow, String>> UDF_TRANSFORM_DEADLETTER_OUT =
          new TupleTag<FailsafeElement<TableRow, String>>() {};
  /** String/String Coder for FailsafeElement. */
  static final FailsafeElementCoder<String, String> FAILSAFE_ELEMENT_CODER =
          FailsafeElementCoder.of(
                  NullableCoder.of(StringUtf8Coder.of()), NullableCoder.of(StringUtf8Coder.of()));
  /** TableRow/String Coder for FailsafeElement. */
  static final FailsafeElementCoder<TableRow, String> FAILSAFE_TABLE_ROW_ELEMENT_CODER =
          FailsafeElementCoder.of(TableRowJsonCoder.of(), NullableCoder.of(StringUtf8Coder.of()));
  // Define the TupleTag's here otherwise the anonymous class will force the test method to
  // be serialized.
  private static final TupleTag<TableRow> TABLE_ROW_TAG = new TupleTag<TableRow>() {};
  private static final TupleTag<FailsafeElement<PubsubMessage, String>> FAILSAFE_ELM_TAG =
      new TupleTag<FailsafeElement<PubsubMessage, String>>() {};
  private static final String jsonifiedTableRow =
          "{\"id\":\"007\",\"state\":\"CA\",\"price\":26.23}";
  private static final String udfOutputRow =
          "{\"id\":\"007\",\"state\":\"CA\",\"price\":26.23,\"someProp\":\"someValue\"}";
  @Rule public final transient TestPipeline pipeline = TestPipeline.create();
  @Rule public ExpectedException expectedException = ExpectedException.none();
  private ValueProvider<String> entityKind = StaticValueProvider.of("TestEntity");
  private ValueProvider<String> uniqueNameColumn = StaticValueProvider.of("id");
  private ValueProvider<String> namespace = StaticValueProvider.of("bq-to-ds-test");
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

  /**
   * Tests {@link com.google.cloud.teleport.v2.transforms.BigQueryConverters.ReadBigQuery} throws
   * exception when neither a query or input table is provided.
   */
  @Test(expected = IllegalArgumentException.class)
  public void testReadBigQueryInvalidInput() {

    BigQueryConverters.BigQueryReadOptions options =
        PipelineOptionsFactory.create().as(BigQueryConverters.BigQueryReadOptions.class);

    options.setInputTableSpec(null);
    options.setQuery(null);

    pipeline.apply(BigQueryConverters.ReadBigQuery.newBuilder().setOptions(options).build());

    pipeline.run();
  }

  /** Tests that {@link BigQueryConverters.TableRowToFailsafeJsonDocument} transform returns the correct element. */
  @Test
  public void testTableRowToJsonDocument() {
    CoderRegistry coderRegistry = pipeline.getCoderRegistry();

    coderRegistry.registerCoderForType(
            FAILSAFE_ELEMENT_CODER.getEncodedTypeDescriptor(),
            FAILSAFE_ELEMENT_CODER);

    coderRegistry.registerCoderForType(
            FAILSAFE_TABLE_ROW_ELEMENT_CODER.getEncodedTypeDescriptor(),
            FAILSAFE_TABLE_ROW_ELEMENT_CODER);

    BigQueryConverters.BigQueryReadOptions options =
            PipelineOptionsFactory.create().as(BigQueryConverters.BigQueryReadOptions.class);

    options.setInputTableSpec(null);
    options.setQuery(null);


    PCollectionTuple testTuple =
    pipeline
        .apply("Create Input", Create.<TableRow>of(ROW).withCoder(TableRowJsonCoder.of()))
        .apply(
            "TestRowToDocument",
            BigQueryConverters.TableRowToFailsafeJsonDocument.newBuilder()
                .setTransformDeadletterOutTag(TRANSFORM_DEADLETTER_OUT)
                .setTransformOutTag(TRANSFORM_OUT)
                .setUdfDeadletterOutTag(UDF_TRANSFORM_DEADLETTER_OUT)
                .setUdfOutTag(UDF_OUT)
                .setOptions(options.as(JavascriptTextTransformer.JavascriptTextTransformerOptions.class))
                .build());

    // Assert
    PAssert.that(testTuple.get(TRANSFORM_OUT)).satisfies(
            collection -> {
              FailsafeElement<TableRow, String> element = collection.iterator().next();
              assertThat(element.getOriginalPayload(), is(equalTo(ROW)));
              assertThat(element.getPayload(), is(equalTo(jsonifiedTableRow)));
              return null;
            }
    );

    // Execute pipeline
    pipeline.run();
  }
}
