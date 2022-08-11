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
package com.google.cloud.teleport.v2.transforms;

import static com.google.common.truth.Truth.assertThat;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Field.Mode;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.teleport.v2.coders.FailsafeElementCoder;
import com.google.cloud.teleport.v2.transforms.BigQueryConverters.AvroToMutation;
import com.google.cloud.teleport.v2.transforms.BigQueryConverters.BigQueryTableConfigManager;
import com.google.cloud.teleport.v2.transforms.BigQueryConverters.FailsafeJsonToTableRow;
import com.google.cloud.teleport.v2.transforms.BigQueryConverters.SchemaUtils;
import com.google.cloud.teleport.v2.transforms.BigQueryConverters.TableRowToGenericRecordFn;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils;
import org.apache.beam.sdk.io.gcp.bigquery.SchemaAndRecord;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessageWithAttributesCoder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.schemas.utils.AvroUtils;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Strings;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.util.Bytes;
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
  private static final String AVRO_SCHEMA_TEMPLATE =
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
              assertThat(new String(result.getOriginalPayload().getPayload())).isEqualTo(payload);
              assertThat(result.getOriginalPayload().getAttributeMap()).isEqualTo(attributes);
              assertThat(result.getPayload()).isEqualTo(payload);
              assertThat(result.getErrorMessage()).isNotNull();
              assertThat(result.getStacktrace()).isNotNull();
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
                    AVRO_SCHEMA_TEMPLATE,
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
  static Record generateNestedAvroRecord() {
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
        new Schema.Parser().parse(String.format(AVRO_SCHEMA_TEMPLATE, avroRecordFieldSchema));
    GenericRecordBuilder addressBuilder =
        new GenericRecordBuilder(avroSchema.getField("address").schema());
    addressBuilder.set("street_number", 12);
    addressBuilder.set("street_name", "Magnolia street");
    GenericRecordBuilder builder = new GenericRecordBuilder(avroSchema);
    builder.set("address", addressBuilder.build());
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

  /**
   * Tests that {@link BigQueryConverters.TableRowToFailsafeJsonDocument} transform returns the
   * correct element.
   */
  @Test
  public void testTableRowToJsonDocument() {
    CoderRegistry coderRegistry = pipeline.getCoderRegistry();

    coderRegistry.registerCoderForType(
        FAILSAFE_ELEMENT_CODER.getEncodedTypeDescriptor(), FAILSAFE_ELEMENT_CODER);

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
                    .setOptions(
                        options.as(
                            JavascriptTextTransformer.JavascriptTextTransformerOptions.class))
                    .build());

    // Assert
    PAssert.that(testTuple.get(TRANSFORM_OUT))
        .satisfies(
            collection -> {
              FailsafeElement<TableRow, String> element = collection.iterator().next();
              assertThat(element.getOriginalPayload()).isEqualTo(ROW);
              assertThat(element.getPayload()).isEqualTo(jsonifiedTableRow);
              return null;
            });

    // Execute pipeline
    pipeline.run();
  }

  /**
   * Tests that {@link BigQueryConverters.BigQueryTableConfigManager} returns expected table names
   * when supplying templated values.
   */
  @Test
  public void serializableFunctionConvertsTableRowToGenericRecordUsingSchema() {
    GenericRecord expectedRecord = generateNestedAvroRecord();
    Row testRow =
        AvroUtils.toBeamRowStrict(
            expectedRecord, AvroUtils.toBeamSchema(expectedRecord.getSchema()));
    TableRow inputRow = BigQueryUtils.toTableRow(testRow);
    TableRowToGenericRecordFn rowToGenericRecordFn =
        TableRowToGenericRecordFn.of(expectedRecord.getSchema());

    GenericRecord actualRecord = rowToGenericRecordFn.apply(inputRow);

    assertThat(actualRecord).isEqualTo(expectedRecord);
  }

  public void testBigQueryTableConfigManagerTemplates() {
    String projectIdVal = "my_project";
    String datasetTemplateVal = "my_dataset";
    String tableTemplateVal = "my_table";
    String outputTableSpec = "";

    BigQueryTableConfigManager mgr =
        new BigQueryTableConfigManager(
            projectIdVal, datasetTemplateVal,
            tableTemplateVal, outputTableSpec);

    String outputTableSpecResult = "my_project:my_dataset.my_table";
    assertThat(mgr.getOutputTableSpec()).isEqualTo(outputTableSpecResult);
  }

  /**
   * Tests that {@link BigQueryConverters.BigQueryTableConfigManager} returns expected table names
   * when supplying full table path.
   */
  @Test
  public void testBigQueryTableConfigManagerTableSpec() {
    String projectIdVal = null;
    String datasetTemplateVal = null;
    String tableTemplateVal = null;
    String outputTableSpec = "my_project:my_dataset.my_table";

    BigQueryTableConfigManager mgr =
        new BigQueryTableConfigManager(
            projectIdVal, datasetTemplateVal,
            tableTemplateVal, outputTableSpec);

    assertThat(mgr.getDatasetTemplate()).isEqualTo("my_dataset");
    assertThat(mgr.getTableTemplate()).isEqualTo("my_table");
  }

  /**
   * Tests that {@link BigQueryConverters.SchemaUtils} properly cleans and returns a BigQuery Schema
   * from a JSON string.
   */
  @Test
  public void testSchemaUtils() {
    String jsonSchemaStr = "[{\"type\":\"STRING\",\"name\":\"column\",\"mode\":\"NULLABLE\"}]";
    List<Field> fields = SchemaUtils.schemaFromString(jsonSchemaStr);

    assertThat(fields.get(0).getName()).isEqualTo("column");
    assertThat(fields.get(0).getMode()).isEqualTo(Mode.NULLABLE);
    assertThat(fields.get(0).getType()).isEqualTo(LegacySQLTypeName.STRING);
  }

  /** Tests that {@link BigQueryConverters.AvroToMutation} creates a Mutation. */
  @Test
  public void testAvroToMutation() {
    // Arrange
    String rowkey = "rowkey";
    String columnFamily = "CF";
    AvroToMutation avroToMutation =
        AvroToMutation.newBuilder().setColumnFamily(columnFamily).setRowkey(rowkey).build();

    TableSchema bqSchema =
        new TableSchema()
            .setFields(
                Arrays.asList(
                    new TableFieldSchema().setName(rowkey).setType("STRING"),
                    new TableFieldSchema().setName(shortStringField).setType("STRING")));

    Schema avroSchema =
        new Schema.Parser()
            .parse(
                String.format(
                    AVRO_SCHEMA_TEMPLATE,
                    new StringBuilder()
                        .append(String.format(avroFieldTemplate, rowkey, "string", idFieldDesc))
                        .append(",")
                        .append(generateShortStringField())
                        .toString()));
    GenericRecordBuilder builder = new GenericRecordBuilder(avroSchema);
    builder.set(rowkey, idFieldValueStr);
    builder.set(shortStringField, shortStringFieldValue);
    Record record = builder.build();
    SchemaAndRecord inputBqData = new SchemaAndRecord(record, bqSchema);

    // Act
    Mutation mutation = avroToMutation.apply(inputBqData);

    // Assert
    // Assert: Rowkey is set
    assertThat(Bytes.toString(mutation.getRow())).isEqualTo(idFieldValueStr);

    assertThat(mutation.getFamilyCellMap().size()).isEqualTo(1);

    // Assert: One cell was set with a value
    List<Cell> cells = mutation.getFamilyCellMap().get(Bytes.toBytes(columnFamily));
    assertThat(cells.size()).isEqualTo(1);
    assertThat(shortStringField).isEqualTo(Bytes.toString(CellUtil.cloneQualifier(cells.get(0))));
    assertThat(shortStringFieldValue).isEqualTo(Bytes.toString(CellUtil.cloneValue(cells.get(0))));
  }

  @Test
  public void testAvroToMutationNullColumnValue() {
    // Arrange
    String rowkey = "rowkey";
    String columnFamily = "CF";
    AvroToMutation avroToMutation =
        AvroToMutation.newBuilder().setColumnFamily(columnFamily).setRowkey(rowkey).build();

    TableSchema bqSchema =
        new TableSchema()
            .setFields(
                Arrays.asList(
                    new TableFieldSchema().setName(rowkey).setType("STRING"),
                    new TableFieldSchema().setName(shortStringField).setType("STRING")));

    String nullableStringField =
        "{"
            + String.format(" \"name\" : \"%s\",", shortStringField)
            + " \"type\" : [\"null\", \"string\"],"
            + String.format(" \"doc\"  : \"%s\"", shortStringFieldDesc)
            + "}";
    Schema avroSchema =
        new Schema.Parser()
            .parse(
                String.format(
                    AVRO_SCHEMA_TEMPLATE,
                    new StringBuilder()
                        .append(String.format(avroFieldTemplate, rowkey, "string", idFieldDesc))
                        .append(",")
                        .append(nullableStringField)
                        .toString()));
    GenericRecordBuilder builder = new GenericRecordBuilder(avroSchema);
    builder.set(rowkey, idFieldValueStr);
    builder.set(shortStringField, null);
    Record record = builder.build();
    SchemaAndRecord inputBqData = new SchemaAndRecord(record, bqSchema);

    // Act
    Mutation mutation = avroToMutation.apply(inputBqData);

    // Assert
    // Assert: Rowkey is set
    assertThat(Bytes.toString(mutation.getRow())).isEqualTo(idFieldValueStr);

    assertThat(mutation.getFamilyCellMap().size()).isEqualTo(1);

    // Assert: One cell was set with a value
    List<Cell> cells = mutation.getFamilyCellMap().get(Bytes.toBytes(columnFamily));
    assertThat(cells.size()).isEqualTo(1);
    assertThat(shortStringField).isEqualTo(Bytes.toString(CellUtil.cloneQualifier(cells.get(0))));
    assertThat(CellUtil.cloneValue(cells.get(0))).isEmpty();
  }

  @Test
  public void testSanitizeBigQueryChars() {
    String sourceName = "my$table.name";

    String name = BigQueryConverters.sanitizeBigQueryChars(sourceName, "_");

    assertThat(name).isEqualTo("my_table_name");
  }

  @Test
  public void testSanitizeBigQueryDatasetChars() {
    String sourceName = "my$data-set.name";

    String name = BigQueryConverters.sanitizeBigQueryDatasetChars(sourceName, "_");

    assertThat(name).isEqualTo("my_data_set_name");
  }
}
