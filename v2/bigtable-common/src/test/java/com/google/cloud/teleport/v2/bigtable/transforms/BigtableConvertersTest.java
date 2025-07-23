/*
 * Copyright (C) 2023 Google LLC
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
package com.google.cloud.teleport.v2.bigtable.transforms;

import static com.google.common.truth.Truth.assertThat;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.teleport.v2.bigtable.transforms.BigtableConverters.AvroToMutation;
import java.util.Arrays;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.beam.sdk.io.gcp.bigquery.SchemaAndRecord;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link BigtableConverters}. */
@RunWith(JUnit4.class)
public class BigtableConvertersTest {

  private static final String AVRO_SCHEMA_TEMPLATE =
      "{"
          + " \"type\" : \"record\","
          + " \"name\" : \"BigQueryTestData\","
          + " \"namespace\" : \"\","
          + " \"fields\" :"
          + "  [%s],"
          + " \"doc:\" : \"A basic Avro schema for unit testing purposes\""
          + "}";

  private final String avroFieldTemplate =
      "{" + " \"name\" : \"%s\"," + " \"type\" : \"%s\"," + " \"doc\"  : \"%s\"" + "}";

  private final String shortStringField = "author";
  private final String shortStringFieldDesc = "Author name";
  private final String idFieldDesc = "Unique identifier";
  private final String idFieldValueStr = "87234";
  private final String shortStringFieldValue = "Morgan le Fay";

  private final Mutation dummyMutation =
      new Put("r".getBytes()).addColumn("f".getBytes(), "q".getBytes(), "v".getBytes());

  /** Generates a short string Avro field. */
  private String generateShortStringField() {
    return String.format(avroFieldTemplate, shortStringField, "string", shortStringFieldDesc);
  }

  /** Tests that {@link BigtableConverters.AvroToMutation} creates a Mutation. */
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
                        .append(generateShortStringField())));
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
    BigtableConverters.AvroToMutation avroToMutation =
        BigtableConverters.AvroToMutation.newBuilder()
            .setColumnFamily(columnFamily)
            .setRowkey(rowkey)
            .build();

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
                        .append(nullableStringField)));
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
  public void testAvroToMutationNullColumnValueSkipped() {
    // additional field
    String shortStringField2 = shortStringField + "_2";
    String shortStringFieldDesc2 = shortStringFieldDesc + "_2";
    String shortStringFieldValue2 = shortStringFieldValue + "_2";

    // Arrange
    String rowkey = "rowkey";
    String columnFamily = "CF";
    BigtableConverters.AvroToMutation avroToMutation =
        BigtableConverters.AvroToMutation.newBuilder()
            .setColumnFamily(columnFamily)
            .setRowkey(rowkey)
            .setSkipNullValues(true)
            .build();

    TableSchema bqSchema =
        new TableSchema()
            .setFields(
                Arrays.asList(
                    new TableFieldSchema().setName(rowkey).setType("STRING"),
                    new TableFieldSchema().setName(shortStringField).setType("STRING"),
                    new TableFieldSchema().setName(shortStringField2).setType("STRING")));

    String nullableStringField =
        "{"
            + String.format(" \"name\" : \"%s\",", shortStringField)
            + " \"type\" : [\"null\", \"string\"],"
            + String.format(" \"doc\"  : \"%s\"", shortStringFieldDesc)
            + "}";
    String nullableStringField2 =
        "{"
            + String.format(" \"name\" : \"%s\",", shortStringField2)
            + " \"type\" : [\"null\", \"string\"],"
            + String.format(" \"doc\"  : \"%s\"", shortStringFieldDesc2)
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
                        .append(",")
                        .append(nullableStringField2)));
    GenericRecordBuilder builder = new GenericRecordBuilder(avroSchema);
    builder.set(rowkey, idFieldValueStr);
    builder.set(shortStringField, null);
    builder.set(shortStringField2, shortStringFieldValue2);
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
    assertThat(shortStringField2).isEqualTo(Bytes.toString(CellUtil.cloneQualifier(cells.get(0))));
    assertThat(shortStringFieldValue2).isEqualTo(Bytes.toString(CellUtil.cloneValue(cells.get(0))));
  }

  @Test
  public void testAvroToMutationColumnBasedTimestamp() {
    // additional field for user provided timestamp
    String cellTimestampField = "cell_timestamp";
    String cellTimestampFieldDesc = "Cell timestamp from bq column";
    Long cellTimestampFieldValue = System.currentTimeMillis();

    // Arrange
    String rowkey = "rowkey";
    String columnFamily = "CF";
    AvroToMutation avroToMutation =
        AvroToMutation.newBuilder()
            .setColumnFamily(columnFamily)
            .setRowkey(rowkey)
            .setTimestampColumn(cellTimestampField)
            .build();

    TableSchema bqSchema =
        new TableSchema()
            .setFields(
                Arrays.asList(
                    new TableFieldSchema().setName(rowkey).setType("STRING"),
                    new TableFieldSchema().setName(shortStringField).setType("STRING"),
                    new TableFieldSchema().setName(cellTimestampField).setType("INT64")));

    Schema avroSchema =
        new Schema.Parser()
            .parse(
                String.format(
                    AVRO_SCHEMA_TEMPLATE,
                    new StringBuilder()
                        .append(String.format(avroFieldTemplate, rowkey, "string", idFieldDesc))
                        .append(",")
                        .append(generateShortStringField())
                        .append(",")
                        .append(
                            String.format(
                                avroFieldTemplate,
                                cellTimestampField,
                                "long",
                                cellTimestampFieldDesc))));
    GenericRecordBuilder builder = new GenericRecordBuilder(avroSchema);
    builder.set(rowkey, idFieldValueStr);
    builder.set(shortStringField, shortStringFieldValue);
    builder.set(cellTimestampField, cellTimestampFieldValue);
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
    assertThat(cellTimestampFieldValue).isEqualTo(cells.get(0).getTimestamp());
  }

  @Test
  public void testAvroToMutationColumnTimestampErrors() {
    // testing for expected errors - column dne, double, string, null
    // additional field for user provided timestamp
    String cellTimestampField = "cell_timestamp";
    String cellTimestampFieldDesc = "Cell timestamp from bq column";
    Long cellTimestampFieldValue = null;

    String cellTimestampFieldDouble = "cell_timestamp_double";
    Double cellTimestampFieldValueDouble = 1.102d;

    // Arrange
    String rowkey = "rowkey";
    String columnFamily = "CF";

    TableSchema bqSchema =
        new TableSchema()
            .setFields(
                Arrays.asList(
                    new TableFieldSchema().setName(rowkey).setType("STRING"),
                    new TableFieldSchema().setName(shortStringField).setType("STRING"),
                    new TableFieldSchema().setName(cellTimestampField).setType("INT64"),
                    new TableFieldSchema().setName(cellTimestampFieldDouble).setType("DECIMAL")));

    String nullableCellTimestampField =
        "{"
            + String.format(" \"name\" : \"%s\",", cellTimestampField)
            + " \"type\" : [\"null\", \"long\"],"
            + String.format(" \"doc\"  : \"%s\"", cellTimestampFieldDesc)
            + "}";

    Schema avroSchema =
        new Schema.Parser()
            .parse(
                String.format(
                    AVRO_SCHEMA_TEMPLATE,
                    new StringBuilder()
                        .append(String.format(avroFieldTemplate, rowkey, "string", idFieldDesc))
                        .append(",")
                        .append(generateShortStringField())
                        .append(",")
                        .append(nullableCellTimestampField)
                        .append(",")
                        .append(
                            String.format(
                                avroFieldTemplate,
                                cellTimestampFieldDouble,
                                "double",
                                cellTimestampFieldDesc))));
    GenericRecordBuilder builder = new GenericRecordBuilder(avroSchema);
    builder.set(rowkey, idFieldValueStr);
    builder.set(shortStringField, shortStringFieldValue);
    builder.set(cellTimestampField, cellTimestampFieldValue);
    builder.set(cellTimestampFieldDouble, cellTimestampFieldValueDouble);
    Record record = builder.build();
    SchemaAndRecord inputBqData = new SchemaAndRecord(record, bqSchema);

    // Act

    // test missing column
    boolean testExceptionMissingColumn = false;
    AvroToMutation avroToMutation1 =
        AvroToMutation.newBuilder()
            .setColumnFamily(columnFamily)
            .setRowkey(rowkey)
            .setTimestampColumn("missing_field_for_tscolumn")
            .build();
    try {
      Mutation mutation = avroToMutation1.apply(inputBqData);
    } catch (IllegalArgumentException e) {
      testExceptionMissingColumn = true;
      // error: Avro error accessing timestamp column 'missing_field_for_tscolumn' for row with key:
      // '87234'. Details: Not a valid schema field: missing_field_for_tscolumn
    }

    // test cast exception / double
    boolean testExceptionDouble = false;
    AvroToMutation avroToMutation2 =
        AvroToMutation.newBuilder()
            .setColumnFamily(columnFamily)
            .setRowkey(rowkey)
            .setTimestampColumn(cellTimestampFieldDouble)
            .build();
    try {
      Mutation mutation = avroToMutation2.apply(inputBqData);
    } catch (IllegalArgumentException e) {
      testExceptionDouble = true;
      // error: Timestamp column 'cell_timestamp_double' for row with key '87234' is of unexpected
      // type Double. Expected Long.
    }

    // test cast exception / string
    boolean testExceptionString = false;
    AvroToMutation avroToMutation3 =
        AvroToMutation.newBuilder()
            .setColumnFamily(columnFamily)
            .setRowkey(rowkey)
            .setTimestampColumn(shortStringField)
            .build();
    try {
      Mutation mutation = avroToMutation3.apply(inputBqData);
    } catch (IllegalArgumentException e) {
      testExceptionString = true;
      // error: Timestamp column 'author' for row with key '87234' is of unexpected type String.
      // Expected Long.
    }

    // test null timestamp set for missing value
    AvroToMutation avroToMutation =
        AvroToMutation.newBuilder()
            .setColumnFamily(columnFamily)
            .setRowkey(rowkey)
            .setTimestampColumn(cellTimestampField)
            .build();
    Mutation mutation = avroToMutation.apply(inputBqData);
    List<Cell> cells = mutation.getFamilyCellMap().get(Bytes.toBytes(columnFamily));

    // Assert
    // Assert: Exceptions thrown
    assertThat(testExceptionMissingColumn).isTrue();
    assertThat(testExceptionDouble).isTrue();
    assertThat(testExceptionString).isTrue();

    // Assert: null is handled, no timestamp is set
    List<Cell> cellsUnsetTsComparison = dummyMutation.getFamilyCellMap().get(Bytes.toBytes("f"));
    assertThat(cellsUnsetTsComparison.get(0).getTimestamp()).isEqualTo(cells.get(0).getTimestamp());
  }
}
