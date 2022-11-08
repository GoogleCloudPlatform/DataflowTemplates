/*
 * Copyright (C) 2019 Google LLC
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
package com.google.cloud.teleport.spanner;

import static org.apache.avro.Schema.Type.BOOLEAN;
import static org.apache.avro.Schema.Type.BYTES;
import static org.apache.avro.Schema.Type.DOUBLE;
import static org.apache.avro.Schema.Type.FLOAT;
import static org.apache.avro.Schema.Type.INT;
import static org.apache.avro.Schema.Type.LONG;
import static org.apache.avro.Schema.Type.STRING;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.teleport.spanner.common.NumericUtils;
import com.google.cloud.teleport.spanner.common.Type;
import com.google.cloud.teleport.spanner.ddl.Table;
import java.nio.ByteBuffer;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.util.Utf8;
import org.junit.Test;

/** Unit tests for {@link AvroRecordConverter}. */
public class AvroRecordConverterTest {

  private static List<Integer> intArray = Arrays.asList(1, 2, 3, null);
  private static List<Long> longArray = Arrays.asList(1L, 2L, 3L, null);
  private static List<Float> floatArray = Arrays.asList(1f, 2f, 3f, null);
  private static List<Double> doubleArray = Arrays.asList(1d, 2d, 3d, null);
  private static List<Utf8> stringArray =
      Arrays.asList(new Utf8("1"), new Utf8("2"), new Utf8("3"), null);
  private static List<Boolean> booleanArray = Arrays.asList(true, false, null);

  private static final String LOGICAL_TYPE_PROP_NAME = "logicalType";
  private static final String TIMESTAMP_MILLIS_LOGICAL_TYPE = "timestamp-millis";
  private static final String TIMESTAMP_MICROS_LOGICAL_TYPE = "timestamp-micros";
  private static final String DATE_LOGICAL_TYPE = "date";

  @Test
  public void integerArray() {
    String colName = "arrayofint";
    Schema schema = createArrayAvroSchema(colName, LONG);

    // Null field
    GenericRecord avroRecord =
        new GenericRecordBuilder(schema).set("id", 0).set(colName, null).build();
    Optional<List<Long>> result = AvroRecordConverter.readInt64Array(avroRecord, LONG, colName);
    assertFalse(result.isPresent());

    // Convert from int to Int64.
    avroRecord = new GenericRecordBuilder(schema).set("id", 0).set(colName, intArray).build();
    result = AvroRecordConverter.readInt64Array(avroRecord, INT, colName);
    assertArrayEquals(longArray.toArray(), result.get().toArray());

    // Convert from long to Int64.
    avroRecord = new GenericRecordBuilder(schema).set("id", 0).set(colName, longArray).build();
    result = AvroRecordConverter.readInt64Array(avroRecord, LONG, colName);
    assertArrayEquals(longArray.toArray(), result.get().toArray());

    // Convert from String to Int64.
    avroRecord = new GenericRecordBuilder(schema).set("id", 0).set(colName, stringArray).build();
    result = AvroRecordConverter.readInt64Array(avroRecord, STRING, colName);
    assertArrayEquals(longArray.toArray(), result.get().toArray());

    // Other types throws exception
    final GenericRecord avroRecord1 =
        new GenericRecordBuilder(schema).set("id", 0).set(colName, booleanArray).build();
    assertThrows(
        IllegalArgumentException.class,
        () -> AvroRecordConverter.readInt64Array(avroRecord1, BOOLEAN, colName));

    Table.Builder tableBuilder = Table.builder();
    tableBuilder
        .name("record")
        .column("id")
        .type(Type.int64())
        .endColumn()
        .column(colName)
        .type(Type.array(Type.int64()))
        .endColumn();
    avroRecord = new GenericRecordBuilder(schema).set("id", 0L).set(colName, longArray).build();
    AvroRecordConverter avroRecordConverter = new AvroRecordConverter(tableBuilder.build());
    Mutation mutation = avroRecordConverter.apply(avroRecord);
    assertArrayEquals(
        longArray.toArray(), mutation.asMap().get(colName).getInt64Array().toArray(new Long[0]));
  }

  @Test
  public void testParseInteger() {
    String colName = "integer";

    // long to int64
    Table.Builder tableBuilder = Table.builder();
    tableBuilder.name("record").column(colName).type(Type.int64()).endColumn();
    GenericRecord avroRecord =
        new GenericRecordBuilder(createAvroSchema(colName, LONG)).set(colName, 10L).build();
    final AvroRecordConverter avroRecordConverter = new AvroRecordConverter(tableBuilder.build());
    Mutation mutation = avroRecordConverter.apply(avroRecord);
    assertEquals(10L, mutation.asMap().get(colName).getInt64());

    // Convert from int to Int64.
    avroRecord = new GenericRecordBuilder(createAvroSchema(colName, INT)).set(colName, 10).build();
    mutation = avroRecordConverter.apply(avroRecord);
    assertEquals(10L, mutation.asMap().get(colName).getInt64());

    // Convert from String to Int64.
    avroRecord =
        new GenericRecordBuilder(createAvroSchema(colName, STRING))
            .set(colName, new Utf8("10"))
            .build();
    mutation = avroRecordConverter.apply(avroRecord);
    assertEquals(10L, mutation.asMap().get(colName).getInt64());

    // Other types throws exception
    final GenericRecord avroRecord1 =
        new GenericRecordBuilder(createAvroSchema(colName, BOOLEAN)).set(colName, false).build();
    assertThrows(IllegalArgumentException.class, () -> avroRecordConverter.apply(avroRecord1));
  }

  @Test
  public void floatArray() {
    String colName = "arrayoffloat";
    Schema schema = createArrayAvroSchema(colName, DOUBLE);

    // Null field
    GenericRecord avroRecord = new GenericRecordBuilder(schema).set("id", 0).build();
    Optional<List<Double>> result =
        AvroRecordConverter.readFloat64Array(avroRecord, DOUBLE, colName);
    assertFalse(result.isPresent());

    // Convert from float to Float64.
    avroRecord = new GenericRecordBuilder(schema).set("id", 0).set(colName, floatArray).build();
    result = AvroRecordConverter.readFloat64Array(avroRecord, FLOAT, colName);
    assertArrayEquals(doubleArray.toArray(), result.get().toArray());

    // Convert from double to Float64.
    avroRecord = new GenericRecordBuilder(schema).set("id", 0).set(colName, doubleArray).build();
    result = AvroRecordConverter.readFloat64Array(avroRecord, DOUBLE, colName);
    assertArrayEquals(doubleArray.toArray(), result.get().toArray());

    // Convert from int to Float64.
    avroRecord = new GenericRecordBuilder(schema).set("id", 0).set(colName, intArray).build();
    result = AvroRecordConverter.readFloat64Array(avroRecord, INT, colName);
    assertArrayEquals(doubleArray.toArray(), result.get().toArray());

    // Convert from long to Float64.
    avroRecord = new GenericRecordBuilder(schema).set("id", 0).set(colName, longArray).build();
    result = AvroRecordConverter.readFloat64Array(avroRecord, LONG, colName);
    assertArrayEquals(doubleArray.toArray(), result.get().toArray());

    // Convert from String to Float64.
    avroRecord = new GenericRecordBuilder(schema).set("id", 0).set(colName, stringArray).build();
    result = AvroRecordConverter.readFloat64Array(avroRecord, STRING, colName);
    assertArrayEquals(doubleArray.toArray(), result.get().toArray());

    // Other types throw exception.
    final GenericRecord avroRecord1 =
        new GenericRecordBuilder(schema).set("id", 0).set(colName, booleanArray).build();
    assertThrows(
        IllegalArgumentException.class,
        () -> AvroRecordConverter.readFloat64Array(avroRecord1, BOOLEAN, colName));

    Table.Builder tableBuilder = Table.builder();
    tableBuilder
        .name("record")
        .column("id")
        .type(Type.int64())
        .endColumn()
        .column(colName)
        .type(Type.array(Type.float64()))
        .endColumn();
    avroRecord = new GenericRecordBuilder(schema).set("id", 1L).set(colName, doubleArray).build();
    AvroRecordConverter avroRecordConverter = new AvroRecordConverter(tableBuilder.build());
    Mutation mutation = avroRecordConverter.apply(avroRecord);
    assertArrayEquals(
        doubleArray.toArray(),
        mutation.asMap().get(colName).getFloat64Array().toArray(new Double[0]));
  }

  @Test
  public void testParseFloat() {
    String colName = "float";
    Table.Builder tableBuilder = Table.builder();
    tableBuilder.name("record").column(colName).type(Type.float64()).endColumn();

    // double to float64
    GenericRecord avroRecord =
        new GenericRecordBuilder(createAvroSchema(colName, DOUBLE)).set(colName, 10.5).build();
    final AvroRecordConverter avroRecordConverter = new AvroRecordConverter(tableBuilder.build());
    Mutation mutation = avroRecordConverter.apply(avroRecord);
    assertEquals(10.5, mutation.asMap().get(colName).getFloat64(), 0.000001);

    // float to float64
    avroRecord =
        new GenericRecordBuilder(createAvroSchema(colName, FLOAT))
            .set(colName, (float) 10.6)
            .build();
    mutation = avroRecordConverter.apply(avroRecord);
    assertEquals(10.6, mutation.asMap().get(colName).getFloat64(), 0.000001);

    // int to float64
    avroRecord = new GenericRecordBuilder(createAvroSchema(colName, INT)).set(colName, 9).build();
    mutation = avroRecordConverter.apply(avroRecord);
    assertEquals(9.0, mutation.asMap().get(colName).getFloat64(), 0.000001);

    // long to float64
    avroRecord =
        new GenericRecordBuilder(createAvroSchema(colName, LONG)).set(colName, 11L).build();
    mutation = avroRecordConverter.apply(avroRecord);
    assertEquals(11.0, mutation.asMap().get(colName).getFloat64(), 0.000001);

    // string to float64
    avroRecord =
        new GenericRecordBuilder(createAvroSchema(colName, STRING))
            .set(colName, new Utf8("15.3"))
            .build();
    mutation = avroRecordConverter.apply(avroRecord);
    assertEquals(15.3, mutation.asMap().get(colName).getFloat64(), 0.000001);

    // other types throw exception
    final GenericRecord avroRecord1 =
        new GenericRecordBuilder(createAvroSchema(colName, BOOLEAN)).set(colName, false).build();
    assertThrows(IllegalArgumentException.class, () -> avroRecordConverter.apply(avroRecord1));
  }

  @Test
  public void stringArray() {
    String colName = "arrayofstring";
    Schema schema = createArrayAvroSchema(colName, STRING);

    // Null field
    GenericRecord avroRecord = new GenericRecordBuilder(schema).set("id", 0).build();
    Optional<List<String>> result =
        AvroRecordConverter.readStringArray(avroRecord, STRING, colName);
    assertFalse(result.isPresent());

    // Convert from float to String.
    String[] expectedFloatStringArray = {"1.0", "2.0", "3.0", null};
    avroRecord = new GenericRecordBuilder(schema).set("id", 0).set(colName, floatArray).build();
    result = AvroRecordConverter.readStringArray(avroRecord, FLOAT, colName);
    assertArrayEquals(expectedFloatStringArray, result.get().toArray());

    // Convert from double to String.
    avroRecord = new GenericRecordBuilder(schema).set("id", 0).set(colName, doubleArray).build();
    result = AvroRecordConverter.readStringArray(avroRecord, DOUBLE, colName);
    assertArrayEquals(expectedFloatStringArray, result.get().toArray());

    // Convert from Utf8 to String.
    String[] expectedStringArray = {"1", "2", "3", null};
    avroRecord = new GenericRecordBuilder(schema).set("id", 0).set(colName, stringArray).build();
    result = AvroRecordConverter.readStringArray(avroRecord, STRING, colName);
    assertArrayEquals(expectedStringArray, result.get().toArray());

    // Convert from int to String.
    avroRecord = new GenericRecordBuilder(schema).set("id", 0).set(colName, intArray).build();
    result = AvroRecordConverter.readStringArray(avroRecord, INT, colName);
    assertArrayEquals(expectedStringArray, result.get().toArray());

    // Convert from long to String.
    avroRecord = new GenericRecordBuilder(schema).set("id", 0).set(colName, longArray).build();
    result = AvroRecordConverter.readStringArray(avroRecord, LONG, colName);
    assertArrayEquals(expectedStringArray, result.get().toArray());

    // Convert from boolean to String.
    String[] expectedBooleanArray = {"true", "false", null};
    avroRecord = new GenericRecordBuilder(schema).set("id", 0).set(colName, booleanArray).build();
    result = AvroRecordConverter.readStringArray(avroRecord, BOOLEAN, colName);
    assertArrayEquals(expectedBooleanArray, result.get().toArray());

    // Other types throw exception.
    final GenericRecord avroRecord1 =
        new GenericRecordBuilder(schema)
            .set("id", 0)
            .set(colName, new ArrayList<ByteArray>())
            .build();
    assertThrows(
        IllegalArgumentException.class,
        () -> AvroRecordConverter.readStringArray(avroRecord1, BYTES, colName));

    Table.Builder tableBuilder = Table.builder();
    tableBuilder
        .name("record")
        .column("id")
        .type(Type.int64())
        .endColumn()
        .column(colName)
        .type(Type.array(Type.string()))
        .endColumn();
    avroRecord = new GenericRecordBuilder(schema).set("id", 2L).set(colName, stringArray).build();
    AvroRecordConverter avroRecordConverter = new AvroRecordConverter(tableBuilder.build());
    Mutation mutation = avroRecordConverter.apply(avroRecord);
    assertArrayEquals(
        new String[] {"1", "2", "3", null},
        mutation.asMap().get(colName).getStringArray().toArray(new String[0]));

    schema =
        SchemaBuilder.record("record")
            .fields()
            .name(colName)
            .type()
            .optional()
            .array()
            .items()
            .nullable()
            .stringType()
            .endRecord();

    avroRecord = new GenericRecordBuilder(schema).set(colName, stringArray).build();
    avroRecordConverter = new AvroRecordConverter(tableBuilder.build());
    mutation = avroRecordConverter.apply(avroRecord);
    assertArrayEquals(
        new String[] {"1", "2", "3", null},
        mutation.asMap().get(colName).getStringArray().toArray(new String[0]));
  }

  @Test
  public void testParseString() {
    String colName = "string";
    Table.Builder tableBuilder = Table.builder();
    tableBuilder.name("record").column(colName).type(Type.string()).endColumn();

    // String to String
    GenericRecord avroRecord =
        new GenericRecordBuilder(createAvroSchema(colName, STRING))
            .set(colName, new Utf8("testString"))
            .build();
    final AvroRecordConverter avroRecordConverter = new AvroRecordConverter(tableBuilder.build());
    Mutation mutation = avroRecordConverter.apply(avroRecord);
    assertEquals("testString", mutation.asMap().get(colName).getString());

    // Convert from float to String.
    avroRecord =
        new GenericRecordBuilder(createAvroSchema(colName, FLOAT))
            .set(colName, (float) 9.9)
            .build();
    mutation = avroRecordConverter.apply(avroRecord);
    assertEquals("9.9", mutation.asMap().get(colName).getString());

    // Convert from double to String.
    avroRecord =
        new GenericRecordBuilder(createAvroSchema(colName, DOUBLE)).set(colName, 10.1).build();
    mutation = avroRecordConverter.apply(avroRecord);
    assertEquals("10.1", mutation.asMap().get(colName).getString());

    // Convert from int to String.
    avroRecord = new GenericRecordBuilder(createAvroSchema(colName, INT)).set(colName, 8).build();
    mutation = avroRecordConverter.apply(avroRecord);
    assertEquals("8", mutation.asMap().get(colName).getString());

    // Convert from long to String.
    avroRecord = new GenericRecordBuilder(createAvroSchema(colName, LONG)).set(colName, 7L).build();
    mutation = avroRecordConverter.apply(avroRecord);
    assertEquals("7", mutation.asMap().get(colName).getString());

    // Other types throw exception.
    final GenericRecord avroRecord1 =
        new GenericRecordBuilder(createAvroSchema(colName, BYTES))
            .set(colName, new ByteArray[0])
            .build();
    assertThrows(IllegalArgumentException.class, () -> avroRecordConverter.apply(avroRecord1));
  }

  @Test
  public void booleanArray() {
    String colName = "arrayofboolean";
    Schema schema = createArrayAvroSchema(colName, BOOLEAN);

    // Null field
    GenericRecord avroRecord = new GenericRecordBuilder(schema).set("id", 0).build();
    Optional<List<Boolean>> result =
        AvroRecordConverter.readBoolArray(avroRecord, BOOLEAN, colName);
    assertFalse(result.isPresent());

    // Convert from boolean to Boolean.
    avroRecord = new GenericRecordBuilder(schema).set("id", 0).set(colName, booleanArray).build();
    result = AvroRecordConverter.readBoolArray(avroRecord, BOOLEAN, colName);
    assertArrayEquals(booleanArray.toArray(), result.get().toArray());

    // Convert from String to boolean.
    List<Utf8> stringBooleanArray = Arrays.asList(new Utf8("true"), new Utf8("false"), null);
    avroRecord =
        new GenericRecordBuilder(schema).set("id", 0).set(colName, stringBooleanArray).build();
    result = AvroRecordConverter.readBoolArray(avroRecord, STRING, colName);
    assertArrayEquals(booleanArray.toArray(), result.get().toArray());

    avroRecord = new GenericRecordBuilder(schema).set("id", 0).set(colName, null).build();
    result = AvroRecordConverter.readBoolArray(avroRecord, STRING, colName);
    assertFalse(result.isPresent());

    // Other types throw exception.
    final GenericRecord avroRecord1 =
        new GenericRecordBuilder(schema).set("id", 0).set(colName, intArray).build();
    assertThrows(
        IllegalArgumentException.class,
        () -> AvroRecordConverter.readBoolArray(avroRecord1, INT, colName));

    Table.Builder tableBuilder = Table.builder();
    tableBuilder
        .name("record")
        .column("id")
        .type(Type.int64())
        .endColumn()
        .column(colName)
        .type(Type.array(Type.bool()))
        .endColumn();
    avroRecord = new GenericRecordBuilder(schema).set("id", 1L).set(colName, booleanArray).build();
    AvroRecordConverter avroRecordConverter = new AvroRecordConverter(tableBuilder.build());
    Mutation mutation = avroRecordConverter.apply(avroRecord);
    assertArrayEquals(
        booleanArray.toArray(),
        mutation.asMap().get(colName).getBoolArray().toArray(new Boolean[0]));
  }

  @Test
  public void testParseBoolean() {
    String colName = "boolean";
    Table.Builder tableBuilder = Table.builder();
    tableBuilder.name("record").column(colName).type(Type.bool()).endColumn();

    // boolean to boolean
    GenericRecord avroRecord =
        new GenericRecordBuilder(createAvroSchema(colName, BOOLEAN)).set(colName, false).build();
    final AvroRecordConverter avroRecordConverter = new AvroRecordConverter(tableBuilder.build());
    Mutation mutation = avroRecordConverter.apply(avroRecord);
    assertFalse(mutation.asMap().get(colName).getBool());

    // string to boolean
    avroRecord =
        new GenericRecordBuilder(createAvroSchema(colName, STRING))
            .set(colName, new Utf8("true"))
            .build();
    mutation = avroRecordConverter.apply(avroRecord);
    assertTrue(mutation.asMap().get(colName).getBool());

    // Other types throw exception.
    final GenericRecord avroRecord1 =
        new GenericRecordBuilder(createAvroSchema(colName, INT)).set(colName, 1).build();
    assertThrows(IllegalArgumentException.class, () -> avroRecordConverter.apply(avroRecord1));
  }

  @Test
  public void numericArray() {
    String colName = "arrayofnumeric";
    Schema schema = createArrayAvroSchema(colName, BYTES);

    // Null field
    GenericRecord avroRecord = new GenericRecordBuilder(schema).set("id", 0).build();
    Optional<List<String>> result =
        AvroRecordConverter.readNumericArray(avroRecord, BYTES, colName);
    assertFalse(result.isPresent());

    String[] readableNumericValues = {
      "-123.456000000",
      "0.000000000",
      "2387653.235320000",
      null,
      "0.000000020",
      "-99999999999999999999999999999.999999999",
      null,
      "100000000000000000000001.000001000"
    };
    List<ByteBuffer> avroNumericValues =
        Stream.of(readableNumericValues)
            .map(x -> x == null ? null : ByteBuffer.wrap(NumericUtils.stringToBytes(x)))
            .collect(Collectors.toList());
    avroRecord =
        new GenericRecordBuilder(schema).set("id", 0).set(colName, avroNumericValues).build();
    result = AvroRecordConverter.readNumericArray(avroRecord, BYTES, colName);
    assertArrayEquals(readableNumericValues, result.get().toArray());

    // throws exception for other types
    schema = createArrayAvroSchema(colName, INT);
    final GenericRecord avroRecord1 =
        new GenericRecordBuilder(schema).set("id", 0).set(colName, intArray).build();
    assertThrows(
        IllegalArgumentException.class,
        () -> AvroRecordConverter.readNumericArray(avroRecord1, INT, colName));
    assertArrayEquals(readableNumericValues, result.get().toArray());

    schema = createArrayAvroSchema(colName, BYTES);
    Table.Builder tableBuilder = Table.builder();
    tableBuilder
        .name("record")
        .column("id")
        .type(Type.int64())
        .endColumn()
        .column(colName)
        .type(Type.array(Type.numeric()))
        .endColumn();
    avroRecord =
        new GenericRecordBuilder(schema).set("id", 3L).set(colName, avroNumericValues).build();
    AvroRecordConverter avroRecordConverter = new AvroRecordConverter(tableBuilder.build());
    Mutation mutation = avroRecordConverter.apply(avroRecord);
    assertArrayEquals(
        readableNumericValues,
        mutation.asMap().get(colName).getStringArray().toArray(new String[0]));
  }

  @Test
  public void testParseNumeric() {
    String colName = "string";
    String expectedNumericValue = "100000000000000000000001.000001000";
    Table.Builder tableBuilder = Table.builder();
    tableBuilder.name("record").column(colName).type(Type.numeric()).endColumn();
    GenericRecord avroRecord =
        new GenericRecordBuilder(createAvroSchema(colName, BYTES))
            .set(colName, ByteBuffer.wrap(NumericUtils.stringToBytes(expectedNumericValue)))
            .build();
    final AvroRecordConverter avroRecordConverter = new AvroRecordConverter(tableBuilder.build());
    Mutation mutation = avroRecordConverter.apply(avroRecord);
    assertEquals(expectedNumericValue, mutation.asMap().get(colName).getString());

    // Other types throw exception.
    final GenericRecord avroRecord1 =
        new GenericRecordBuilder(createAvroSchema(colName, INT)).set(colName, 3).build();
    assertThrows(IllegalArgumentException.class, () -> avroRecordConverter.apply(avroRecord1));
  }

  @Test
  public void timestampArray() {
    String colName = "arrayoftimestamp";
    Schema schema = createArrayAvroSchema(colName, STRING);

    // Null field
    GenericRecord avroRecord = new GenericRecordBuilder(schema).set("id", 0).build();
    Optional<List<Timestamp>> result =
        AvroRecordConverter.readTimestampArray(avroRecord, STRING, null, colName);
    assertFalse(result.isPresent());

    // Strings as Timestamps.
    String[] timestampStrings = {"2019-02-13T01:00:00Z", "1969-07-20T20:17:00Z", null};
    List<Utf8> utf8Timestamps =
        Stream.of(timestampStrings)
            .map(x -> x == null ? null : new Utf8(x))
            .collect(Collectors.toList());
    Timestamp[] expectedTimestamps =
        Stream.of(timestampStrings)
            .map(x -> x == null ? null : Timestamp.parseTimestamp(x))
            .toArray(Timestamp[]::new);
    avroRecord = new GenericRecordBuilder(schema).set("id", 0).set(colName, utf8Timestamps).build();
    result = AvroRecordConverter.readTimestampArray(avroRecord, STRING, null, colName);
    assertArrayEquals(expectedTimestamps, result.get().toArray());

    // Longs as microsecond Timestamps.
    List<Long> longMicroTimestamps =
        Stream.of(expectedTimestamps)
            .map(x -> x == null ? null : x.getSeconds() * 1000000L + x.getNanos() / 1000)
            .collect(Collectors.toList());
    avroRecord =
        new GenericRecordBuilder(schema).set("id", 0).set(colName, longMicroTimestamps).build();
    result = AvroRecordConverter.readTimestampArray(avroRecord, LONG, null, colName);
    assertArrayEquals(expectedTimestamps, result.get().toArray());

    // With logical type
    result =
        AvroRecordConverter.readTimestampArray(
            avroRecord, LONG, LogicalTypes.timestampMicros(), colName);
    assertArrayEquals(expectedTimestamps, result.get().toArray());

    // Longs as millisecond Timestamps.
    List<Long> longMilliTimestamps =
        Stream.of(expectedTimestamps)
            .map(x -> x == null ? null : x.getSeconds() * 1000L + x.getNanos() / 1000000L)
            .collect(Collectors.toList());
    avroRecord =
        new GenericRecordBuilder(schema).set("id", 0).set(colName, longMilliTimestamps).build();
    result =
        AvroRecordConverter.readTimestampArray(
            avroRecord, LONG, LogicalTypes.timestampMillis(), colName);
    assertArrayEquals(expectedTimestamps, result.get().toArray());

    final GenericRecord avroRecord2 =
        new GenericRecordBuilder(schema).set("id", 0).set(colName, longMilliTimestamps).build();
    assertThrows(
        IllegalArgumentException.class,
        () ->
            AvroRecordConverter.readTimestampArray(
                avroRecord2, LONG, LogicalTypes.date(), colName));

    // Other types throw exception.
    final GenericRecord avroRecord1 =
        new GenericRecordBuilder(schema).set("id", 0).set(colName, floatArray).build();
    assertThrows(
        IllegalArgumentException.class,
        () -> AvroRecordConverter.readTimestampArray(avroRecord1, FLOAT, null, colName));

    Table.Builder tableBuilder = Table.builder();
    tableBuilder
        .name("record")
        .column("id")
        .type(Type.int64())
        .endColumn()
        .column(colName)
        .type(Type.array(Type.timestamp()))
        .endColumn();
    avroRecord =
        new GenericRecordBuilder(schema).set("id", 1L).set(colName, utf8Timestamps).build();
    AvroRecordConverter avroRecordConverter = new AvroRecordConverter(tableBuilder.build());
    Mutation mutation = avroRecordConverter.apply(avroRecord);
    assertArrayEquals(
        expectedTimestamps,
        mutation.asMap().get(colName).getTimestampArray().toArray(new Timestamp[0]));
  }

  @Test
  public void testParseTimestamp() {
    String colName = "timestamp";
    String expectedTimestampValue = "2019-02-13T01:00:00Z";
    Table.Builder tableBuilder = Table.builder();
    tableBuilder.name("record").column(colName).type(Type.timestamp()).endColumn();

    // string to Timestamp
    GenericRecord avroRecord =
        new GenericRecordBuilder(createAvroSchema(colName, STRING))
            .set(colName, new Utf8(expectedTimestampValue))
            .build();
    final AvroRecordConverter avroRecordConverter = new AvroRecordConverter(tableBuilder.build());
    Mutation mutation = avroRecordConverter.apply(avroRecord);
    assertEquals(
        Timestamp.parseTimestamp(expectedTimestampValue),
        mutation.asMap().get(colName).getTimestamp());

    // Longs as microsecond Timestamps.
    Timestamp timestamp = Timestamp.parseTimestamp(expectedTimestampValue);
    Long timestampMicrosecond = timestamp.getSeconds() * 1000000L + timestamp.getNanos() / 1000;
    avroRecord =
        new GenericRecordBuilder(createAvroSchema(colName, LONG))
            .set(colName, timestampMicrosecond)
            .build();
    mutation = avroRecordConverter.apply(avroRecord);
    assertEquals(timestamp, mutation.asMap().get(colName).getTimestamp());

    // With logical type
    Schema schema = createAvroSchema(colName, LONG, TIMESTAMP_MICROS_LOGICAL_TYPE);
    avroRecord = new GenericRecordBuilder(schema).set(colName, timestampMicrosecond).build();
    mutation = avroRecordConverter.apply(avroRecord);
    assertEquals(timestamp, mutation.asMap().get(colName).getTimestamp());

    // Longs as millisecond Timestamps.
    schema = createAvroSchema(colName, LONG, TIMESTAMP_MILLIS_LOGICAL_TYPE);
    Long timestampMilliSecond = timestamp.getSeconds() * 1000 + timestamp.getNanos() / 1000000L;
    avroRecord = new GenericRecordBuilder(schema).set(colName, timestampMilliSecond).build();
    mutation = avroRecordConverter.apply(avroRecord);
    assertEquals(timestamp, mutation.asMap().get(colName).getTimestamp());

    // Other types throw exception.
    schema = createAvroSchema(colName, DOUBLE);
    final GenericRecord avroRecord1 = new GenericRecordBuilder(schema).set(colName, 1.1).build();
    assertThrows(IllegalArgumentException.class, () -> avroRecordConverter.apply(avroRecord1));
  }

  @Test
  public void dateArray() {
    String colName = "arrayofdate";
    Schema schema = createArrayAvroSchema(colName, STRING);

    // Strings as Dates.
    String[] dateStrings = {"2019-02-13", "1969-07-20", null};
    List<Utf8> utf8Dates =
        Stream.of(dateStrings)
            .map(x -> x == null ? null : new Utf8(x))
            .collect(Collectors.toList());
    Date[] expectedDates =
        Stream.of(dateStrings).map(x -> x == null ? null : Date.parseDate(x)).toArray(Date[]::new);

    Table.Builder tableBuilder = Table.builder();
    tableBuilder
        .name("record")
        .column("id")
        .type(Type.int64())
        .endColumn()
        .column(colName)
        .type(Type.array(Type.date()))
        .endColumn();
    GenericRecord avroRecord =
        new GenericRecordBuilder(schema).set("id", 5L).set(colName, utf8Dates).build();
    final AvroRecordConverter avroRecordConverter = new AvroRecordConverter(tableBuilder.build());
    Mutation mutation = avroRecordConverter.apply(avroRecord);
    assertArrayEquals(
        expectedDates, mutation.asMap().get(colName).getDateArray().toArray(new Date[0]));

    // Empty array
    avroRecord = new GenericRecordBuilder(schema).set("id", 5L).set(colName, null).build();
    mutation = avroRecordConverter.apply(avroRecord);
    assertTrue(mutation.asMap().get(colName).isNull());

    // Other types throw exception.
    schema = createArrayAvroSchema(colName, DOUBLE);
    final GenericRecord avroRecord1 =
        new GenericRecordBuilder(schema).set("id", 1L).set(colName, doubleArray).build();
    assertThrows(IllegalArgumentException.class, () -> avroRecordConverter.apply(avroRecord1));
  }

  @Test
  public void testParseDate() throws ParseException {
    String colName = "date";
    Schema schema = createAvroSchema(colName, STRING);

    String expectedDateValue = "2019-02-13";
    Table.Builder tableBuilder = Table.builder();
    tableBuilder.name("record").column(colName).type(Type.date()).endColumn();
    GenericRecord avroRecord =
        new GenericRecordBuilder(schema).set(colName, new Utf8(expectedDateValue)).build();
    final AvroRecordConverter avroRecordConverter = new AvroRecordConverter(tableBuilder.build());
    Mutation mutation = avroRecordConverter.apply(avroRecord);
    assertEquals(Date.parseDate(expectedDateValue), mutation.asMap().get(colName).getDate());

    schema = createAvroSchema(colName, INT, DATE_LOGICAL_TYPE);

    SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
    java.util.Date date = formatter.parse(expectedDateValue);
    Integer intDate = (int) (date.getTime() / (24L * 3600L * 1000L) + 1);
    avroRecord = new GenericRecordBuilder(schema).set(colName, intDate).build();
    mutation = avroRecordConverter.apply(avroRecord);
    assertFalse(mutation.asMap().get(colName).isNull());

    // Other types throw exception.
    final GenericRecord avroRecord1 =
        new GenericRecordBuilder(createAvroSchema(colName, DOUBLE)).set(colName, 5.0).build();
    assertThrows(IllegalArgumentException.class, () -> avroRecordConverter.apply(avroRecord1));

    final GenericRecord avroRecord2 =
        new GenericRecordBuilder(createAvroSchema(colName, INT)).set(colName, 5).build();
    assertThrows(IllegalArgumentException.class, () -> avroRecordConverter.apply(avroRecord2));
  }

  @Test
  public void pgNumericArray() {
    String colName = "arrayOfPgNumeric";
    Schema schema = createArrayAvroSchema(colName, BYTES);

    // Null field
    GenericRecord avroRecord = new GenericRecordBuilder(schema).set("id", 0).build();
    Optional<List<String>> result =
        AvroRecordConverter.readPgNumericArray(avroRecord, BYTES, colName);
    assertFalse(result.isPresent());

    StringBuilder maxPgNumeric = new StringBuilder();
    StringBuilder minPgNumeric = new StringBuilder("-");
    for (int i = 0; i < NumericUtils.PG_MAX_PRECISION - NumericUtils.PG_MAX_SCALE; i++) {
      maxPgNumeric.append("9");
      minPgNumeric.append("9");
    }
    maxPgNumeric.append(".");
    minPgNumeric.append(".");
    for (int i = 0; i < NumericUtils.PG_MAX_SCALE; i++) {
      maxPgNumeric.append("9");
      minPgNumeric.append("9");
    }

    String[] readablePgNumericValues = {
      "-123.456000000",
      "0.000000000",
      "2387653.235320000",
      null,
      "0.000000020",
      "-99999999999999999999999999999.999999999",
      "NaN",
      "100000000000000000000001.000001000",
      maxPgNumeric.toString(),
      minPgNumeric.toString()
    };

    List<ByteBuffer> avroPgNumericValues =
        Stream.of(readablePgNumericValues)
            .map(x -> x == null ? null : ByteBuffer.wrap(NumericUtils.pgStringToBytes(x)))
            .collect(Collectors.toList());
    avroRecord =
        new GenericRecordBuilder(schema).set("id", 0).set(colName, avroPgNumericValues).build();
    result = AvroRecordConverter.readPgNumericArray(avroRecord, BYTES, colName);
    assertArrayEquals(readablePgNumericValues, result.get().toArray());

    // Other types throw exception.
    schema = createArrayAvroSchema(colName, INT);
    final GenericRecord avroRecord1 =
        new GenericRecordBuilder(schema).set("id", 0).set(colName, intArray).build();
    assertThrows(
        IllegalArgumentException.class,
        () -> AvroRecordConverter.readPgNumericArray(avroRecord1, INT, colName));

    schema = createArrayAvroSchema(colName, BYTES);
    Table.Builder tableBuilder = Table.builder(Dialect.POSTGRESQL);
    tableBuilder
        .name("record")
        .column("id")
        .type(Type.int64())
        .endColumn()
        .column(colName)
        .type(Type.pgArray(Type.pgNumeric()))
        .endColumn();
    avroRecord =
        new GenericRecordBuilder(schema).set("id", 2L).set(colName, avroPgNumericValues).build();
    AvroRecordConverter avroRecordConverter = new AvroRecordConverter(tableBuilder.build());
    Mutation mutation = avroRecordConverter.apply(avroRecord);
    assertArrayEquals(
        readablePgNumericValues,
        mutation.asMap().get(colName).getStringArray().toArray(new String[0]));
  }

  @Test
  public void testParsePgNumeric() {
    String colName = "string";
    Schema schema = createAvroSchema(colName, BYTES);
    String expectedNumericValue = "100000000000000000000001.000001000";
    Table.Builder tableBuilder = Table.builder();
    tableBuilder.name("record").column(colName).type(Type.pgNumeric()).endColumn();
    GenericRecord avroRecord =
        new GenericRecordBuilder(schema)
            .set(colName, ByteBuffer.wrap(NumericUtils.pgStringToBytes(expectedNumericValue)))
            .build();
    final AvroRecordConverter avroRecordConverter = new AvroRecordConverter(tableBuilder.build());
    Mutation mutation = avroRecordConverter.apply(avroRecord);
    assertEquals(expectedNumericValue, mutation.asMap().get(colName).getString());

    // Other types throw exception.
    final GenericRecord avroRecord1 =
        new GenericRecordBuilder(createAvroSchema(colName, INT)).set(colName, 20).build();
    assertThrows(IllegalArgumentException.class, () -> avroRecordConverter.apply(avroRecord1));
  }

  @Test
  public void bytesArray() {
    String colName = "arrayofbytes";
    Schema schema = createArrayAvroSchema(colName, BYTES);

    // Null field
    GenericRecord avroRecord = new GenericRecordBuilder(schema).set("id", 0L).build();
    Table.Builder tableBuilder = Table.builder();
    tableBuilder
        .name("record")
        .column("id")
        .type(Type.int64())
        .endColumn()
        .column(colName)
        .type(Type.array(Type.bytes()))
        .endColumn();
    final AvroRecordConverter avroRecordConverter = new AvroRecordConverter(tableBuilder.build());
    Mutation mutation = avroRecordConverter.apply(avroRecord);
    assertTrue(mutation.asMap().get(colName).isNull());

    String[] readableByteValues = {"1", "2", "3", null};
    List<ByteArray> expectedByteArrays =
        Stream.of(readableByteValues)
            .map(x -> x == null ? null : ByteArray.copyFrom(x))
            .collect(Collectors.toList());
    List<ByteBuffer> avroByteValues =
        Stream.of(readableByteValues)
            .map(x -> x == null ? null : ByteBuffer.wrap(x.getBytes()))
            .collect(Collectors.toList());
    avroRecord =
        new GenericRecordBuilder(schema).set("id", 2L).set(colName, avroByteValues).build();
    mutation = avroRecordConverter.apply(avroRecord);
    assertArrayEquals(
        expectedByteArrays.toArray(),
        mutation.asMap().get(colName).getBytesArray().toArray(new ByteArray[0]));

    // Other types throw exception.
    schema = createArrayAvroSchema(colName, INT);
    final GenericRecord avroRecord1 =
        new GenericRecordBuilder(schema).set("id", 9L).set(colName, intArray).build();
    assertThrows(IllegalArgumentException.class, () -> avroRecordConverter.apply(avroRecord1));

    schema = createArrayAvroSchema(colName, STRING);

    // Null field
    avroRecord = new GenericRecordBuilder(schema).set("id", 8L).set(colName, null).build();
    mutation = avroRecordConverter.apply(avroRecord);
    assertTrue(mutation.asMap().get(colName).isNull());

    // String as bytes
    avroRecord = new GenericRecordBuilder(schema).set("id", 7L).set(colName, stringArray).build();
    mutation = avroRecordConverter.apply(avroRecord);
    assertArrayEquals(
        expectedByteArrays.toArray(),
        mutation.asMap().get(colName).getBytesArray().toArray(new ByteArray[0]));
  }

  @Test
  public void testParseBytes() {
    String colName = "bytes";
    String expectedNumericValue = "1.0";
    Table.Builder tableBuilder = Table.builder();
    tableBuilder.name("record").column(colName).type(Type.bytes()).endColumn();
    GenericRecord avroRecord =
        new GenericRecordBuilder(createAvroSchema(colName, BYTES))
            .set(colName, ByteBuffer.wrap(expectedNumericValue.getBytes()))
            .build();
    final AvroRecordConverter avroRecordConverter = new AvroRecordConverter(tableBuilder.build());
    Mutation mutation = avroRecordConverter.apply(avroRecord);
    assertEquals(
        expectedNumericValue, new String(mutation.asMap().get(colName).getBytes().toByteArray()));

    // String to Bytes
    avroRecord =
        new GenericRecordBuilder(createAvroSchema(colName, STRING))
            .set(colName, new Utf8(expectedNumericValue))
            .build();
    mutation = avroRecordConverter.apply(avroRecord);
    assertEquals(
        expectedNumericValue, new String(mutation.asMap().get(colName).getBytes().toByteArray()));

    // Other types throw exception.
    final GenericRecord avroRecord1 =
        new GenericRecordBuilder(createAvroSchema(colName, INT)).set(colName, 4).build();
    assertThrows(IllegalArgumentException.class, () -> avroRecordConverter.apply(avroRecord1));
  }

  @Test
  public void invalidColumnInAvro() {
    String colName = "arrayofint";
    Schema schema = createArrayAvroSchema(colName, LONG);

    GenericRecord avroRecord =
        new GenericRecordBuilder(schema).set("id", 0L).set(colName, null).build();
    Table.Builder tableBuilder = Table.builder();
    tableBuilder.name("record").column("id1");
    AvroRecordConverter avroRecordConverter = new AvroRecordConverter(tableBuilder.build());
    assertThrows(IllegalArgumentException.class, () -> avroRecordConverter.apply(avroRecord));

    schema =
        SchemaBuilder.builder()
            .record("record")
            .fields()
            .requiredLong("id")
            .name("union")
            .type()
            .unionOf()
            .booleanType()
            .and()
            .stringType()
            .endUnion()
            .booleanDefault(true)
            .endRecord();

    GenericRecord avroRecord1 = new GenericRecordBuilder(schema).set("id", 0L).build();
    tableBuilder = Table.builder();
    tableBuilder
        .name("record")
        .column("id")
        .type(Type.int64())
        .endColumn()
        .column("union")
        .type(Type.struct())
        .endColumn();
    AvroRecordConverter avroRecordConverter1 = new AvroRecordConverter(tableBuilder.build());
    assertThrows(IllegalArgumentException.class, () -> avroRecordConverter1.apply(avroRecord1));

    schema =
        SchemaBuilder.builder()
            .record("record")
            .fields()
            .requiredLong("id")
            .name("union")
            .type()
            .array()
            .items()
            .unionOf()
            .booleanType()
            .and()
            .stringType()
            .endUnion()
            .arrayDefault(null)
            .endRecord();

    GenericRecord avroRecord2 = new GenericRecordBuilder(schema).set("id", 0L).build();
    tableBuilder = Table.builder();
    tableBuilder
        .name("record")
        .column("id")
        .type(Type.int64())
        .endColumn()
        .column("union")
        .type(Type.array(Type.struct()))
        .endColumn();
    AvroRecordConverter avroRecordConverter2 = new AvroRecordConverter(tableBuilder.build());
    assertThrows(IllegalArgumentException.class, () -> avroRecordConverter2.apply(avroRecord2));
  }

  private Schema createAvroSchema(String columnName, Schema.Type type) {
    return createAvroSchema(columnName, type, null);
  }

  private Schema createAvroSchema(String columnName, Schema.Type type, String logicalType) {
    Schema typeSchema = Schema.create(type);
    if (logicalType != null) {
      typeSchema.addProp(LOGICAL_TYPE_PROP_NAME, logicalType);
    }
    return SchemaBuilder.record("record")
        .fields()
        .name(columnName)
        .type()
        .optional()
        .type(typeSchema)
        .endRecord();
  }

  private Schema createArrayAvroSchema(String columnName, Schema.Type type) {
    return createArrayAvroSchema(columnName, type, null);
  }

  private Schema createArrayAvroSchema(String columnName, Schema.Type type, String logicalType) {
    Schema typeSchema = Schema.create(type);
    if (logicalType != null) {
      typeSchema.addProp(LOGICAL_TYPE_PROP_NAME, logicalType);
    }
    return SchemaBuilder.record("record")
        .fields()
        .requiredLong("id")
        .name(columnName)
        .type()
        .optional()
        .array()
        .items()
        .type(typeSchema)
        .endRecord();
  }
}
