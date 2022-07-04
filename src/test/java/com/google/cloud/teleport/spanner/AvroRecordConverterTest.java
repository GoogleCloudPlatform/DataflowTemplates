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
import static org.junit.Assert.assertFalse;

import com.google.cloud.Timestamp;
import com.google.cloud.teleport.spanner.common.NumericUtils;
import java.nio.ByteBuffer;
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

  @Test
  public void integerArray() {
    String colName = "arrayofint";
    Schema schema =
        SchemaBuilder.record("record")
            .fields()
            .requiredLong("id")
            .name(colName)
            .type()
            .nullable()
            .array()
            .items()
            .longType()
            .noDefault()
            .endRecord();

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
  }

  @Test
  public void floatArray() {
    String colName = "arrayoffloat";
    Schema schema =
        SchemaBuilder.record("record")
            .fields()
            .requiredLong("id")
            .name(colName)
            .type()
            .optional()
            .array()
            .items()
            .doubleType()
            .endRecord();

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
  }

  @Test
  public void stringArray() {
    String colName = "arrayofstring";
    Schema schema =
        SchemaBuilder.record("record")
            .fields()
            .requiredLong("id")
            .name(colName)
            .type()
            .optional()
            .array()
            .items()
            .stringType()
            .endRecord();

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
  }

  @Test
  public void booleanArray() {
    String colName = "arrayofboolean";
    Schema schema =
        SchemaBuilder.record("record")
            .fields()
            .requiredLong("id")
            .name(colName)
            .type()
            .optional()
            .array()
            .items()
            .booleanType()
            .endRecord();

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
  }

  @Test
  public void numericArray() {
    String colName = "arrayofnumeric";
    Schema schema =
        SchemaBuilder.record("record")
            .fields()
            .requiredBytes("id")
            .name(colName)
            .type()
            .optional()
            .array()
            .items()
            .bytesType()
            .endRecord();

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
  }

  @Test
  public void timestampArray() {
    String colName = "arrayoftimestamp";
    Schema schema =
        SchemaBuilder.record("record")
            .fields()
            .requiredLong("id")
            .name(colName)
            .type()
            .optional()
            .array()
            .items()
            .stringType()
            .endRecord();

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
  }

  @Test
  public void pgNumericArray() {
    String colName = "arrayOfPgNumeric";
    Schema schema =
        SchemaBuilder.record("record")
            .fields()
            .requiredBytes("id")
            .name(colName)
            .type()
            .optional()
            .array()
            .items()
            .bytesType()
            .endRecord();

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
  }
}
