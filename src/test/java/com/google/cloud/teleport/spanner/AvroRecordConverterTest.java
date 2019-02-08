/*
 * Copyright (C) 2019 Google Inc.
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

package com.google.cloud.teleport.spanner;

import static org.apache.avro.Schema.Type.BOOLEAN;
import static org.apache.avro.Schema.Type.DOUBLE;
import static org.apache.avro.Schema.Type.FLOAT;
import static org.apache.avro.Schema.Type.INT;
import static org.apache.avro.Schema.Type.LONG;
import static org.apache.avro.Schema.Type.STRING;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
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
    Schema schema =
        SchemaBuilder.record("record")
            .fields()
            .requiredLong("id")
            .name("arrayofint")
            .type()
            .nullable()
            .array()
            .items()
            .longType()
            .noDefault()
            .endRecord();

    // Null field
    GenericRecord avroRecord =
        new GenericRecordBuilder(schema).set("id", 0).set("arrayofint", null).build();
    Optional<List<Long>> result =
        AvroRecordConverter.readInt64Array(avroRecord, LONG, "arrayofint");
    assertFalse(result.isPresent());

    // Convert from int to Int64.
    avroRecord = new GenericRecordBuilder(schema).set("id", 0).set("arrayofint", intArray).build();
    result = AvroRecordConverter.readInt64Array(avroRecord, INT, "arrayofint");
    assertArrayEquals(longArray.toArray(), result.get().toArray());

    // Convert from long to Int64.
    avroRecord = new GenericRecordBuilder(schema).set("id", 0).set("arrayofint", longArray).build();
    result = AvroRecordConverter.readInt64Array(avroRecord, LONG, "arrayofint");
    assertArrayEquals(longArray.toArray(), result.get().toArray());

    // Convert from String to Int64.
    avroRecord =
        new GenericRecordBuilder(schema).set("id", 0).set("arrayofint", stringArray).build();
    result = AvroRecordConverter.readInt64Array(avroRecord, STRING, "arrayofint");
    assertArrayEquals(longArray.toArray(), result.get().toArray());
  }

  @Test
  public void floatArray() {
    Schema schema =
        SchemaBuilder.record("record")
            .fields()
            .requiredLong("id")
            .name("arrayoffloat")
            .type()
            .optional()
            .array()
            .items()
            .doubleType()
            .endRecord();

    // Null field
    GenericRecord avroRecord = new GenericRecordBuilder(schema).set("id", 0).build();
    Optional<List<Double>> result =
        AvroRecordConverter.readFloat64Array(avroRecord, DOUBLE, "arrayoffloat");
    assertFalse(result.isPresent());

    // Convert from float to Float64.
    avroRecord =
        new GenericRecordBuilder(schema).set("id", 0).set("arrayoffloat", floatArray).build();
    result = AvroRecordConverter.readFloat64Array(avroRecord, FLOAT, "arrayoffloat");
    assertArrayEquals(doubleArray.toArray(), result.get().toArray());

    // Convert from double to Float64.
    avroRecord =
        new GenericRecordBuilder(schema).set("id", 0).set("arrayoffloat", doubleArray).build();
    result = AvroRecordConverter.readFloat64Array(avroRecord, DOUBLE, "arrayoffloat");
    assertArrayEquals(doubleArray.toArray(), result.get().toArray());

    // Convert from int to Float64.
    avroRecord =
        new GenericRecordBuilder(schema).set("id", 0).set("arrayoffloat", intArray).build();
    result = AvroRecordConverter.readFloat64Array(avroRecord, INT, "arrayoffloat");
    assertArrayEquals(doubleArray.toArray(), result.get().toArray());

    // Convert from long to Float64.
    avroRecord =
        new GenericRecordBuilder(schema).set("id", 0).set("arrayoffloat", longArray).build();
    result = AvroRecordConverter.readFloat64Array(avroRecord, LONG, "arrayoffloat");
    assertArrayEquals(doubleArray.toArray(), result.get().toArray());

    // Convert from String to Float64.
    avroRecord =
        new GenericRecordBuilder(schema).set("id", 0).set("arrayoffloat", stringArray).build();
    result = AvroRecordConverter.readFloat64Array(avroRecord, STRING, "arrayoffloat");
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
}
