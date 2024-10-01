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
package com.google.cloud.teleport.spanner;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Value;
import com.google.cloud.teleport.spanner.common.NumericUtils;
import com.google.cloud.teleport.spanner.common.Type;
import com.google.cloud.teleport.spanner.ddl.Ddl;
import com.google.common.collect.Lists;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.junit.Test;

/** Test for {@link SpannerRecordConverter}. */
public class SpannerRecordConverterTest {

  private DdlToAvroSchemaConverter converter =
      new DdlToAvroSchemaConverter("booleans", "booleans", false);

  private DdlToAvroSchemaConverter logicalTypeConverter =
      new DdlToAvroSchemaConverter("booleans", "booleans", true);

  @Test
  public void simple() {
    Ddl ddl =
        Ddl.builder()
            .createTable("users")
            .column("id")
            .int64()
            .notNull()
            .endColumn()
            .column("email")
            .string()
            .size(15)
            .notNull()
            .endColumn()
            .column("name")
            .string()
            .max()
            .endColumn()
            .column("bool")
            .bool()
            .endColumn()
            .column("float")
            .float32()
            .endColumn()
            .column("double")
            .float64()
            .endColumn()
            .primaryKey()
            .asc("id")
            .end()
            .endTable()
            .build();
    Schema schema = converter.convert(ddl).iterator().next();
    SpannerRecordConverter recordConverter = new SpannerRecordConverter(schema);
    Struct struct =
        Struct.newBuilder()
            .set("id")
            .to(1L)
            .set("email")
            .to("abc@google.com")
            .set("name")
            .to("John Doe")
            .set("bool")
            .to(true)
            .set("float")
            .to(3.2f)
            .set("double")
            .to(30.2)
            .build();

    GenericRecord avroRecord = recordConverter.convert(struct);

    assertThat(avroRecord.get("id"), equalTo(1L));
    assertThat(avroRecord.get("email"), equalTo("abc@google.com"));
    assertThat(avroRecord.get("name"), equalTo("John Doe"));
    assertEquals(true, avroRecord.get("bool"));
    assertEquals(3.2f, avroRecord.get("float"));
    assertEquals(30.2, avroRecord.get("double"));
  }

  @Test
  public void nulls() {
    Ddl ddl =
        Ddl.builder()
            .createTable("users")
            .column("id")
            .int64()
            .notNull()
            .endColumn()
            .column("age")
            .int64()
            .endColumn()
            .column("name")
            .string()
            .max()
            .endColumn()
            .column("bytes")
            .bytes()
            .max()
            .endColumn()
            .column("date")
            .date()
            .endColumn()
            .column("ts")
            .timestamp()
            .endColumn()
            .column("bool")
            .bool()
            .endColumn()
            .column("float")
            .float32()
            .endColumn()
            .column("double")
            .float64()
            .endColumn()
            .primaryKey()
            .asc("id")
            .end()
            .endTable()
            .build();
    Schema schema = converter.convert(ddl).iterator().next();
    SpannerRecordConverter recordConverter = new SpannerRecordConverter(schema);
    Struct struct =
        Struct.newBuilder()
            .set("id")
            .to(1L)
            .set("age")
            .to((Long) null)
            .set("name")
            .to((String) null)
            .set("bytes")
            .to((ByteArray) null)
            .set("date")
            .to((Date) null)
            .set("ts")
            .to((Timestamp) null)
            .set("bool")
            .to((Boolean) null)
            .set("float")
            .to((Float) null)
            .set("double")
            .to((Double) null)
            .build();

    GenericRecord avroRecord = recordConverter.convert(struct);

    assertThat(avroRecord.get("id"), equalTo(1L));
    assertThat(avroRecord.get("age"), is((Long) null));
    assertThat(avroRecord.get("name"), is((String) null));
    assertThat(avroRecord.get("bytes"), is((ByteArray) null));
    assertThat(avroRecord.get("date"), is((String) null));
    assertThat(avroRecord.get("ts"), is((String) null));
    assertThat(avroRecord.get("bool"), is((Boolean) null));
    assertThat(avroRecord.get("float"), is((Float) null));
    assertThat(avroRecord.get("double"), is((Double) null));
  }

  @Test
  public void dateTimestamp() {
    Ddl ddl =
        Ddl.builder()
            .createTable("users")
            .column("id")
            .int64()
            .notNull()
            .endColumn()
            .column("date")
            .date()
            .endColumn()
            .column("ts")
            .timestamp()
            .endColumn()
            .primaryKey()
            .asc("id")
            .end()
            .endTable()
            .build();
    Schema schema = converter.convert(ddl).iterator().next();
    SpannerRecordConverter recordConverter = new SpannerRecordConverter(schema);
    Struct struct =
        Struct.newBuilder()
            .set("id")
            .to(1L)
            .set("date")
            .to(Date.fromYearMonthDay(2018, 2, 2))
            .set("ts")
            .to(Timestamp.ofTimeMicroseconds(10))
            .build();

    GenericRecord avroRecord = recordConverter.convert(struct);

    assertThat(avroRecord.get("id"), equalTo(1L));
    assertThat(avroRecord.get("date"), equalTo("2018-02-02"));
    assertThat(avroRecord.get("ts"), equalTo("1970-01-01T00:00:00.000010000Z"));
  }

  @Test
  public void timestampLogical() {
    Ddl ddl =
        Ddl.builder()
            .createTable("users")
            .column("id")
            .int64()
            .notNull()
            .endColumn()
            .column("date")
            .date()
            .endColumn()
            .column("ts")
            .timestamp()
            .endColumn()
            .column("ts_array")
            .type(Type.array(Type.timestamp()))
            .endColumn()
            .primaryKey()
            .asc("id")
            .end()
            .endTable()
            .build();
    Schema schema = logicalTypeConverter.convert(ddl).iterator().next();
    SpannerRecordConverter recordConverter = new SpannerRecordConverter(schema);
    Struct struct =
        Struct.newBuilder()
            .set("id")
            .to(1L)
            .set("date")
            .to(Date.fromYearMonthDay(2018, 2, 2))
            .set("ts")
            .to(Timestamp.ofTimeMicroseconds(10))
            .set("ts_array")
            .toTimestampArray(
                Lists.newArrayList(
                    null,
                    null,
                    Timestamp.ofTimeMicroseconds(10L),
                    Timestamp.ofTimeSecondsAndNanos(10000, 100000),
                    Timestamp.parseTimestamp("1970-01-01T00:00:00Z"),
                    Timestamp.MIN_VALUE,
                    Timestamp.MAX_VALUE))
            .build();

    GenericRecord avroRecord = recordConverter.convert(struct);

    assertThat(avroRecord.get("id"), equalTo(1L));
    assertThat(avroRecord.get("date"), equalTo("2018-02-02"));
    assertThat(avroRecord.get("ts"), equalTo(10L));
    assertThat(
        avroRecord.get("ts_array"),
        equalTo(
            Arrays.asList(
                null, null, 10L, 10000000100L, 0L, -62135596800000000L, 253402300799999999L)));
  }

  @Test
  public void arrays() {
    Ddl ddl =
        Ddl.builder()
            .createTable("users")
            .column("id")
            .int64()
            .notNull()
            .endColumn()
            .column("ints")
            .type(Type.array(Type.int64()))
            .endColumn()
            .column("strings")
            .type(Type.array(Type.string()))
            .max()
            .endColumn()
            .column("ts")
            .type(Type.array(Type.timestamp()))
            .endColumn()
            .column("date")
            .type(Type.array(Type.date()))
            .endColumn()
            .column("bool")
            .type(Type.array(Type.bool()))
            .endColumn()
            .column("float")
            .type(Type.array(Type.float32()))
            .endColumn()
            .column("double")
            .type(Type.array(Type.float64()))
            .endColumn()
            .column("bytes")
            .type(Type.array(Type.bytes()))
            .size(35)
            .endColumn()
            .primaryKey()
            .asc("id")
            .end()
            .endTable()
            .build();
    Schema schema = converter.convert(ddl).iterator().next();
    SpannerRecordConverter recordConverter = new SpannerRecordConverter(schema);
    Struct struct =
        Struct.newBuilder()
            .set("id")
            .to(1L)
            .set("ints")
            .toInt64Array(Lists.newArrayList(1L, null, 2L))
            .set("strings")
            .toStringArray(Lists.newArrayList(null, null, "one"))
            .set("ts")
            .toTimestampArray(Lists.newArrayList(null, null, Timestamp.ofTimeMicroseconds(10L)))
            .set("date")
            .toDateArray(Lists.newArrayList(null, null, Date.fromYearMonthDay(2018, 2, 2)))
            .set("bool")
            .toBoolArray(Lists.newArrayList(true, false, null))
            .set("float")
            .toFloat32Array(Lists.newArrayList(1.0f, 2.1f, 3.3f, null))
            .set("double")
            .toFloat64Array(Lists.newArrayList(1.0, 2.1, 3.3, null))
            .set("bytes")
            .toBytesArray(
                Lists.newArrayList(ByteArray.copyFrom("1234"), ByteArray.copyFrom("5678"), null))
            .build();

    GenericRecord avroRecord = recordConverter.convert(struct);

    assertThat(avroRecord.get("id"), equalTo(1L));
    assertThat(avroRecord.get("ints"), equalTo(Arrays.asList(1L, null, 2L)));
    assertThat(avroRecord.get("strings"), equalTo(Arrays.asList(null, null, "one")));
    assertThat(avroRecord.get("date"), equalTo(Arrays.asList(null, null, "2018-02-02")));
    assertThat(
        avroRecord.get("ts"), equalTo(Arrays.asList(null, null, "1970-01-01T00:00:00.000010000Z")));
    assertThat(avroRecord.get("bool"), equalTo(Arrays.asList(true, false, null)));
    assertThat(avroRecord.get("float"), equalTo(Arrays.asList(1.0f, 2.1f, 3.3f, null)));
    assertThat(avroRecord.get("double"), equalTo(Arrays.asList(1.0, 2.1, 3.3, null)));
    assertEquals(ByteBuffer.wrap("1234".getBytes()), ((List) avroRecord.get("bytes")).get(0));
    assertEquals(ByteBuffer.wrap("5678".getBytes()), ((List) avroRecord.get("bytes")).get(1));
    assertNull(((List) avroRecord.get("bytes")).get(2));
  }

  @Test
  public void numerics() {
    Ddl ddl =
        Ddl.builder()
            .createTable("numerictable")
            .column("id")
            .int64()
            .notNull()
            .endColumn()
            .column("numeric")
            .type(Type.numeric())
            .endColumn()
            .column("numeric_arr")
            .type(Type.array(Type.numeric()))
            .endColumn()
            .primaryKey()
            .asc("id")
            .end()
            .endTable()
            .build();
    Schema schema = converter.convert(ddl).iterator().next();
    SpannerRecordConverter recordConverter = new SpannerRecordConverter(schema);

    String[] numericArrValues = {null, "-25398514232141142.012479", null, "1999999999.1246"};
    Struct struct =
        Struct.newBuilder()
            .set("id")
            .to(1L)
            .set("numeric")
            .to(new BigDecimal(-912348.125))
            .set("numeric_arr")
            .toStringArray(Lists.newArrayList(numericArrValues))
            .build();

    GenericRecord avroRecord = recordConverter.convert(struct);
    List<ByteBuffer> expectedNumericArr =
        Stream.of(numericArrValues)
            .map(x -> x == null ? null : ByteBuffer.wrap(NumericUtils.stringToBytes(x)))
            .collect(Collectors.toList());

    assertThat(avroRecord.get("id"), equalTo(1L));
    assertThat(
        avroRecord.get("numeric"),
        equalTo(ByteBuffer.wrap(NumericUtils.stringToBytes("-912348.125"))));
    assertThat(avroRecord.get("numeric_arr"), equalTo(expectedNumericArr));
  }

  @Test
  public void json() {
    Ddl ddl =
        Ddl.builder()
            .createTable("jsontable")
            .column("id")
            .int64()
            .notNull()
            .endColumn()
            .column("json")
            .type(Type.json())
            .endColumn()
            .column("json_arr")
            .type(Type.array(Type.json()))
            .endColumn()
            .primaryKey()
            .asc("id")
            .end()
            .endTable()
            .build();
    Schema schema = converter.convert(ddl).iterator().next();
    SpannerRecordConverter recordConverter = new SpannerRecordConverter(schema);

    String[] jsonArrValues = {
      null, "[1,null,true,2.2523,\"hello\"]", null, "{\"a\":{\"a\":2.5},\"b\":null}"
    };
    Struct struct =
        Struct.newBuilder()
            .set("id")
            .to(1L)
            .set("json")
            .to("\"hello my friend\"")
            .set("json_arr")
            .toStringArray(Lists.newArrayList(jsonArrValues))
            .build();

    GenericRecord avroRecord = recordConverter.convert(struct);

    assertThat(avroRecord.get("id"), equalTo(1L));
    assertThat(avroRecord.get("json"), equalTo("\"hello my friend\""));
    assertThat(
        avroRecord.get("json_arr"),
        equalTo(
            Arrays.asList(
                null, "[1,null,true,2.2523,\"hello\"]", null, "{\"a\":{\"a\":2.5},\"b\":null}")));
  }

  @Test
  public void pgSimple() {
    Ddl ddl =
        Ddl.builder(Dialect.POSTGRESQL)
            .createTable("users")
            .column("id")
            .pgInt8()
            .notNull()
            .endColumn()
            .column("email")
            .pgVarchar()
            .size(15)
            .notNull()
            .endColumn()
            .column("name")
            .pgVarchar()
            .max()
            .endColumn()
            .primaryKey()
            .asc("id")
            .end()
            .endTable()
            .build();
    Schema schema = converter.convert(ddl).iterator().next();
    SpannerRecordConverter recordConverter = new SpannerRecordConverter(schema, Dialect.POSTGRESQL);
    Struct struct =
        Struct.newBuilder()
            .set("id")
            .to(1L)
            .set("email")
            .to("abc@google.com")
            .set("name")
            .to("John Doe")
            .build();

    GenericRecord avroRecord = recordConverter.convert(struct);

    assertThat(avroRecord.get("id"), equalTo(1L));
    assertThat(avroRecord.get("email"), equalTo("abc@google.com"));
    assertThat(avroRecord.get("name"), equalTo("John Doe"));
  }

  @Test
  public void pgNulls() {
    Ddl ddl =
        Ddl.builder(Dialect.POSTGRESQL)
            .createTable("users")
            .column("id")
            .pgInt8()
            .notNull()
            .endColumn()
            .column("age")
            .pgInt8()
            .endColumn()
            .column("name")
            .pgVarchar()
            .max()
            .endColumn()
            .column("bytes")
            .pgBytea()
            .endColumn()
            .column("ts")
            .pgTimestamptz()
            .endColumn()
            .column("date")
            .pgDate()
            .endColumn()
            .primaryKey()
            .asc("id")
            .end()
            .endTable()
            .build();
    Schema schema = converter.convert(ddl).iterator().next();
    SpannerRecordConverter recordConverter = new SpannerRecordConverter(schema, Dialect.POSTGRESQL);
    Struct struct =
        Struct.newBuilder()
            .set("id")
            .to(1L)
            .set("age")
            .to((Long) null)
            .set("name")
            .to((String) null)
            .set("bytes")
            .to((ByteArray) null)
            .set("ts")
            .to((Timestamp) null)
            .set("date")
            .to((Date) null)
            .build();

    GenericRecord avroRecord = recordConverter.convert(struct);

    assertThat(avroRecord.get("id"), equalTo(1L));
    assertThat(avroRecord.get("age"), is((Long) null));
    assertThat(avroRecord.get("name"), is((String) null));
    assertThat(avroRecord.get("bytes"), is((ByteArray) null));
    assertThat(avroRecord.get("ts"), is((String) null));
    assertThat(avroRecord.get("date"), is((String) null));
  }

  @Test
  public void pgDateTimestamptz() {
    Ddl ddl =
        Ddl.builder(Dialect.POSTGRESQL)
            .createTable("users")
            .column("id")
            .pgInt8()
            .notNull()
            .endColumn()
            .column("ts")
            .pgTimestamptz()
            .endColumn()
            .column("date")
            .pgDate()
            .endColumn()
            .primaryKey()
            .asc("id")
            .end()
            .endTable()
            .build();
    Schema schema = converter.convert(ddl).iterator().next();
    SpannerRecordConverter recordConverter = new SpannerRecordConverter(schema, Dialect.POSTGRESQL);
    Struct struct =
        Struct.newBuilder()
            .set("id")
            .to(1L)
            .set("ts")
            .to(Timestamp.ofTimeMicroseconds(10))
            .set("date")
            .to(Date.fromYearMonthDay(2018, 2, 2))
            .build();

    GenericRecord avroRecord = recordConverter.convert(struct);

    assertThat(avroRecord.get("id"), equalTo(1L));
    assertThat(avroRecord.get("ts"), equalTo("1970-01-01T00:00:00.000010000Z"));
    assertThat(avroRecord.get("date"), equalTo("2018-02-02"));
  }

  @Test
  public void pgTimestamptzLogical() {
    Ddl ddl =
        Ddl.builder(Dialect.POSTGRESQL)
            .createTable("users")
            .column("id")
            .pgInt8()
            .notNull()
            .endColumn()
            .column("ts1")
            .pgTimestamptz()
            .endColumn()
            .column("ts2")
            .pgTimestamptz()
            .endColumn()
            .column("ts3")
            .pgTimestamptz()
            .endColumn()
            .column("ts4")
            .pgTimestamptz()
            .endColumn()
            .column("ts5")
            .pgTimestamptz()
            .endColumn()
            .primaryKey()
            .asc("id")
            .end()
            .endTable()
            .build();
    Schema schema = logicalTypeConverter.convert(ddl).iterator().next();
    SpannerRecordConverter recordConverter = new SpannerRecordConverter(schema, Dialect.POSTGRESQL);
    Struct struct =
        Struct.newBuilder()
            .set("id")
            .to(1L)
            .set("ts1")
            .to(Timestamp.ofTimeMicroseconds(10))
            .set("ts2")
            .to(Timestamp.ofTimeSecondsAndNanos(10000, 100000))
            .set("ts3")
            .to(Timestamp.parseTimestamp("1970-01-01T00:00:00Z"))
            .set("ts4")
            .to(Timestamp.MIN_VALUE)
            .set("ts5")
            .to(Timestamp.MAX_VALUE)
            .build();

    GenericRecord avroRecord = recordConverter.convert(struct);

    assertThat(avroRecord.get("id"), equalTo(1L));
    assertThat(avroRecord.get("ts1"), equalTo(10L));
    assertThat(avroRecord.get("ts2"), equalTo(10000000100L));
    assertThat(avroRecord.get("ts3"), equalTo(0L));
    assertThat(avroRecord.get("ts4"), equalTo(-62135596800000000L));
    assertThat(avroRecord.get("ts5"), equalTo(253402300799999999L));
  }

  @Test
  public void pgCommitTimestamp() {
    Ddl ddl =
        Ddl.builder(Dialect.POSTGRESQL)
            .createTable("users")
            .column("id")
            .pgInt8()
            .notNull()
            .endColumn()
            .column("ts1")
            .pgSpannerCommitTimestamp()
            .endColumn()
            .column("ts2")
            .pgSpannerCommitTimestamp()
            .endColumn()
            .column("ts3")
            .pgSpannerCommitTimestamp()
            .endColumn()
            .column("ts4")
            .pgSpannerCommitTimestamp()
            .endColumn()
            .column("ts5")
            .pgSpannerCommitTimestamp()
            .endColumn()
            .primaryKey()
            .asc("id")
            .end()
            .endTable()
            .build();

    Struct struct =
        Struct.newBuilder()
            .set("id")
            .to(1L)
            .set("ts1")
            .to(Timestamp.ofTimeMicroseconds(1))
            .set("ts2")
            .to(Timestamp.ofTimeSecondsAndNanos(10, 1000))
            .set("ts3")
            .to(Timestamp.parseTimestamp("1970-01-01T00:00:00Z"))
            .set("ts4")
            .to(Timestamp.MIN_VALUE)
            .set("ts5")
            .to(Timestamp.MAX_VALUE)
            .build();

    // By default, export timestamps as strings.
    Schema schema = converter.convert(ddl).iterator().next();
    SpannerRecordConverter recordConverter = new SpannerRecordConverter(schema, Dialect.POSTGRESQL);
    GenericRecord avroRecord = recordConverter.convert(struct);
    assertThat(avroRecord.get("id"), equalTo(1L));
    assertThat(avroRecord.get("ts1"), equalTo("1970-01-01T00:00:00.000001000Z"));
    assertThat(avroRecord.get("ts2"), equalTo("1970-01-01T00:00:10.000001000Z"));
    assertThat(avroRecord.get("ts3"), equalTo("1970-01-01T00:00:00Z"));
    assertThat(avroRecord.get("ts4"), equalTo("0001-01-01T00:00:00Z"));
    assertThat(avroRecord.get("ts5"), equalTo("9999-12-31T23:59:59.999999999Z"));

    // Export timestamps as timestamp-micros logical type.
    Schema schemaLogicalType = logicalTypeConverter.convert(ddl).iterator().next();
    SpannerRecordConverter recordConverterLogicalType =
        new SpannerRecordConverter(schemaLogicalType, Dialect.POSTGRESQL);
    GenericRecord avroRecordLogicalType = recordConverterLogicalType.convert(struct);
    assertThat(avroRecordLogicalType.get("id"), equalTo(1L));
    assertThat(avroRecordLogicalType.get("ts1"), equalTo(1L));
    assertThat(avroRecordLogicalType.get("ts2"), equalTo(10000001L));
    assertThat(avroRecordLogicalType.get("ts3"), equalTo(0L));
    assertThat(avroRecordLogicalType.get("ts4"), equalTo(-62135596800000000L));
    assertThat(avroRecordLogicalType.get("ts5"), equalTo(253402300799999999L));
  }

  @Test
  public void pgNumerics() {
    Ddl ddl =
        Ddl.builder(Dialect.POSTGRESQL)
            .createTable("numerictable")
            .column("id")
            .pgInt8()
            .notNull()
            .endColumn()
            .column("numeric1")
            .type(Type.pgNumeric())
            .endColumn()
            .column("numeric2")
            .type(Type.pgNumeric())
            .endColumn()
            .column("numeric3")
            .type(Type.pgNumeric())
            .endColumn()
            .column("numeric_arr")
            .type(Type.pgArray(Type.pgNumeric()))
            .endColumn()
            .primaryKey()
            .asc("id")
            .end()
            .endTable()
            .build();
    Schema schema = converter.convert(ddl).iterator().next();
    SpannerRecordConverter recordConverter = new SpannerRecordConverter(schema, Dialect.POSTGRESQL);

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

    String[] pgNumericArrValues = {
      null, "NaN", null, maxPgNumeric.toString(), minPgNumeric.toString()
    };

    Struct struct =
        Struct.newBuilder()
            .set("id")
            .to(1L)
            .set("numeric1")
            .to("-9305028.140032")
            .set("numeric2")
            .to("-25398514232141142.012479")
            .set("numeric3")
            .to("1999999999.1246")
            .set("numeric_arr")
            .toStringArray(Lists.newArrayList(pgNumericArrValues))
            .build();

    GenericRecord avroRecord = recordConverter.convert(struct);
    List<ByteBuffer> expectedPgNumericArr =
        Stream.of(pgNumericArrValues)
            .map(x -> x == null ? null : ByteBuffer.wrap(NumericUtils.pgStringToBytes(x)))
            .collect(Collectors.toList());

    assertThat(avroRecord.get("id"), equalTo(1L));
    assertThat(
        avroRecord.get("numeric1"),
        equalTo(ByteBuffer.wrap(NumericUtils.pgStringToBytes("-9305028.140032"))));
    assertThat(
        avroRecord.get("numeric2"),
        equalTo(ByteBuffer.wrap(NumericUtils.pgStringToBytes("-25398514232141142.012479"))));
    assertThat(
        avroRecord.get("numeric3"),
        equalTo(ByteBuffer.wrap(NumericUtils.pgStringToBytes("1999999999.1246"))));
    assertThat(avroRecord.get("numeric_arr"), equalTo(expectedPgNumericArr));
  }

  @Test
  public void pgJsonbs() {
    Ddl ddl =
        Ddl.builder(Dialect.POSTGRESQL)
            .createTable("jsonbtable")
            .column("id")
            .int64()
            .notNull()
            .endColumn()
            .column("jsonb")
            .type(Type.pgJsonb())
            .endColumn()
            .column("jsonb_arr")
            .type(Type.pgArray(Type.pgJsonb()))
            .endColumn()
            .primaryKey()
            .asc("id")
            .end()
            .endTable()
            .build();
    Schema schema = converter.convert(ddl).iterator().next();
    SpannerRecordConverter recordConverter = new SpannerRecordConverter(schema, Dialect.POSTGRESQL);
    String[] jsonbArrayValues = {
      null, "[1,null,true,2.2523,\"hello\"]", null, "{\"a\":{\"a\":2.5},\"b\":null}"
    };

    Struct struct =
        Struct.newBuilder()
            .set("id")
            .to(1L)
            .set("jsonb")
            .to(Value.pgJsonb("{\"a\": true, \"b\": 2}"))
            .set("jsonb_arr")
            .to(Value.pgJsonbArray(Lists.newArrayList(jsonbArrayValues)))
            .build();

    GenericRecord avroRecord = recordConverter.convert(struct);

    assertThat(avroRecord.get("id"), equalTo(1L));
    assertThat(avroRecord.get("jsonb"), equalTo("{\"a\": true, \"b\": 2}"));
  }

  @Test
  public void pgArrays() {
    Ddl ddl =
        Ddl.builder(Dialect.POSTGRESQL)
            .createTable("users")
            .column("id")
            .pgInt8()
            .notNull()
            .endColumn()
            .column("ints")
            .type(Type.pgArray(Type.pgInt8()))
            .endColumn()
            .column("varchars")
            .type(Type.pgArray(Type.pgVarchar()))
            .max()
            .endColumn()
            .column("texts")
            .type(Type.pgArray(Type.pgText()))
            .endColumn()
            .column("ts")
            .type(Type.pgArray(Type.pgTimestamptz()))
            .endColumn()
            .column("date")
            .type(Type.pgArray(Type.pgDate()))
            .endColumn()
            .primaryKey()
            .asc("id")
            .end()
            .endTable()
            .build();
    Schema schema = converter.convert(ddl).iterator().next();
    SpannerRecordConverter recordConverter = new SpannerRecordConverter(schema, Dialect.POSTGRESQL);
    Struct struct =
        Struct.newBuilder()
            .set("id")
            .to(1L)
            .set("ints")
            .toInt64Array(Lists.newArrayList(1L, null, 2L))
            .set("varchars")
            .toStringArray(Lists.newArrayList(null, null, "one"))
            .set("texts")
            .toStringArray(Lists.newArrayList(null, null, "two"))
            .set("ts")
            .toTimestampArray(Lists.newArrayList(null, null, Timestamp.ofTimeMicroseconds(10L)))
            .set("date")
            .toDateArray(Lists.newArrayList(null, null, Date.fromYearMonthDay(2018, 2, 2)))
            .build();

    GenericRecord avroRecord = recordConverter.convert(struct);

    assertThat(avroRecord.get("id"), equalTo(1L));
    assertThat(avroRecord.get("ints"), equalTo(Arrays.asList(1L, null, 2L)));
    assertThat(avroRecord.get("varchars"), equalTo(Arrays.asList(null, null, "one")));
    assertThat(avroRecord.get("texts"), equalTo(Arrays.asList(null, null, "two")));
    assertThat(avroRecord.get("date"), equalTo(Arrays.asList(null, null, "2018-02-02")));
    assertThat(
        avroRecord.get("ts"), equalTo(Arrays.asList(null, null, "1970-01-01T00:00:00.000010000Z")));
  }

  @Test
  public void generatedColumn() {
    Ddl ddl =
        Ddl.builder()
            .createTable("users")
            .column("id")
            .int64()
            .notNull()
            .endColumn()
            .column("generatedInt")
            .int64()
            .size(15)
            .notNull()
            .generatedAs("2 + 5")
            .endColumn()
            .primaryKey()
            .asc("id")
            .end()
            .endTable()
            .build();
    Schema schema = converter.convert(ddl).iterator().next();
    SpannerRecordConverter recordConverter = new SpannerRecordConverter(schema);
    Struct struct = Struct.newBuilder().set("id").to(1L).set("generatedInt").to(7L).build();

    GenericRecord avroRecord = recordConverter.convert(struct);

    assertEquals(1L, avroRecord.get("id"));
    assertNull(avroRecord.get("generatedInt"));
  }

  @Test
  public void dateToString() {
    List<Date> dates =
        Arrays.asList(
            Date.fromYearMonthDay(10, 9, 5),
            Date.fromYearMonthDay(500, 3, 4),
            Date.fromYearMonthDay(2016, 12, 31),
            Date.fromYearMonthDay(1, 1, 1));

    for (Date date : dates) {
      assertEquals(SpannerRecordConverter.dateToString(date), date.toString());
    }
  }

  @Test
  public void proto() {
    Ddl ddl =
        Ddl.builder()
            .createTable("prototable")
            .column("id")
            .int64()
            .notNull()
            .endColumn()
            .column("proto")
            .type(Type.proto("com.google.cloud.teleport.spanner.tests.TestMessage"))
            .endColumn()
            .column("proto_arr")
            .type(Type.array(Type.proto("com.google.cloud.teleport.spanner.tests.TestEnum")))
            .endColumn()
            .column("enum")
            .type(Type.protoEnum("com.google.cloud.teleport.spanner.tests.TestEnum"))
            .endColumn()
            .column("enum_arr")
            .type(Type.array(Type.protoEnum("com.google.cloud.teleport.spanner.tests.TestEnum")))
            .endColumn()
            .primaryKey()
            .asc("id")
            .end()
            .endTable()
            .build();
    Schema schema = converter.convert(ddl).iterator().next();
    SpannerRecordConverter recordConverter = new SpannerRecordConverter(schema);
    com.google.cloud.teleport.spanner.tests.TestMessage msg1 =
        com.google.cloud.teleport.spanner.tests.TestMessage.newBuilder().setValue("A").build();
    com.google.cloud.teleport.spanner.tests.TestMessage msg2 =
        com.google.cloud.teleport.spanner.tests.TestMessage.newBuilder().setValue("B").build();

    ByteArray[] protoArrValues = {
      null, ByteArray.copyFrom(msg1.toByteArray()), null, ByteArray.copyFrom(msg2.toByteArray())
    };

    com.google.cloud.teleport.spanner.tests.TestEnum enum1 =
        com.google.cloud.teleport.spanner.tests.TestEnum.VALUE1;
    com.google.cloud.teleport.spanner.tests.TestEnum enum2 =
        com.google.cloud.teleport.spanner.tests.TestEnum.VALUE2;
    Struct struct =
        Struct.newBuilder()
            .set("id")
            .to(1L)
            .set("proto")
            .to((ByteArray) ByteArray.copyFrom(msg1.toByteArray()))
            .set("proto_arr")
            .toBytesArray(Lists.newArrayList(protoArrValues))
            .set("enum")
            .to(enum1.getNumber())
            .set("enum_arr")
            .toInt64Array(
                Lists.newArrayList(new Long(enum1.getNumber()), null, new Long(enum2.getNumber())))
            .build();

    GenericRecord avroRecord = recordConverter.convert(struct);
    assertThat(avroRecord.get("id"), equalTo(1L));
    assertEquals(avroRecord.get("proto"), ByteBuffer.wrap(msg1.toByteArray()));
    assertEquals(ByteBuffer.wrap(msg1.toByteArray()), ((List) avroRecord.get("proto_arr")).get(1));
    assertEquals(ByteBuffer.wrap(msg2.toByteArray()), ((List) avroRecord.get("proto_arr")).get(3));
    assertThat(((List) avroRecord.get("proto_arr")).get(0), is((ByteArray) null));
    assertThat(((List) avroRecord.get("proto_arr")).get(2), is((ByteArray) null));
    assertThat(avroRecord.get("enum"), equalTo(new Long(enum1.getNumber())));
    assertThat(
        avroRecord.get("enum_arr"),
        equalTo(Arrays.asList(new Long(enum1.getNumber()), null, new Long(enum2.getNumber()))));
  }
}
