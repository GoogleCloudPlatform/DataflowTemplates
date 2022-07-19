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
import com.google.cloud.teleport.spanner.common.NumericUtils;
import com.google.cloud.teleport.spanner.common.Type;
import com.google.cloud.teleport.spanner.ddl.Ddl;
import com.google.common.collect.Lists;
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
            .set("double")
            .to(30.2)
            .build();

    GenericRecord avroRecord = recordConverter.convert(struct);

    assertThat(avroRecord.get("id"), equalTo(1L));
    assertThat(avroRecord.get("email"), equalTo("abc@google.com"));
    assertThat(avroRecord.get("name"), equalTo("John Doe"));
    assertEquals(true, avroRecord.get("bool"));
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
            .to("-9305028.140032")
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
        equalTo(ByteBuffer.wrap(NumericUtils.stringToBytes("-9305028.140032"))));
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
}
