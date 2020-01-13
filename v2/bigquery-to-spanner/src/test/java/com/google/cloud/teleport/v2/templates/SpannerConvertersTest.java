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
package com.google.cloud.teleport.v2.templates;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Mutation.Op;
import com.google.cloud.spanner.Value;
import com.google.cloud.teleport.v2.templates.SpannerConverters.RowToMutation.RowToMutationDoFn;
import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import java.nio.ByteBuffer;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.joda.time.DateTime;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Unit tests for {@link BigQueryToSpanner}.
 */
@RunWith(JUnit4.class)
public class SpannerConvertersTest {

  @Rule
  public final transient TestPipeline pipeline = TestPipeline.create();
  private String table = "TABLE";

  @Test
  @Category(NeedsRunner.class)
  public void testRowToMutationTransform_emptySchemaAndRow() {
    Schema schema = Schema.builder().build();
    Row row = Row.withSchema(schema).build();
    PCollection<Row> rows = pipeline.apply(Create.of(row).withRowSchema(schema));
    Mutation expectedMutation = Mutation.newInsertOrUpdateBuilder(table).build();
    PCollection<Mutation> mutations = rows
        .apply(SpannerConverters.toMutation(table, Op.INSERT_OR_UPDATE));
    PAssert.that(mutations).containsInAnyOrder(expectedMutation);
    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testRowToMutationTransform_arrayField() {

    Schema schema = Schema.builder()
        .addField("int16", FieldType.INT16)
        .addField("int32", FieldType.INT32)
        .addField("int64", FieldType.INT64)
        .addField("boolean", FieldType.BOOLEAN)
        .addField("bytes", FieldType.BYTES)
        .addField("datetime", FieldType.DATETIME)
        .addField("double", FieldType.DOUBLE)
        .addField("float", FieldType.FLOAT)
        .addField("string", FieldType.STRING)
        .addField("int16Array", FieldType.array(FieldType.INT16))
        .addField("int32Array", FieldType.array(FieldType.INT32))
        .addField("int64Array", FieldType.array(FieldType.INT64))
        .addField("booleanArray", FieldType.array(FieldType.BOOLEAN))
        .addField("bytesArray", FieldType.array(FieldType.BYTES))
        .addField("datetimeArray", FieldType.array(FieldType.DATETIME))
        .addField("doubleArray", FieldType.array(FieldType.DOUBLE))
        .addField("floatArray", FieldType.array(FieldType.FLOAT))
        .addField("stringArray", FieldType.array(FieldType.STRING))
        .addField("nullable", FieldType.STRING.withNullable(true))
        .addField("nullableArray", FieldType.array(FieldType.STRING).withNullable(true))
        .addField("date", SpannerConverters.SQL_DATE_TYPE)
        .build();

    Row row = Row.withSchema(schema)
        .addValue((short) 1)
        .addValue(1)
        .addValue(1L)
        .addValue(true)
        .addValue("hello".getBytes(Charsets.UTF_8))
        .addValue(DateTime.parse("2019-01-01T20:19:32Z"))
        .addValue(1.0d)
        .addValue(1.0f)
        .addValue("hello")
        .addArray((short) 1, (short) 2, (short) 3, (short) 4)
        .addArray(1, 2, 3, 4)
        .addArray(1L, 2L, 3L, 4L)
        .addArray(true, false, true)
        .addArray(ByteBuffer.wrap("hello".getBytes(Charsets.UTF_8)),
            ByteBuffer.wrap("world".getBytes(Charsets.UTF_8)))
        .addArray(DateTime.parse("2019-01-01T20:19:32Z"), DateTime.parse("2019-01-01T20:19:33Z"))
        .addArray(1.0d, 2.0d, 3.0d)
        .addArray(1.0f, 2.0f, 3.0f)
        .addArray("hello", "world")
        .addValue(null) // should not be set on mutation object
        .addValue(null) // should not be set on mutation object
        .addValue(DateTime.parse("2019-01-01T20:19:32Z")) // Date
        .build();
    PCollection<Row> rows = pipeline.apply(Create.of(row).withRowSchema(schema));
    Mutation expectedMutation = Mutation.newInsertOrUpdateBuilder(table)
        .set("int16").to(1L)
        .set("int32").to(1L)
        .set("int64").to(1L)
        .set("boolean").to(true)
        .set("bytes").to(ByteArray.copyFrom("hello"))
        .set("datetime").to(Timestamp.parseTimestamp("2019-01-01T20:19:32Z"))
        .set("double").to(1.0d)
        .set("float").to(1.0d)
        .set("string").to("hello")
        .set("int16Array").toInt64Array(new long[]{1L, 2L, 3L, 4L})
        .set("int32Array").toInt64Array(new long[]{1L, 2L, 3L, 4L})
        .set("int64Array").toInt64Array(new long[]{1L, 2L, 3L, 4L})
        .set("booleanArray").toBoolArray(new boolean[]{true, false, true})
        .set("bytesArray")
        .toBytesArray(Lists.newArrayList(ByteArray.copyFrom("hello"), ByteArray.copyFrom("world")))
        .set("datetimeArray").toTimestampArray(Lists
            .newArrayList(Timestamp.parseTimestamp("2019-01-01T20:19:32Z"),
                Timestamp.parseTimestamp("2019-01-01T20:19:33Z")))
        .set("doubleArray").toFloat64Array(new double[]{1.0d, 2.0d, 3.0d})
        .set("floatArray").toFloat64Array(new double[]{1.0d, 2.0d, 3.0d})
        .set("stringArray").toStringArray(Lists.newArrayList("hello", "world"))
        .set("date").to(com.google.cloud.Date.parseDate("2019-01-01"))
        .build();
    PCollection<Mutation> mutations = rows
        .apply(SpannerConverters.toMutation(table, Op.INSERT_OR_UPDATE));
    PAssert.that(mutations).containsInAnyOrder(expectedMutation);
    pipeline.run();
  }

  @Test
  public void testGetValueGetter_String() {
    SerializableFunction<Object, Value> getter = new RowToMutationDoFn(null, null)
        .getValueGetter(FieldType.STRING);
    assertEquals("hello", getter.apply("hello").getString());
  }

  @Test
  public void testGetValueGetter_Int64() {
    SerializableFunction<Object, Value> getter = new RowToMutationDoFn(null, null)
        .getValueGetter(FieldType.INT64);
    assertEquals(1L, getter.apply(1L).getInt64());
  }

  @Test
  public void testGetValueGetter_Int32() {
    SerializableFunction<Object, Value> getter = new RowToMutationDoFn(null, null)
        .getValueGetter(FieldType.INT32);
    assertEquals(1L, getter.apply(1).getInt64());
  }

  @Test
  public void testGetValueGetter_Int16() {
    SerializableFunction<Object, Value> getter = new RowToMutationDoFn(null, null)
        .getValueGetter(FieldType.INT16);
    assertEquals(1L, getter.apply((short) 1).getInt64());
  }


  @Test
  public void testGetValueGetter_Double() {
    SerializableFunction<Object, Value> getter = new RowToMutationDoFn(null, null)
        .getValueGetter(FieldType.DOUBLE);
    assertEquals(1.0d, getter.apply(1.0d).getFloat64(), 0.0);
  }

  @Test
  public void testGetValueGetter_Float() {
    SerializableFunction<Object, Value> getter = new RowToMutationDoFn(null, null)
        .getValueGetter(FieldType.FLOAT);
    assertEquals(1.0d, getter.apply(1.0f).getFloat64(), 0.0);
  }

  @Test
  public void testGetValueGetter_Boolean() {
    SerializableFunction<Object, Value> getter = new RowToMutationDoFn(null, null)
        .getValueGetter(FieldType.BOOLEAN);
    assertTrue(getter.apply(true).getBool());
    assertFalse(getter.apply(false).getBool());
  }

  @Test
  public void testGetValueGetter_Bytes() {
    SerializableFunction<Object, Value> getter = new RowToMutationDoFn(null, null)
        .getValueGetter(FieldType.BYTES);
    assertEquals(ByteArray.copyFrom("Hello World".getBytes()),
        getter.apply("Hello World".getBytes()).getBytes());
  }

  @Test
  public void testGetValueGetter_StringArray() {
    SerializableFunction<Object, Value> getter = new RowToMutationDoFn(null, null)
        .getValueGetter(FieldType.array(FieldType.STRING));
    assertThat(Lists.newArrayList("Hello", "World"),
        is(getter.apply(Lists.newArrayList("Hello", "World")).getStringArray()));
  }

  @Test
  public void testGetValueGetter_StringNullable() {
    SerializableFunction<Object, Value> getter = new RowToMutationDoFn(null, null)
        .getValueGetter(FieldType.STRING.withNullable(true));
    assertEquals("hello", getter.apply("hello").getString());
  }

  @Test
  public void testGetValueGetter_TimestampWithTZ() {
    SerializableFunction<Object, Value> getter = new RowToMutationDoFn(null, null)
        .getValueGetter(FieldType.DATETIME);
    Timestamp ts1 = getter.apply(DateTime.parse("2019-01-01T20:19:32Z")).getTimestamp();
    assertEquals("2019-01-01T20:19:32Z", ts1.toString());
    Timestamp ts2 = getter.apply(DateTime.parse("2019-01-01T20:19:32+10")).getTimestamp();
    assertEquals("2019-01-01T10:19:32Z", ts2.toString());
    Timestamp ts3 = getter.apply(DateTime.parse("2019-01-01T20:19:32-10")).getTimestamp();
    assertEquals("2019-01-02T06:19:32Z", ts3.toString());
  }

  @Test
  public void testGetValueGetter_LogicalLocalDate() {
    SerializableFunction<Object, Value> getter = new RowToMutationDoFn(null, null)
        .getValueGetter(SpannerConverters.SQL_DATE_TYPE);
    Date date1 = getter.apply(DateTime.parse("2019-01-01T20:19:32+08:00")).getDate();
    assertEquals(2019, date1.getYear());
    assertEquals(1, date1.getMonth());
    assertEquals(1, date1.getDayOfMonth());

    Date date2 = getter.apply(DateTime.parse("2019-01-01T20:19:32Z")).getDate();
    assertEquals(2019, date2.getYear());
    assertEquals(1, date2.getMonth());
    assertEquals(1, date2.getDayOfMonth());
  }

  @Test
  public void testNewMutationBuilder() {
    assertEquals(RowToMutationDoFn.newMutationBuilder(table, Op.INSERT).build().getOperation(),
        Op.INSERT);
    assertEquals(RowToMutationDoFn.newMutationBuilder(table, Op.UPDATE).build().getOperation(),
        Op.UPDATE);
    assertEquals(
        RowToMutationDoFn.newMutationBuilder(table, Op.INSERT_OR_UPDATE).build().getOperation(),
        Op.INSERT_OR_UPDATE);
    assertEquals(RowToMutationDoFn.newMutationBuilder(table, Op.REPLACE).build().getOperation(),
        Op.REPLACE);
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testNewMutationBuilderDeleteNotSupported() {
    RowToMutationDoFn.newMutationBuilder(table, Op.DELETE);
  }
}
