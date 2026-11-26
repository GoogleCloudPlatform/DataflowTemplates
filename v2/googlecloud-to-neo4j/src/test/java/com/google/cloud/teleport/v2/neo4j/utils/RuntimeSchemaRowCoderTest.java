/*
 * Copyright (C) 2026 Google LLC
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
package com.google.cloud.teleport.v2.neo4j.utils;

import static com.google.common.truth.Truth.assertThat;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaTranslation;
import org.apache.beam.sdk.schemas.logicaltypes.SqlTypes;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.values.Row;
import org.junit.Before;
import org.junit.Test;

public class RuntimeSchemaRowCoderTest {

  @Before
  public void resetCoderCaches() {
    RuntimeSchemaRowCoder.resetCaches();
  }

  @Test
  public void roundTripsRowsWithTheirSchema() throws Exception {
    var schema =
        Schema.builder()
            .addStringField("name")
            .addInt64Field("count")
            .addLogicalTypeField("date", SqlTypes.DATE)
            .addLogicalTypeField("time", SqlTypes.TIME)
            .addLogicalTypeField("timestamp", SqlTypes.DATETIME)
            .addArrayField("tags", Schema.FieldType.STRING)
            .build();
    var row =
        Row.withSchema(schema)
            .addValues(
                "a-name",
                42L,
                LocalDate.of(2026, 11, 25),
                LocalTime.of(16, 30),
                LocalDateTime.of(2026, 11, 25, 16, 30),
                java.util.List.of("one", "two"))
            .build();
    var out = new ByteArrayOutputStream();

    RuntimeSchemaRowCoder.INSTANCE.encode(row, out);
    var decoded =
        RuntimeSchemaRowCoder.INSTANCE.decode(new ByteArrayInputStream(out.toByteArray()));

    assertThat(decoded.getSchema()).isEqualTo(schema);
    assertThat(decoded).isEqualTo(row);
  }

  @Test
  public void roundTripsBuiltInDatetime() throws Exception {
    var schema = Schema.builder().addDateTimeField("timestamp").build();
    var row =
        Row.withSchema(schema)
            .addValue(org.joda.time.Instant.ofEpochMilli(1_700_000_000_000L))
            .build();
    var out = new ByteArrayOutputStream();

    RuntimeSchemaRowCoder.INSTANCE.encode(row, out);
    var decoded =
        RuntimeSchemaRowCoder.INSTANCE.decode(new ByteArrayInputStream(out.toByteArray()));

    assertThat(decoded.getSchema()).isEqualTo(schema);
    assertThat(decoded).isEqualTo(row);
  }

  @Test
  public void roundTripsNestedRows() throws Exception {
    var nested = Schema.builder().addStringField("inner").build();
    var schema = Schema.builder().addRowField("nested", nested).build();
    var row =
        Row.withSchema(schema).addValue(Row.withSchema(nested).addValue("value").build()).build();
    var out = new ByteArrayOutputStream();

    RuntimeSchemaRowCoder.INSTANCE.encode(row, out);
    var decoded =
        RuntimeSchemaRowCoder.INSTANCE.decode(new ByteArrayInputStream(out.toByteArray()));

    assertThat(decoded).isEqualTo(row);
  }

  @Test
  public void roundTripsMoreSchemasThanTheCacheHolds() throws Exception {
    // Deliberately exceeds the cache bound: correctness must not depend on the cached state.
    var rows = new ArrayList<Row>();
    var encodedRows = new ArrayList<byte[]>();
    for (int i = 0; i < RuntimeSchemaRowCoder.MAX_CACHED_SCHEMAS + 2; i++) {
      var schema = Schema.builder().addStringField("field_" + i).build();
      var row = Row.withSchema(schema).addValue("value-" + i).build();
      try (var out = new ByteArrayOutputStream()) {
        RuntimeSchemaRowCoder.INSTANCE.encode(row, out);
        rows.add(row);
        encodedRows.add(out.toByteArray());
      }
    }

    for (int i = 0; i < rows.size(); i++) {
      var encoded = encodedRows.get(i);
      try (var input = new ByteArrayInputStream(encoded)) {
        var decoded = RuntimeSchemaRowCoder.INSTANCE.decode(input);
        assertThat(decoded).isEqualTo(rows.get(i));
      }
    }
  }

  @Test
  public void cachesStayBounded() throws Exception {
    var encodedRows = new ArrayList<byte[]>();
    for (int i = 0; i < RuntimeSchemaRowCoder.MAX_CACHED_SCHEMAS * 3; i++) {
      var schema = Schema.builder().addStringField("field_" + i).build();
      var row = Row.withSchema(schema).addValue("value-" + i).build();
      try (var out = new ByteArrayOutputStream()) {
        RuntimeSchemaRowCoder.INSTANCE.encode(row, out);
        encodedRows.add(out.toByteArray());
      }
    }
    assertThat(RuntimeSchemaRowCoder.cachedEncoderCount())
        .isAtMost(RuntimeSchemaRowCoder.MAX_CACHED_SCHEMAS);

    for (var encoded : encodedRows) {
      RuntimeSchemaRowCoder.INSTANCE.decode(new ByteArrayInputStream(encoded));
    }

    assertThat(RuntimeSchemaRowCoder.cachedDecoderCount())
        .isAtMost(RuntimeSchemaRowCoder.MAX_CACHED_SCHEMAS);
    assertThat(RuntimeSchemaRowCoder.cachedEncoderCount())
        .isAtMost(RuntimeSchemaRowCoder.MAX_CACHED_SCHEMAS);
  }

  @Test
  public void roundTripsConcurrently() throws Exception {
    var rows = new ArrayList<Row>();
    var encodedRows = new ArrayList<byte[]>();
    for (int i = 0; i < 8; i++) {
      var schema = Schema.builder().addStringField("field_" + i).addInt64Field("count").build();
      var row = Row.withSchema(schema).addValues("value-" + i, (long) i).build();
      try (var out = new ByteArrayOutputStream()) {
        RuntimeSchemaRowCoder.INSTANCE.encode(row, out);
        encodedRows.add(out.toByteArray());
      }
      rows.add(row);
    }

    int threads = 8;
    var executor = Executors.newFixedThreadPool(threads);
    try {
      var start = new CountDownLatch(1);
      var futures = new ArrayList<Future<?>>();
      for (int t = 0; t < threads; t++) {
        int offset = t;
        futures.add(
            executor.submit(
                () -> {
                  start.await();
                  for (int i = 0; i < 1_000; i++) {
                    int index = (offset + i) % rows.size();
                    var decoded =
                        RuntimeSchemaRowCoder.INSTANCE.decode(
                            new ByteArrayInputStream(encodedRows.get(index)));
                    assertThat(decoded).isEqualTo(rows.get(index));
                    try (var out = new ByteArrayOutputStream()) {
                      RuntimeSchemaRowCoder.INSTANCE.encode(rows.get(index), out);
                      assertThat(out.toByteArray()).isEqualTo(encodedRows.get(index));
                    }
                  }
                  return null;
                }));
      }
      start.countDown();
      for (var future : futures) {
        future.get(60, TimeUnit.SECONDS);
      }
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void decodesTheSamePayloadRepeatedly() throws Exception {
    var schema = Schema.builder().addStringField("name").build();
    var row = Row.withSchema(schema).addValue("value").build();
    byte[] payload;
    try (var out = new ByteArrayOutputStream()) {
      RuntimeSchemaRowCoder.INSTANCE.encode(row, out);
      payload = out.toByteArray();
    }

    var first = RuntimeSchemaRowCoder.INSTANCE.decode(new ByteArrayInputStream(payload));
    var second = RuntimeSchemaRowCoder.INSTANCE.decode(new ByteArrayInputStream(payload));

    assertThat(first).isEqualTo(row);
    assertThat(second).isEqualTo(row);
  }

  @Test
  public void structuralValueReflectsRowContent() {
    var firstSchema = Schema.builder().addStringField("name").build();
    var secondSchema = Schema.builder().addStringField("name").build();
    var first = Row.withSchema(firstSchema).addValue("same").build();
    var second = Row.withSchema(secondSchema).addValue("same").build();
    var different = Row.withSchema(firstSchema).addValue("other").build();

    assertThat(RuntimeSchemaRowCoder.INSTANCE.structuralValue(first))
        .isEqualTo(RuntimeSchemaRowCoder.INSTANCE.structuralValue(second));
    assertThat(RuntimeSchemaRowCoder.INSTANCE.structuralValue(first))
        .isNotEqualTo(RuntimeSchemaRowCoder.INSTANCE.structuralValue(different));
  }

  @Test
  public void schemaProtoIsDeterministicAcrossSchemaInstances() {
    var first = Schema.builder().addStringField("name").build();
    var second = Schema.builder().addStringField("name").build();

    assertThat(first).isEqualTo(second);
    assertThat(first.getUUID()).isNotEqualTo(second.getUUID());

    // The coder relies on the 3-argument overload: including the random UUID would make the
    // encoded schema bytes differ for equal schemas.
    assertThat(SchemaTranslation.schemaToProto(first, true, false).toByteArray())
        .isEqualTo(SchemaTranslation.schemaToProto(second, true, false).toByteArray());
    // Java serialization, which the coder used to rely on, does embed the UUID.
    assertThat(SerializableUtils.serializeToByteArray(first))
        .isNotEqualTo(SerializableUtils.serializeToByteArray(second));
  }
}
