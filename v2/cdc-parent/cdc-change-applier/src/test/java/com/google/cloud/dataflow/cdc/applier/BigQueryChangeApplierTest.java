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
package com.google.cloud.dataflow.cdc.applier;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.joda.time.DateTime;
import org.joda.time.Instant;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Tests for BigQueryChangeApplier. */
public class BigQueryChangeApplierTest {

  static final Logger LOG = LoggerFactory.getLogger(BigQueryChangeApplierTest.class);

  static final String TABLE_NAME = "myinstance.mydb.mytable";
  static final Schema KEY_SCHEMA =
      Schema.of(Field.of("pk1", FieldType.STRING), Field.of("pk2", FieldType.INT32));

  static final Schema RECORD_SCHEMA1 =
      Schema.of(
          Field.of("pk1", FieldType.STRING),
          Field.of("pk2", FieldType.INT32),
          Field.of("field1", FieldType.DATETIME),
          Field.of("field2", FieldType.BYTES));

  static final Schema UPDATE_RECORD_SCHEMA =
      Schema.of(
          Field.of("operation", FieldType.STRING),
          Field.of("tableName", FieldType.STRING),
          Field.of("primaryKey", FieldType.row(KEY_SCHEMA)),
          Field.of("fullRecord", FieldType.row(RECORD_SCHEMA1)),
          Field.of("timestampMs", FieldType.INT64));

  static Long timestampCounter = 0L;

  static Row testInsertRecord(Row row) {
    Row keyRow =
        Row.withSchema(KEY_SCHEMA)
            .addValue(row.getValue("pk1"))
            .addValue(row.getValue("pk2"))
            .build();

    return Row.withSchema(UPDATE_RECORD_SCHEMA)
        .addValue("INSERT")
        .addValue(TABLE_NAME)
        .addValue(keyRow)
        .addValue(row)
        .addValue(timestampCounter)
        .build();
  }

  @Test
  void testSchemasEmittedOnlyOnChanges() {
    TestStream<Row> testSream =
        TestStream.create(SerializableCoder.of(Row.class))
            .addElements(
                testInsertRecord(
                    Row.withSchema(RECORD_SCHEMA1)
                        .addValues("k1", 1, DateTime.now(), "bytes".getBytes())
                        .build()),
                testInsertRecord(
                    Row.withSchema(RECORD_SCHEMA1)
                        .addValues("k1", 2, DateTime.now(), "bytes".getBytes())
                        .build()))
            .advanceWatermarkTo(Instant.now())
            .advanceWatermarkToInfinity();

    Pipeline p = Pipeline.create();

    PCollection<Row> input = p.apply(testSream).setRowSchema(UPDATE_RECORD_SCHEMA);

    PCollection<KV<String, KV<Schema, Schema>>> tableSchemaCollection =
        BigQueryChangeApplier.buildTableSchemaCollection(input);

    PAssert.that(tableSchemaCollection)
        .containsInAnyOrder(KV.of(TABLE_NAME, KV.of(KEY_SCHEMA, RECORD_SCHEMA1)));
    p.run().waitUntilFinish();
  }
}
