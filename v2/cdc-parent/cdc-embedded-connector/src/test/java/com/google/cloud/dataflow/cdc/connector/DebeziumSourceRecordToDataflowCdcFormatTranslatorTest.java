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
package com.google.cloud.dataflow.cdc.connector;

import com.google.cloud.dataflow.cdc.common.DataflowCdcRowFormat;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.debezium.time.MicroTimestamp;
import io.debezium.time.NanoTimestamp;
import io.debezium.time.Timestamp;
import org.apache.beam.sdk.values.Row;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Date;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Tests for message translations. */
public class DebeziumSourceRecordToDataflowCdcFormatTranslatorTest {

  private static final List<Schema> UNSUPPORTED_TYPES = ImmutableList.of(
      SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA).optional().build(),
      SchemaBuilder.array(Schema.STRING_SCHEMA).optional().build());

  @Test
  public void testNoPkIsTranslated() {
    Schema keySchema = SchemaBuilder.struct().build();
    Struct key = null;

    Schema internalStructSchema = SchemaBuilder.struct()
        .field("astring", Schema.STRING_SCHEMA).build();

    Schema valueAfterSchema = SchemaBuilder.struct()
        .field("team", Schema.STRING_SCHEMA)
        .field("year_founded", Schema.INT32_SCHEMA)
        .field("some_timestamp", Schema.INT64_SCHEMA)
        .field("some_milli_timestamp", Timestamp.schema())
        .field("some_kafka_timestamp", org.apache.kafka.connect.data.Timestamp.SCHEMA)
        .field("float_field", Schema.FLOAT32_SCHEMA)
        .field("double_field", Schema.FLOAT64_SCHEMA)
        .field("decimal_field", Decimal.schema(8))
        .field("struct_field", internalStructSchema)
        .build();

    Schema valueSchema = SchemaBuilder.struct()
        .field("after", valueAfterSchema)
        .field("op", Schema.STRING_SCHEMA)
        .field("ts_ms", Schema.INT64_SCHEMA)
        .build();

    Struct value = new Struct(valueSchema)
        .put("op", "c")
        .put("ts_ms", 1569287580660L)
        .put("after", new Struct(valueAfterSchema)
            .put("team", "team_PXHU")
            .put("year_founded", 1916)
            .put("some_timestamp", 123456579L)
            .put("some_milli_timestamp", 123456579L)
            .put("some_kafka_timestamp", new Date(123456579L))
            .put("float_field", new Float(123.456))
            .put("double_field", 123456579.98654321)
            .put("decimal_field", new BigDecimal("123456579.98654321"))
            .put("struct_field",
                new Struct(internalStructSchema).put("astring", "mastring")));

    String topicName = "mainstance.cdcForDataflow.team_metadata";

    SourceRecord input = new SourceRecord(
        ImmutableMap.of("server", "mainstance"),
        ImmutableMap.of(
            "file", "mysql-bin.000023",
            "pos", 110489,
            "gtids", "36797132-a366-11e9-ac33-42010a800456:1-6407169",
            "row", 1, "snapshot", true),
        topicName,
        keySchema,
        key,
        valueSchema,
        value);

    DebeziumSourceRecordToDataflowCdcFormatTranslator translator =
        new DebeziumSourceRecordToDataflowCdcFormatTranslator();
    Row translatedRecord = translator.translate(input);
    Row fullRecord = translatedRecord.getRow(DataflowCdcRowFormat.FULL_RECORD);

    assertThat(
        translatedRecord.getSchema().hasField(DataflowCdcRowFormat.PRIMARY_KEY), is(false));
    assertThat(fullRecord.getString("team"),
        is(value.getStruct("after").getString("team")));
    assertThat(fullRecord.getInt32("year_founded"),
        is(value.getStruct("after").getInt32("year_founded")));
    assertThat(fullRecord.getInt64("some_timestamp"),
        is(value.getStruct("after").getInt64("some_timestamp")));
    assertThat(fullRecord.getDateTime("some_milli_timestamp").getMillis(),
            is(value.getStruct("after").getInt64("some_milli_timestamp")));
    assertThat(fullRecord.getDateTime("some_kafka_timestamp").getMillis(),
            is(((Date) value.getStruct("after").get("some_kafka_timestamp")).getTime()));
    assertThat(fullRecord.getFloat("float_field"),
        is(value.getStruct("after").getFloat32("float_field")));
    assertThat(fullRecord.getDouble("double_field"),
        is(value.getStruct("after").getFloat64("double_field")));
    assertThat(fullRecord.getValue("decimal_field"),
        is(value.getStruct("after").get("decimal_field")));
    assertThat(fullRecord.getRow("struct_field").getString("astring"),
        is(value.getStruct("after").getStruct("struct_field").getString("astring")));
  }

  @Test
  public void testFullSourceRecordTranslation() {

    Schema keySchema = SchemaBuilder.struct()
        .field("team", Schema.STRING_SCHEMA).build();
    Struct key = new Struct(keySchema).put("team", "team_PXHU");

    Schema internalStructSchema = SchemaBuilder.struct()
        .field("astring", Schema.STRING_SCHEMA).build();

    Schema valueAfterSchema = SchemaBuilder.struct()
        .field("team", Schema.STRING_SCHEMA)
        .field("year_founded", Schema.INT32_SCHEMA)
        .field("some_timestamp", Schema.INT64_SCHEMA)
        .field("some_milli_timestamp", Timestamp.schema())
        .field("some_kafka_timestamp", org.apache.kafka.connect.data.Timestamp.SCHEMA)
        .field("float_field", Schema.FLOAT32_SCHEMA)
        .field("double_field", Schema.FLOAT64_SCHEMA)
        .field("decimal_field", Decimal.schema(8))
        .field("struct_field", internalStructSchema)
        .build();

    Schema valueSchema = SchemaBuilder.struct()
        .field("after", valueAfterSchema)
        .field("op", Schema.STRING_SCHEMA)
        .field("ts_ms", Schema.INT64_SCHEMA)
        .build();

    Struct value = new Struct(valueSchema)
        .put("op", "c")
        .put("ts_ms", 1569287580660L)
        .put("after", new Struct(valueAfterSchema)
            .put("team", "team_PXHU")
            .put("year_founded", 1916)
            .put("some_timestamp", 123456579L)
            .put("some_milli_timestamp", 123456579L)
            .put("some_kafka_timestamp", new Date(123456579L))
            .put("float_field", new Float(123.456))
            .put("double_field", 123456579.98654321)
            .put("decimal_field", new BigDecimal("123456579.98654321"))
            .put("struct_field",
                new Struct(internalStructSchema).put("astring", "mastring")));

    String topicName = "mainstance.cdcForDataflow.team_metadata";

    SourceRecord input = new SourceRecord(
        ImmutableMap.of("server", "mainstance"),
        ImmutableMap.of(
            "file", "mysql-bin.000023",
            "pos", 110489,
            "gtids", "36797132-a366-11e9-ac33-42010a800456:1-6407169",
            "row", 1, "snapshot", true),
        topicName,
        keySchema,
        key,
        valueSchema,
        value);

    DebeziumSourceRecordToDataflowCdcFormatTranslator translator =
        new DebeziumSourceRecordToDataflowCdcFormatTranslator();
    Row translatedRecord = translator.translate(input);

    Row fullRecord = translatedRecord.getRow("fullRecord");

    assertThat(fullRecord.getString("team"),
        is(value.getStruct("after").getString("team")));
    assertThat(fullRecord.getInt32("year_founded"),
        is(value.getStruct("after").getInt32("year_founded")));
    assertThat(fullRecord.getInt64("some_timestamp"),
        is(value.getStruct("after").getInt64("some_timestamp")));
    assertThat(fullRecord.getDateTime("some_milli_timestamp").getMillis(),
        is(value.getStruct("after").getInt64("some_milli_timestamp")));
    assertThat(fullRecord.getDateTime("some_kafka_timestamp").getMillis(),
        is(((Date) value.getStruct("after").get("some_kafka_timestamp")).getTime()));
    assertThat(fullRecord.getFloat("float_field"),
        is(value.getStruct("after").getFloat32("float_field")));
    assertThat(fullRecord.getDouble("double_field"),
        is(value.getStruct("after").getFloat64("double_field")));
    assertThat(fullRecord.getValue("decimal_field"),
        is(value.getStruct("after").get("decimal_field")));
    assertThat(fullRecord.getRow("struct_field").getString("astring"),
        is(value.getStruct("after").getStruct("struct_field").getString("astring")));
  }

  @Test
  public void testUnsupportedTypes() {
    Schema keySchema = SchemaBuilder.struct()
        .field("team", Schema.STRING_SCHEMA).build();
    Struct key = new Struct(keySchema).put("team", "team_PXHU");

    Schema internalStructSchema = SchemaBuilder.struct()
        .field("astring", Schema.STRING_SCHEMA).build();

    UNSUPPORTED_TYPES.forEach(type -> {
      Schema valueAfterSchema = SchemaBuilder.struct()
          .field("team", type)
          .build();

      Schema valueSchema = SchemaBuilder.struct()
          .field("after", valueAfterSchema)
          .field("op", Schema.STRING_SCHEMA)
          .field("ts_ms", Schema.INT64_SCHEMA)
          .build();

      Struct value = new Struct(valueSchema)
          .put("op", "c")
          .put("ts_ms", 1569287580660L)
          .put("after", new Struct(valueAfterSchema)
              .put("team", null));

      String topicName = "mainstance.cdcForDataflow.team_metadata";

      SourceRecord input = new SourceRecord(
          ImmutableMap.of("server", "mainstance"),
          ImmutableMap.of(
              "file", "mysql-bin.000023",
              "pos", 110489,
              "gtids", "36797132-a366-11e9-ac33-42010a800456:1-6407169",
              "row", 1, "snapshot", true),
          topicName,
          keySchema,
          key,
          valueSchema,
          value);

      final DebeziumSourceRecordToDataflowCdcFormatTranslator translator =
          new DebeziumSourceRecordToDataflowCdcFormatTranslator();
      assertThrows(DataException.class,
          () -> translator.translate(input));
    });
  }
}
