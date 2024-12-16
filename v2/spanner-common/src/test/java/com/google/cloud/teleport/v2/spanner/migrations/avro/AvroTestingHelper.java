/*
 * Copyright (C) 2024 Google LLC
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
package com.google.cloud.teleport.v2.spanner.migrations.avro;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

public class AvroTestingHelper {
  public static final Schema TIMESTAMPTZ_SCHEMA =
      SchemaBuilder.record("timestampTz")
          .fields()
          .name("timestamp")
          .type(LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG)))
          .noDefault()
          .name("offset")
          .type(LogicalTypes.timeMillis().addToSchema(Schema.create(Schema.Type.INT)))
          .noDefault()
          .endRecord();

  public static final Schema DATETIME_SCHEMA =
      SchemaBuilder.record("datetime")
          .fields()
          .name("date")
          .type(LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT)))
          .noDefault()
          .name("time")
          .type(LogicalTypes.timeMicros().addToSchema(Schema.create(Schema.Type.LONG)))
          .noDefault()
          .endRecord();

  public static final Schema INTERVAL_SCHEMA =
      SchemaBuilder.record("interval")
          .fields()
          .name("months")
          .type()
          .intType()
          .noDefault()
          .name("hours")
          .type()
          .intType()
          .noDefault()
          .name("micros")
          .type()
          .longType()
          .noDefault()
          .endRecord();

  public static final Schema INTERVAL_NANOS_SCHEMA =
      SchemaBuilder.builder()
          .record("intervalNano")
          .fields()
          .name("years")
          .type(SchemaBuilder.builder().longType())
          .withDefault(0L)
          .name("months")
          .type(SchemaBuilder.builder().longType())
          .withDefault(0L)
          .name("days")
          .type(SchemaBuilder.builder().longType())
          .withDefault(0L)
          .name("hours")
          .type(SchemaBuilder.builder().longType())
          .withDefault(0L)
          .name("minutes")
          .type(SchemaBuilder.builder().longType())
          .withDefault(0L)
          .name("seconds")
          .type(SchemaBuilder.builder().longType())
          .withDefault(0L)
          .name("nanos")
          .type(SchemaBuilder.builder().longType())
          .withDefault(0L)
          .endRecord();

  public static final Schema UNSUPPORTED_SCHEMA =
      SchemaBuilder.record("unsupportedName")
          .fields()
          .name("abc")
          .type()
          .intType()
          .noDefault()
          .endRecord();

  public static GenericRecord createTimestampTzRecord(Long timestamp, Integer offset) {
    GenericRecord genericRecord = new GenericData.Record(TIMESTAMPTZ_SCHEMA);
    genericRecord.put("timestamp", timestamp);
    genericRecord.put("offset", offset);
    return genericRecord;
  }

  public static GenericRecord createDatetimeRecord(Integer date, Long time) {
    GenericRecord genericRecord = new GenericData.Record(DATETIME_SCHEMA);
    genericRecord.put("date", date);
    genericRecord.put("time", time);
    return genericRecord;
  }

  public static GenericRecord createIntervalRecord(Integer months, Integer hours, Long micros) {
    GenericRecord genericRecord = new GenericData.Record(INTERVAL_SCHEMA);
    genericRecord.put("months", months);
    genericRecord.put("hours", hours);
    genericRecord.put("micros", micros);
    return genericRecord;
  }

  public static GenericRecord createIntervalNanosRecord(
      Long years, Long months, Long days, Long hours, Long minutes, Long seconds, Long nanos) {
    GenericRecord genericRecord = new GenericData.Record(INTERVAL_NANOS_SCHEMA);
    genericRecord.put("years", years);
    genericRecord.put("months", months);
    genericRecord.put("days", days);
    genericRecord.put("hours", hours);
    genericRecord.put("minutes", minutes);
    genericRecord.put("seconds", seconds);
    genericRecord.put("nanos", nanos);
    return genericRecord;
  }
}
