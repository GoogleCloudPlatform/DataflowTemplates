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
package com.google.cloud.teleport.v2.source.reader.io.schema.typemapping.provider.unified;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

/**
 * Wraps Constants for Custom Schemas needed by <a
 * href=https://cloud.google.com/datastream/docs/unified-types>unified types</a>.
 */
public final class CustomSchema {

  public static final class DateTime {
    public static final String RECORD_NAME = "datetime";
    public static final String DATE_FIELD_NAME = "date";
    public static final String TIME_FIELD_NAME = "time";
    public static final String TIME_FIELD_LOGICAL_TYPE_NAME = "time-micros";
    public static final Schema SCHEMA =
        SchemaBuilder.builder()
            .record(RECORD_NAME)
            .fields()
            .name(DATE_FIELD_NAME)
            .type(LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT)))
            .noDefault()
            .name(TIME_FIELD_NAME)
            .type(LogicalTypes.timeMicros().addToSchema(Schema.create(Schema.Type.LONG)))
            .noDefault()
            .endRecord();

    /** Static final class wrapping only constants. * */
    private DateTime() {}
  }

  public static final class Interval {
    public static final String RECORD_NAME = "interval";
    public static final String MONTHS_FIELD_NAME = "months";
    public static final String HOURS_FIELD_NAME = "hours";
    public static final String MICROS_FIELD_NAME = "micros";
    public static final Schema SCHEMA =
        SchemaBuilder.builder()
            .record(RECORD_NAME)
            .fields()
            .name(MONTHS_FIELD_NAME)
            .type(SchemaBuilder.builder().intType())
            .noDefault()
            .name(HOURS_FIELD_NAME)
            .type(SchemaBuilder.builder().intType())
            .noDefault()
            .name(MICROS_FIELD_NAME)
            .type(SchemaBuilder.builder().longType())
            .noDefault()
            .endRecord();

    /** Static final class wrapping only constants. * */
    private Interval() {}
  }

  public static final class TimeStampTz {
    public static final String RECORD_NAME = "timestampTz";
    public static final String TIMESTAMP_FIELD_NAME = "timestamp";
    public static final String TIMESTAMP_FIELD_LOGICAL_TYPE_NAME = "timestamp-micros";
    public static final String OFFSET_FIELD_NAME = "offset";
    public static final String OFFSET_FIELD_LOGICAL_TYPE_NAME = "time-millis";

    public static final Schema SCHEMA =
        SchemaBuilder.builder()
            .record(RECORD_NAME)
            .fields()
            .name(TIMESTAMP_FIELD_NAME)
            .type(LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG)))
            .noDefault()
            .name(OFFSET_FIELD_NAME)
            .type(LogicalTypes.timeMillis().addToSchema(Schema.create(Schema.Type.INT)))
            .noDefault()
            .endRecord();

    /** Static final class wrapping only constants. * */
    private TimeStampTz() {}
  }

  public static final class TimeTz {
    public static final String RECORD_NAME = "timeTz";
    public static final String TIME_FIELD_NAME = "time";
    public static final String TIME_FIELD_LOGICAL_TYPE_NAME = "time-micros";
    public static final String OFFSET_FIELD_NAME = "offset";
    public static final String OFFSET_FIELD_LOGICAL_TYPE_NAME = "time-millis";

    public static final Schema SCHEMA =
        SchemaBuilder.builder()
            .record(RECORD_NAME)
            .fields()
            .name(TIME_FIELD_NAME)
            .type(LogicalTypes.timeMicros().addToSchema(Schema.create(Schema.Type.LONG)))
            .noDefault()
            .name(OFFSET_FIELD_NAME)
            .type(LogicalTypes.timeMillis().addToSchema(Schema.create(Schema.Type.INT)))
            .noDefault()
            .endRecord();

    /** Static final class wrapping only constants. * */
    private TimeTz() {}
  }

  /** Static final class wrapping only constants. * */
  private CustomSchema() {}
}
