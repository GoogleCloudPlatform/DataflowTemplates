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

import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.teleport.v2.source.reader.io.schema.typemapping.provider.unified.CustomSchema.DateTime;
import com.google.cloud.teleport.v2.source.reader.io.schema.typemapping.provider.unified.CustomSchema.Interval;
import com.google.cloud.teleport.v2.source.reader.io.schema.typemapping.provider.unified.CustomSchema.TimeStampTz;
import com.google.cloud.teleport.v2.source.reader.io.schema.typemapping.provider.unified.CustomSchema.TimeTz;
import org.apache.avro.Schema;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

/** Test class for {@link CustomSchema}. */
@RunWith(MockitoJUnitRunner.class)
public class CustomSchemaTest {
  @Test
  public void testDateTime() {
    assertThat(DateTime.SCHEMA.getName()).isEqualTo(DateTime.RECORD_NAME);
    assertThat(DateTime.SCHEMA.getType()).isEqualTo(Schema.Type.RECORD);
    assertThat(DateTime.SCHEMA.getField(DateTime.DATE_FIELD_NAME).schema().getType())
        .isEqualTo(Schema.Type.INT);
    assertThat(DateTime.SCHEMA.getField(DateTime.TIME_FIELD_NAME).schema().getType())
        .isEqualTo(Schema.Type.LONG);
    assertThat(
            DateTime.SCHEMA.getField(DateTime.TIME_FIELD_NAME).schema().getLogicalType().getName())
        .isEqualTo(DateTime.TIME_FIELD_LOGICAL_TYPE_NAME);
  }

  @Test
  public void testInterval() {
    assertThat(Interval.SCHEMA.getName()).isEqualTo(Interval.RECORD_NAME);
    assertThat(Interval.SCHEMA.getType()).isEqualTo(Schema.Type.RECORD);
    assertThat(Interval.SCHEMA.getField(Interval.MONTHS_FIELD_NAME).schema().getType())
        .isEqualTo(Schema.Type.INT);
    assertThat(Interval.SCHEMA.getField(Interval.HOURS_FIELD_NAME).schema().getType())
        .isEqualTo(Schema.Type.INT);
    assertThat(Interval.SCHEMA.getField(Interval.MICROS_FIELD_NAME).schema().getType())
        .isEqualTo(Schema.Type.LONG);
  }

  @Test
  public void testTimestampTz() {
    assertThat(TimeStampTz.SCHEMA.getName()).isEqualTo(TimeStampTz.RECORD_NAME);
    assertThat(TimeStampTz.SCHEMA.getType()).isEqualTo(Schema.Type.RECORD);
    assertThat(TimeStampTz.SCHEMA.getField(TimeStampTz.TIMESTAMP_FIELD_NAME).schema().getType())
        .isEqualTo(Schema.Type.LONG);
    assertThat(
            TimeStampTz.SCHEMA
                .getField(TimeStampTz.TIMESTAMP_FIELD_NAME)
                .schema()
                .getLogicalType()
                .getName())
        .isEqualTo(TimeStampTz.TIMESTAMP_FIELD_LOGICAL_TYPE_NAME);
    assertThat(TimeStampTz.SCHEMA.getField(TimeStampTz.OFFSET_FIELD_NAME).schema().getType())
        .isEqualTo(Schema.Type.INT);
    assertThat(
            TimeStampTz.SCHEMA
                .getField(TimeStampTz.OFFSET_FIELD_NAME)
                .schema()
                .getLogicalType()
                .getName())
        .isEqualTo(TimeStampTz.OFFSET_FIELD_LOGICAL_TYPE_NAME);
  }

  @Test
  public void testTimeTz() {
    assertThat(TimeTz.SCHEMA.getName()).isEqualTo(TimeTz.RECORD_NAME);
    assertThat(TimeTz.SCHEMA.getType()).isEqualTo(Schema.Type.RECORD);
    assertThat(TimeTz.SCHEMA.getField(TimeTz.TIME_FIELD_NAME).schema().getType())
        .isEqualTo(Schema.Type.LONG);
    assertThat(TimeTz.SCHEMA.getField(TimeTz.TIME_FIELD_NAME).schema().getLogicalType().getName())
        .isEqualTo(TimeTz.TIME_FIELD_LOGICAL_TYPE_NAME);
    assertThat(TimeTz.SCHEMA.getField(TimeTz.OFFSET_FIELD_NAME).schema().getType())
        .isEqualTo(Schema.Type.INT);
    assertThat(TimeTz.SCHEMA.getField(TimeTz.OFFSET_FIELD_NAME).schema().getLogicalType().getName())
        .isEqualTo(TimeTz.OFFSET_FIELD_LOGICAL_TYPE_NAME);
  }
}
