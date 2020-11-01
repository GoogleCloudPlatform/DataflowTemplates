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
package com.google.cloud.dataflow.cdc.common;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.junit.jupiter.api.Test;

/** Tests for SchemaUtils class. */
public class SchemaUtilsTest {

  @Test
  public void testConvertComplexBeamSchema() {
    Schema complexSchema = Schema.builder()
        .addStringField("operation")
        .addStringField("tableName")
        .addRowField("primaryKey", Schema.builder()
            .addStringField("team")
            .build())
        .addRowField("fullRecord", Schema.builder()
            .addField(Field.nullable("team", FieldType.STRING))
            .addField(Field.of("city", FieldType.STRING))
            .addStringField("country")
            .addInt32Field("year_founded")
            .addInt16Field("tiny_int")
            .addDecimalField("decimal")
            .build())
        .addInt64Field("timestampMs")
        .build();

    assertThat(
        SchemaUtils.toBeamSchema(
            SchemaUtils.fromBeamSchema(complexSchema)),
        is(complexSchema));
  }
}
