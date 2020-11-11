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
package com.google.cloud.dataflow.cdc.applier;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.common.collect.Lists;
import java.util.List;
import org.apache.beam.sdk.schemas.Schema;
import org.junit.jupiter.api.Test;


/** Tests for BigQuerySchemaUtils class. */
public class BigQuerySchemaUtilsTest {

  private static final Schema ALL_BEAM_SCHEMA =
      Schema.builder()
          .addBooleanField("booleanField")
          .addByteField("byteField")
          .addDateTimeField("dateField")
          .addDecimalField("decimalField")
          .addDoubleField("doubleField")
          .addFloatField("floatField")
          .addInt16Field("int16Field")
          .addInt16Field("int64Field")
          .addStringField("stringField")
          .build();

  private static final com.google.cloud.bigquery.Schema ALL_BQ_SCHEMA =
      com.google.cloud.bigquery.Schema.of(
          Field.newBuilder("booleanField", StandardSQLTypeName.BOOL).build(),
          Field.newBuilder("byteField", StandardSQLTypeName.BYTES).build(),
          Field.newBuilder("dateField", StandardSQLTypeName.TIMESTAMP).build(),
          Field.newBuilder("decimalField", StandardSQLTypeName.NUMERIC).build(),
          Field.newBuilder("doubleField", StandardSQLTypeName.FLOAT64).build(),
          Field.newBuilder("floatField", StandardSQLTypeName.FLOAT64).build(),
          Field.newBuilder("int16Field", StandardSQLTypeName.INT64).build(),
          Field.newBuilder("int64Field", StandardSQLTypeName.INT64).build(),
          Field.newBuilder("stringField", StandardSQLTypeName.STRING).build());

  private static final List<Schema> UNSUPPORTED_TYPE_SCHEMAS =
      Lists.newArrayList(
          Schema.builder()
              .addMapField(
                  "myMapField", Schema.FieldType.STRING, Schema.FieldType.STRING)
              .build(),
          Schema.builder()
              .addRowField("myRowField", Schema.builder().build()).build(),
          Schema.builder().addArrayField("myArrayField", Schema.FieldType.STRING).build());

  @Test
  public void testComplexSchemaToBQSchema() {
    assertThat(BigQuerySchemaUtils.beamSchemaToBigQueryClientSchema(ALL_BEAM_SCHEMA),
        is(ALL_BQ_SCHEMA));
  }

  @Test
  public void testUnsupportedTypes() {

    UNSUPPORTED_TYPE_SCHEMAS.forEach(schema -> {
      assertThrows(
          IllegalArgumentException.class,
          () -> BigQuerySchemaUtils.beamSchemaToBigQueryClientSchema(schema),
          "Expected an exception with unsupported type conversion, but didn't see one");
    });
  }
}
