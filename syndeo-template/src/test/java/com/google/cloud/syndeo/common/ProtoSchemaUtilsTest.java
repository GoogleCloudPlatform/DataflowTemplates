/*
 * Copyright (C) 2023 Google LLC
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
package com.google.cloud.syndeo.common;

import static com.google.common.truth.Truth.assertThat;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.logicaltypes.EnumerationType;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.io.Resources;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ProtoSchemaUtilsTest {

  @Test
  public void testParsingSimpleProtoSchema()
      throws IOException, ProtoSchemaUtils.SyndeoSchemaParseException {
    InputStream is = Resources.getResource("proto_schemas/simple_proto3_schema.proto").openStream();
    String sampleData = new String(is.readAllBytes(), StandardCharsets.UTF_8);

    assertThat(ProtoSchemaUtils.beamSchemaFromProtoSchemaDescription(sampleData).getFields())
        .containsExactly(
            Schema.Field.nullable("message_type", Schema.FieldType.STRING),
            Schema.Field.nullable("message", Schema.FieldType.STRING));
  }

  @Test
  public void testParsingMediumAndNestedProtoSchema()
      throws IOException, ProtoSchemaUtils.SyndeoSchemaParseException {
    InputStream is =
        Resources.getResource("proto_schemas/more_complex_proto3_schema.proto").openStream();
    String sampleData = new String(is.readAllBytes(), StandardCharsets.UTF_8);

    assertThat(ProtoSchemaUtils.beamSchemaFromProtoSchemaDescription(sampleData).getFields())
        .containsExactly(
            Schema.Field.nullable("name", Schema.FieldType.STRING),
            Schema.Field.nullable("id", Schema.FieldType.INT32),
            Schema.Field.nullable("timestamp", Schema.FieldType.DOUBLE),
            Schema.Field.nullable("age_in_seconds", Schema.FieldType.INT64),
            Schema.Field.nullable("temperature", Schema.FieldType.FLOAT),
            Schema.Field.nullable("email", Schema.FieldType.STRING),
            Schema.Field.of(
                "phones",
                Schema.FieldType.array(
                    Schema.FieldType.row(
                        Schema.of(
                            Schema.Field.nullable("number", Schema.FieldType.STRING),
                            Schema.Field.nullable(
                                "type",
                                Schema.FieldType.logicalType(
                                    EnumerationType.create("MOBILE", "HOME", "WORK"))))))));
  }
}
