/*
 * Copyright (C) 2024 Google Inc.
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
package org.example.pipeline.json;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.beam.sdk.extensions.avro.schemas.utils.AvroUtils;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.Test;

/** */
public class JsonSchemaTest {

  static final String JSON_SCHEMA =
      """
      {
        "$schema": "http://json-schema.org/draft-06/schema#",
        "type": "object",
        "properties": {
          "agent": {
            "type": "object",
            "properties": {
              "name": {
                "type": "string"
              },
              "id": {
                "type": "number"
              },
              "ephemeral_id": {
                "type": "integer"
              },
              "agent": {
                  "type": "object",
                  "properties": {
                    "name": {
                      "type": "string"
                    }
                  },
                  "additionalProperties": false
                }
            },
            "additionalProperties": false
          },
          "multi_array": {
            "type": "array",
            "items": {
              "anyOf": [
                {
                  "type": "array",
                  "items": {
                    "type": "string"
                  }
                },
                {
                  "type": "null"
                }
              ]
            }
          },
          "multi_array_no_type": {
            "type": "array",
            "items": {
              "type": [
                "null",
                "array"
              ]
            }
          }
        },
        "additionalProperties": false
      }
      """;

  static final Schema EXPECTED_AVRO_SCHEMA_DEFAULT =
      SchemaBuilder.record(JsonSchema.TOP_LEVEL_NAME)
          .namespace(JsonSchema.TOP_LEVEL_NAMESPACE)
          .doc(JsonSchema.DEFAULT_RECORD_DOC_PREFIX + JsonSchema.TOP_LEVEL_NAME)
          .fields()
          .name("multi_array")
          .type()
          .array()
          .items()
          .array()
          .items()
          .stringType()
          .noDefault()
          .name("agent")
          .type()
          .unionOf()
          .nullType()
          .and()
          .record("agent")
          .namespace(JsonSchema.TOP_LEVEL_NAMESPACE + ".root")
          .doc(JsonSchema.DEFAULT_RECORD_DOC_PREFIX + "agent")
          .fields()
          .name("name")
          .type()
          .unionOf()
          .nullType()
          .and()
          .stringType()
          .endUnion()
          .noDefault()
          .name("agent")
          .type()
          .unionOf()
          .nullType()
          .and()
          .record("agent")
          .namespace(JsonSchema.TOP_LEVEL_NAMESPACE + ".root.agent")
          .doc(JsonSchema.DEFAULT_RECORD_DOC_PREFIX + "agent")
          .fields()
          .name("name")
          .type()
          .unionOf()
          .nullType()
          .and()
          .stringType()
          .endUnion()
          .noDefault()
          .endRecord()
          .endUnion()
          .noDefault()
          .name("id")
          .type()
          .unionOf()
          .nullType()
          .and()
          .type(LogicalTypes.decimal(77, 38).addToSchema(Schema.create(Schema.Type.BYTES)))
          .endUnion()
          .noDefault()
          .name("ephemeral_id")
          .type()
          .unionOf()
          .nullType()
          .and()
          .longType()
          .endUnion()
          .noDefault()
          .endRecord()
          .endUnion()
          .noDefault()
          .name("multi_array_no_type")
          .type()
          .array()
          .items()
          .array()
          .items()
          .stringType()
          .noDefault()
          .endRecord();

  @Test
  public void testTransformedAvroSchemaIsBQCompatible() {
    var bqSaneSchema =
        JsonSchema.avroSchemaFromJsonSchema(
            JSON_SCHEMA,
            JsonSchema.ResolutionConfig.of(
                JsonSchema.CombinedSchemaResolution.FAVOR_ARRAYS,
                JsonSchema.MultiArraySchemaResolution.FLATTEN));
    var tableSchema = BigQueryUtils.toTableSchema(AvroUtils.toBeamSchema(bqSaneSchema));
    var reconstructed =
        BigQueryUtils.toGenericAvroSchema(JsonSchema.TOP_LEVEL_NAME, tableSchema.getFields());
    MatcherAssert.assertThat(reconstructed, Matchers.is(bqSaneSchema));
  }

  @Test
  public void testAvroSchemaFromJsonSchema_String_JsonSchemaResolutionConfig() {
    var avroSchema = JsonSchema.avroSchemaFromJsonSchema(JSON_SCHEMA);
    MatcherAssert.assertThat(avroSchema, Matchers.is(EXPECTED_AVRO_SCHEMA_DEFAULT));
  }
}