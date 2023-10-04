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
package com.google.cloud.teleport.v2.auto.schema;

import com.google.cloud.teleport.v2.auto.blocks.PubsubMessageToTableRow;
import com.google.cloud.teleport.v2.auto.blocks.ReadFromPubSub;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;

public class test {
  public static void main(String[] args) {
    //    TemplateOptionSchema a = new TemplateOptionSchema();
    //    a.schemaTypeCreator(ReadFromPubSub.ReadFromPubSubTransformConfiguration.class,
    // null).create("inputSubscription");
    PubsubMessageToTableRow transform = new PubsubMessageToTableRow();
    transform.from(
        Row.withSchema(
                Schema.of(
                    Schema.Field.of("javascriptTextTransformFunctionName", Schema.FieldType.STRING),
                    Schema.Field.of(
                        "javascriptTextTransformReloadIntervalMinutes",
                        Schema.FieldType.INT32.withNullable(true)),
                    Schema.Field.of("javascriptTextTransformGcsPath", Schema.FieldType.STRING),
                    Schema.Field.of("errorHandling", Schema.FieldType.row(Schema.of(Schema.Field.of("output", Schema.FieldType.STRING))).withNullable(true))))
            .withFieldValue("javascriptTextTransformReloadIntervalMinutes", null)
            .withFieldValue("javascriptTextTransformFunctionName", "uppercaseName")
            .withFieldValue(
                "javascriptTextTransformGcsPath", "gs://jkinard-test-templates/yaml/udf.js")
//            .withFieldValue("errorHandling", Row.withSchema(Schema.of(Schema.Field.of("output", Schema.FieldType.STRING))).withFieldValue("output", "myOutput").build())
            .build());
    System.out.println();
  }
}
