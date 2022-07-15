/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.syndeo;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import com.google.cloud.syndeo.common.ProviderUtil.TransformSpec;
import com.google.cloud.syndeo.v1.SyndeoV1.PipelineDescription;

public class WritePipelineSpecForTesting {

  public static void main(String[] args) {
    writeToFile(
        pubsubToAvro(),
        "/Users/laraschmidt/Documents/beam2/config_gen/beam/pubsub_to_avro_config.txt");
  }

  public static void writeToFile(List<TransformSpec> specs, String filename) {
    PipelineDescription configuration = getFromTransformSpecs(specs);

    try {
      File output = new File(filename);
      FileOutputStream file = new FileOutputStream(output);
      configuration.writeTo(file);
      file.close();
    } catch (IOException e) {
      System.out.println("An error occurred.");
    }
  }

  public static List<TransformSpec> pubsubToAvro() {
    List<TransformSpec> specs = new ArrayList<>();
    Schema schema =
        Schema.of(
            Field.of("species", FieldType.STRING),
            Field.of("number", FieldType.INT32),
            Field.of("event_timestamp", FieldType.DATETIME));
    specs.add(
        new TransformSpec(
            "schemaIO:pubsub:read",
            Arrays.asList(
                "projects/google.com:clouddfe/topics/syndeo_demo",
                schema,
                null,
                null,
                "json",
                null,
                null,
                null)));
    specs.add(
        new TransformSpec(
            "schemaIO:avro:write",
            Arrays.asList("gs://clouddfe-laraschmidt/avro-out", schema, 60L)));
    return specs;
  }

  // Gets the pipeline description from the transform specs.
  public static PipelineDescription getFromTransformSpecs(List<TransformSpec> specs) {
    PipelineDescription.Builder configuration = PipelineDescription.newBuilder();
    for (TransformSpec spec : specs) {
      configuration.addTransforms(spec.toProto());
    }
    return configuration.build();
  }
}
