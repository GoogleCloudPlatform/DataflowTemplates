/*
 * Copyright (C) 2018 Google Inc.
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

package com.google.cloud.teleport.templates;

import com.google.cloud.teleport.templates.common.DatastoreConverters.DatastoreReadOptions;
import com.google.cloud.teleport.templates.common.DatastoreConverters.ReadJsonEntities;
import com.google.cloud.teleport.templates.common.JavascriptTextTransformer.JavascriptTextTransformerOptions;
import com.google.cloud.teleport.templates.common.JavascriptTextTransformer.TransformTextViaJavascript;
import com.google.cloud.teleport.templates.common.TextConverters.FilesystemWriteOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

/**
 * Dataflow template which copies Datastore Entities to a Text sink.
 * Text is encoded using JSON encoded entity in the v1/Entity rest format:
 * https://cloud.google.com/datastore/docs/reference/rest/v1/Entity
 */
public class DatastoreToText {

  /**
   * Custom PipelineOptions
   */
  public interface DatastoreToTextOptions extends
      PipelineOptions,
      DatastoreReadOptions,
      JavascriptTextTransformerOptions,
      FilesystemWriteOptions {}

  /**
   * Runs a pipeline which reads in Entities from Datastore, passes in the JSON encoded Entities
   * to a Javascript UDF, and writes the JSON to TextIO sink.
   *
   * @param args arguments to the pipeline
   */
  public static void main(String[] args) {
    DatastoreToTextOptions options = PipelineOptionsFactory.fromArgs(args)
        .withValidation()
        .as(DatastoreToTextOptions.class);

    Pipeline pipeline = Pipeline.create(options);

    pipeline
        .apply(ReadJsonEntities.newBuilder()
            .setGqlQuery(options.getDatastoreReadGqlQuery())
            .setProjectId(options.getDatastoreReadProjectId())
            .setNamespace(options.getDatastoreReadNamespace())
            .build())
        .apply(TransformTextViaJavascript.newBuilder()
            .setFileSystemPath(options.getJavascriptTextTransformGcsPath())
            .setFunctionName(options.getJavascriptTextTransformFunctionName())
            .build())
        .apply(TextIO.write()
            .to(options.getTextWritePrefix())
            .withSuffix(".json"));

    pipeline.run();
  }
}
