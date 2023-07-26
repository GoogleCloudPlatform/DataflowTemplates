/*
 * Copyright (C) 2018 Google LLC
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
package com.google.cloud.teleport.templates;

import com.google.cloud.teleport.templates.common.DatastoreConverters.DatastoreWriteOptions;
import com.google.cloud.teleport.templates.common.DatastoreConverters.WriteJsonEntities;
import com.google.cloud.teleport.templates.common.JavascriptTextTransformer.JavascriptTextTransformerOptions;
import com.google.cloud.teleport.templates.common.JavascriptTextTransformer.TransformTextViaJavascript;
import com.google.cloud.teleport.templates.common.PubsubConverters.PubsubReadOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

/**
 * Dataflow template which copies Pubsub Messages to Datastore. This expects Pubsub messages to
 * contain JSON text in the v1/Entity rest format:
 * https://cloud.google.com/datastore/docs/reference/rest/v1/Entity
 */
public class PubsubToDatastore {
  public interface PubsubToDatastoreOptions
      extends PipelineOptions,
          PubsubReadOptions,
          JavascriptTextTransformerOptions,
          DatastoreWriteOptions {}

  /**
   * Runs a pipeline which reads in JSON from Pubsub, feeds the JSON to a Javascript UDF, and writes
   * the JSON encoded Entities to Datastore.
   *
   * @param args arguments to the pipeline
   */
  public static void main(String[] args) {
    PubsubToDatastoreOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(PubsubToDatastoreOptions.class);

    Pipeline pipeline = Pipeline.create(options);

    pipeline
        .apply(PubsubIO.readStrings().fromTopic(options.getPubsubReadTopic()))
        .apply(
            TransformTextViaJavascript.newBuilder()
                .setFileSystemPath(options.getJavascriptTextTransformGcsPath())
                .setFunctionName(options.getJavascriptTextTransformFunctionName())
                .setReloadFunction(options.getJavascriptFunctionReload())
                .setReloadIntervalMinutes(options.getJavascriptReloadIntervalMinutes())
                .build())
        .apply(
            WriteJsonEntities.newBuilder()
                .setProjectId(options.getDatastoreWriteProjectId())
                .setHintNumWorkers(options.getDatastoreHintNumWorkers())
                .build());

    pipeline.run();
  }
}
