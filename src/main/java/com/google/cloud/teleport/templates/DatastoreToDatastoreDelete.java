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

import com.google.cloud.teleport.templates.common.DatastoreConverters.DatastoreDeleteEntityJson;
import com.google.cloud.teleport.templates.common.DatastoreConverters.DatastoreDeleteOptions;
import com.google.cloud.teleport.templates.common.DatastoreConverters.DatastoreReadOptions;
import com.google.cloud.teleport.templates.common.DatastoreConverters.ReadJsonEntities;
import com.google.cloud.teleport.templates.common.JavascriptTextTransformer.JavascriptTextTransformerOptions;
import com.google.cloud.teleport.templates.common.JavascriptTextTransformer.TransformTextViaJavascript;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

/**
 * Dataflow template which deletes pulled Datastore Entities.
 */
public class DatastoreToDatastoreDelete {

  /**
   * Custom PipelineOptions
   */
  public interface DatastoreToDatastoreDeleteOptions extends
      PipelineOptions,
      DatastoreReadOptions,
      JavascriptTextTransformerOptions,
      DatastoreDeleteOptions {}

  /**
   * Runs a pipeline which reads in Entities from datastore, passes in the JSON encoded Entities
   * to a Javascript UDF, and deletes all the Entities.
   *
   * <p>If the UDF returns value of undefined or null for a given Entity, then that Entity will not
   * be deleted.
   *
   * @param args arguments to the pipeline
   */
  public static void main(String[] args) {
    DatastoreToDatastoreDeleteOptions options = PipelineOptionsFactory.fromArgs(args)
        .withValidation()
        .as(DatastoreToDatastoreDeleteOptions.class);

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
        .apply(DatastoreDeleteEntityJson.newBuilder()
            .setProjectId(options.getDatastoreDeleteProjectId())
            .build());

    pipeline.run();
  }

}
