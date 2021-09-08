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
import com.google.cloud.teleport.templates.common.ErrorConverters.ErrorWriteOptions;
import com.google.cloud.teleport.templates.common.ErrorConverters.LogErrors;
import com.google.cloud.teleport.templates.common.JavascriptTextTransformer.JavascriptTextTransformerOptions;
import com.google.cloud.teleport.templates.common.JavascriptTextTransformer.TransformTextViaJavascript;
import com.google.cloud.teleport.templates.common.TextConverters.FilesystemReadOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.TupleTag;

/**
 * Dataflow template which reads from a Text Source and writes JSON encoded Entities into Datastore.
 * The Json is expected to be in the format of:
 * https://cloud.google.com/datastore/docs/reference/rest/v1/Entity
 */
public class TextToDatastore {

  /** TextToDatastore Pipeline Options. */
  public interface TextToDatastoreOptions
      extends PipelineOptions,
          FilesystemReadOptions,
          JavascriptTextTransformerOptions,
          DatastoreWriteOptions,
          ErrorWriteOptions {}

  /**
   * Runs a pipeline which reads from a Text Source, passes the Text to a Javascript UDF, writes the
   * JSON encoded Entities to a TextIO sink.
   *
   * <p>If your Text Source does not contain JSON encoded Entities, then you'll need to supply a
   * Javascript UDF which transforms your data to be JSON encoded Entities.
   *
   * @param args arguments to the pipeline
   */
  public static void main(String[] args) {
    TextToDatastoreOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(TextToDatastoreOptions.class);

    TupleTag<String> errorTag = new TupleTag<String>("errors") {};

    Pipeline pipeline = Pipeline.create(options);

    pipeline
        .apply(TextIO.read().from(options.getTextReadPattern()))
        .apply(
            TransformTextViaJavascript.newBuilder()
                .setFileSystemPath(options.getJavascriptTextTransformGcsPath())
                .setFunctionName(options.getJavascriptTextTransformFunctionName())
                .build())
        .apply(
            WriteJsonEntities.newBuilder()
                .setProjectId(options.getDatastoreWriteProjectId())
                .setHintNumWorkers(options.getDatastoreHintNumWorkers())
                .setThrottleRampup(options.getDatastoreThrottleRampup())
                .setErrorTag(errorTag)
                .build())
        .apply(
            LogErrors.newBuilder()
                .setErrorWritePath(options.getErrorWritePath())
                .setErrorTag(errorTag)
                .build());

    pipeline.run();
  }
}
