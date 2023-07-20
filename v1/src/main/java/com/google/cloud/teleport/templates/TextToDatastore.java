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

import com.google.cloud.teleport.metadata.MultiTemplate;
import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.templates.TextToDatastore.TextToDatastoreOptions;
import com.google.cloud.teleport.templates.common.DatastoreConverters.DatastoreWriteOptions;
import com.google.cloud.teleport.templates.common.DatastoreConverters.WriteJsonEntities;
import com.google.cloud.teleport.templates.common.ErrorConverters.ErrorWriteOptions;
import com.google.cloud.teleport.templates.common.ErrorConverters.LogErrors;
import com.google.cloud.teleport.templates.common.FirestoreNestedValueProvider;
import com.google.cloud.teleport.templates.common.JavascriptTextTransformer.JavascriptTextTransformerOptions;
import com.google.cloud.teleport.templates.common.JavascriptTextTransformer.TransformTextViaJavascript;
import com.google.cloud.teleport.templates.common.TextConverters.FilesystemReadOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.values.TupleTag;

/**
 * Dataflow template which reads from a Text Source and writes JSON encoded Entities into Datastore.
 * The JSON is expected to be in the format of: <a
 * href="https://cloud.google.com/datastore/docs/reference/rest/v1/Entity">Entity</a>
 *
 * <p>Check out <a
 * href="https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/v1/README_GCS_Text_to_Datastore.md">README
 * Datastore</a> or <a
 * href="https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/v1/README_GCS_Text_to_Firestore.md">README
 * Firestore</a> for instructions on how to use or modify this template.
 */
@MultiTemplate({
  @Template(
      name = "GCS_Text_to_Datastore",
      category = TemplateCategory.BATCH,
      displayName = "Text Files on Cloud Storage to Datastore [Deprecated]",
      description =
          "Batch pipeline. Reads from text files stored in Cloud Storage and writes JSON-encoded entities to Datastore.",
      optionsClass = TextToDatastoreOptions.class,
      skipOptions = {
        "firestoreWriteProjectId",
        "firestoreWriteEntityKind",
        "firestoreWriteNamespace",
        "firestoreHintNumWorkers"
      },
      documentation =
          "https://cloud.google.com/dataflow/docs/guides/templates/provided/cloud-storage-to-datastore",
      contactInformation = "https://cloud.google.com/support"),
  @Template(
      name = "GCS_Text_to_Firestore",
      category = TemplateCategory.BATCH,
      displayName = "Text Files on Cloud Storage to Firestore (Datastore mode)",
      description =
          "Batch pipeline. Reads from text files stored in Cloud Storage and writes JSON-encoded entities to Firestore.",
      optionsClass = TextToDatastoreOptions.class,
      skipOptions = {
        "datastoreWriteProjectId",
        "datastoreWriteEntityKind",
        "datastoreWriteNamespace",
        "datastoreHintNumWorkers"
      },
      documentation =
          "https://cloud.google.com/dataflow/docs/guides/templates/provided/cloud-storage-to-firestore",
      contactInformation = "https://cloud.google.com/support")
})
public class TextToDatastore {

  public static <T> ValueProvider<T> selectProvidedInput(
      ValueProvider<T> datastoreInput, ValueProvider<T> firestoreInput) {
    return new FirestoreNestedValueProvider(datastoreInput, firestoreInput);
  }

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
                .setReloadFunction(options.getJavascriptFunctionReload())
                .setReloadIntervalMinutes(options.getJavascriptReloadIntervalMinutes())
                .build())
        .apply(
            WriteJsonEntities.newBuilder()
                .setProjectId(
                    selectProvidedInput(
                        options.getDatastoreWriteProjectId(), options.getFirestoreWriteProjectId()))
                .setHintNumWorkers(
                    selectProvidedInput(
                        options.getDatastoreHintNumWorkers(), options.getFirestoreHintNumWorkers()))
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
