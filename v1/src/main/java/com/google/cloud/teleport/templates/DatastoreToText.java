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

import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.templates.DatastoreToText.DatastoreToTextOptions;
import com.google.cloud.teleport.templates.common.DatastoreConverters.DatastoreReadOptions;
import com.google.cloud.teleport.templates.common.DatastoreConverters.ReadJsonEntities;
import com.google.cloud.teleport.templates.common.FirestoreNestedValueProvider;
import com.google.cloud.teleport.templates.common.JavascriptTextTransformer.JavascriptTextTransformerOptions;
import com.google.cloud.teleport.templates.common.JavascriptTextTransformer.TransformTextViaJavascript;
import com.google.cloud.teleport.templates.common.TextConverters.FilesystemWriteOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;

/**
 * Dataflow template which copies Datastore Entities to a Text sink. Text is encoded using JSON
 * encoded entity in the v1/Entity rest format: <a
 * href="https://cloud.google.com/datastore/docs/reference/rest/v1/Entity">Entity</a>
 *
 * <p>Check out <a
 * href="https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/v1/README_Datastore_to_GCS_Text.md">README_Datastore_to_GCS_Text</a>
 * or <a
 * href="https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/v1/README_Firestore_to_GCS_Text.md">README_Firestore_to_GCS_Text</a>
 * for instructions on how to use or change this template.
 */
@Template(
    name = "Datastore_to_GCS_Text",
    category = TemplateCategory.LEGACY,
    displayName = "Datastore to Text Files on Cloud Storage [Deprecated]",
    description =
        "The Datastore to Cloud Storage Text template is a batch pipeline that reads Datastore entities and writes them "
            + "to Cloud Storage as text files. You can provide a function to process each entity as a JSON string. "
            + "If you don't provide such a function, every line in the output file will be a JSON-serialized entity.",
    optionsClass = DatastoreToTextOptions.class,
    skipOptions = {
      "firestoreReadNamespace",
      "firestoreReadGqlQuery",
      "firestoreReadProjectId",
      "javascriptFunctionReload",
      "javascriptReloadFunction"
    },
    documentation =
        "https://cloud.google.com/dataflow/docs/guides/templates/provided/datastore-to-cloud-storage",
    contactInformation = "https://cloud.google.com/support",
    requirements = {"Datastore must be set up in the project before running the pipeline."})
@Template(
    name = "Firestore_to_GCS_Text",
    category = TemplateCategory.BATCH,
    displayName = "Firestore (Datastore mode) to Text Files on Cloud Storage",
    description =
        "The Firestore to Cloud Storage Text template is a batch pipeline that reads Firestore entities and writes them "
            + "to Cloud Storage as text files. You can provide a function to process each entity as a JSON string. "
            + "If you don't provide such a function, every line in the output file will be a JSON-serialized entity.",
    optionsClass = DatastoreToTextOptions.class,
    skipOptions = {
      "datastoreReadNamespace",
      "datastoreReadGqlQuery",
      "datastoreReadProjectId",
      "javascriptFunctionReload",
      "javascriptReloadIntervalMinutes"
    },
    documentation =
        "https://cloud.google.com/dataflow/docs/guides/templates/provided/firestore-to-cloud-storage",
    contactInformation = "https://cloud.google.com/support",
    requirements = {"Firestore must be set up in the project before running the pipeline."})
public class DatastoreToText {

  public static ValueProvider<String> selectProvidedInput(
      ValueProvider<String> datastoreInput, ValueProvider<String> firestoreInput) {
    return new FirestoreNestedValueProvider(datastoreInput, firestoreInput);
  }

  /** Custom PipelineOptions. */
  public interface DatastoreToTextOptions
      extends PipelineOptions,
          DatastoreReadOptions,
          JavascriptTextTransformerOptions,
          FilesystemWriteOptions {}

  /**
   * Runs a pipeline which reads in Entities from Datastore, passes in the JSON encoded Entities to
   * a Javascript UDF, and writes the JSON to TextIO sink.
   *
   * @param args arguments to the pipeline
   */
  public static void main(String[] args) {
    DatastoreToTextOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(DatastoreToTextOptions.class);

    Pipeline pipeline = Pipeline.create(options);

    pipeline
        .apply(
            ReadJsonEntities.newBuilder()
                .setGqlQuery(
                    selectProvidedInput(
                        options.getDatastoreReadGqlQuery(), options.getFirestoreReadGqlQuery()))
                .setProjectId(
                    selectProvidedInput(
                        options.getDatastoreReadProjectId(), options.getFirestoreReadProjectId()))
                .setNamespace(
                    selectProvidedInput(
                        options.getDatastoreReadNamespace(), options.getFirestoreReadNamespace()))
                .build())
        .apply(
            TransformTextViaJavascript.newBuilder()
                .setFileSystemPath(options.getJavascriptTextTransformGcsPath())
                .setFunctionName(options.getJavascriptTextTransformFunctionName())
                .build())
        .apply(TextIO.write().to(options.getTextWritePrefix()).withSuffix(".json"));

    pipeline.run();
  }
}
