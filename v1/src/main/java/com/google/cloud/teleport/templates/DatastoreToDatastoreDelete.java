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
import com.google.cloud.teleport.templates.DatastoreToDatastoreDelete.DatastoreToDatastoreDeleteOptions;
import com.google.cloud.teleport.templates.common.DatastoreConverters.DatastoreDeleteEntityJson;
import com.google.cloud.teleport.templates.common.DatastoreConverters.DatastoreDeleteOptions;
import com.google.cloud.teleport.templates.common.DatastoreConverters.DatastoreReadOptions;
import com.google.cloud.teleport.templates.common.DatastoreConverters.ReadJsonEntities;
import com.google.cloud.teleport.templates.common.FirestoreNestedValueProvider;
import com.google.cloud.teleport.templates.common.JavascriptTextTransformer.JavascriptTextTransformerOptions;
import com.google.cloud.teleport.templates.common.JavascriptTextTransformer.TransformTextViaJavascript;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;

/**
 * Dataflow template which deletes pulled Datastore Entities.
 *
 * <p>Check out <a
 * href="https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/v1/README_Datastore_to_Datastore_Delete.md">README
 * Datastore</a> or <a
 * href="https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/v1/README_Firestore_to_Firestore_Delete.md">README
 * Firestore</a> for instructions on how to use or modify this template.
 */
@Template(
    name = "Datastore_to_Datastore_Delete",
    category = TemplateCategory.UTILITIES,
    displayName = "Bulk Delete Entities in Datastore [Deprecated]",
    description =
        "A pipeline which reads in Entities (via a GQL query) from Datastore, optionally passes in the JSON encoded Entities to a JavaScript UDF, and then deletes all matching Entities in the selected target project.",
    optionsClass = DatastoreToDatastoreDeleteOptions.class,
    skipOptions = {
      "firestoreReadGqlQuery",
      "firestoreReadProjectId",
      "firestoreReadNamespace",
      "firestoreDeleteProjectId",
      "firestoreHintNumWorkers"
    },
    documentation =
        "https://cloud.google.com/dataflow/docs/guides/templates/provided/datastore-bulk-delete",
    contactInformation = "https://cloud.google.com/support")
@Template(
    name = "Firestore_to_Firestore_Delete",
    category = TemplateCategory.UTILITIES,
    displayName = "Bulk Delete Entities in Firestore (Datastore mode)",
    description =
        "A pipeline which reads in Entities (via a GQL query) from Firestore, optionally passes in the JSON encoded Entities to a JavaScript UDF, and then deletes all matching Entities in the selected target project.",
    optionsClass = DatastoreToDatastoreDeleteOptions.class,
    optionsOrder = {
      DatastoreReadOptions.class,
      DatastoreDeleteOptions.class,
      JavascriptTextTransformerOptions.class
    },
    skipOptions = {
      "datastoreReadGqlQuery",
      "datastoreReadProjectId",
      "datastoreReadNamespace",
      "datastoreDeleteProjectId",
      "datastoreHintNumWorkers"
    },
    documentation =
        "https://cloud.google.com/dataflow/docs/guides/templates/provided/firestore-bulk-delete",
    contactInformation = "https://cloud.google.com/support")
public class DatastoreToDatastoreDelete {

  public static <T> ValueProvider<T> selectProvidedInput(
      ValueProvider<T> datastoreInput, ValueProvider<T> firestoreInput) {
    return new FirestoreNestedValueProvider<T>(datastoreInput, firestoreInput);
  }

  /** Custom PipelineOptions. */
  public interface DatastoreToDatastoreDeleteOptions
      extends PipelineOptions,
          DatastoreReadOptions,
          JavascriptTextTransformerOptions,
          DatastoreDeleteOptions {}

  /**
   * Runs a pipeline which reads in Entities from datastore, passes in the JSON encoded Entities to
   * a Javascript UDF, and deletes all the Entities.
   *
   * <p>If the UDF returns value of undefined or null for a given Entity, then that Entity will not
   * be deleted.
   *
   * @param args arguments to the pipeline
   */
  public static void main(String[] args) {
    DatastoreToDatastoreDeleteOptions options =
        PipelineOptionsFactory.fromArgs(args)
            .withValidation()
            .as(DatastoreToDatastoreDeleteOptions.class);

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
        .apply(
            DatastoreDeleteEntityJson.newBuilder()
                .setProjectId(
                    selectProvidedInput(
                        options.getDatastoreDeleteProjectId(),
                        options.getFirestoreDeleteProjectId()))
                .setHintNumWorkers(
                    selectProvidedInput(
                        options.getDatastoreHintNumWorkers(), options.getFirestoreHintNumWorkers()))
                .build());

    pipeline.run();
  }
}
