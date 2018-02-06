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

import com.google.cloud.teleport.templates.common.DatastoreConverters.DatastoreReadSchemaCount;
import com.google.cloud.teleport.templates.common.DatastoreConverters.DatastoreReadSchemaCountOptions;
import com.google.cloud.teleport.templates.common.TextConverters.FilesystemWriteOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

/**
 * Dataflow template which pulls a list and count of unique schemas for a given Entity / GQL Query.
 * This information can be used to help migrate Datastore Data to Schemad systems such-as a
 * RDBMS.
 */
public class DatastoreSchemasCountToText {

  interface DatastoreSchemaCountToTextOptions extends
      PipelineOptions,
      DatastoreReadSchemaCountOptions,
      FilesystemWriteOptions {}

  /**
   * Runs a pipeline which reads in Entities from datastore, parses the Entity's schema,
   * and counts the unique number of schemas.
   * @param args arguments to the pipeline
   */
  public static void main(String[] args) {
    DatastoreSchemaCountToTextOptions options = PipelineOptionsFactory.fromArgs(args)
        .withValidation()
        .as(DatastoreSchemaCountToTextOptions.class);

    Pipeline pipeline = Pipeline.create(options);

    pipeline
        .apply(DatastoreReadSchemaCount.newBuilder()
            .setGqlQuery(options.getDatastoreReadGqlQuery())
            .setProjectId(options.getDatastoreReadProjectId())
            .setNamespace(options.getDatastoreReadNamespace())
            .build())
        .apply(TextIO.write()
            .to(options.getTextWritePrefix())
            .withSuffix(".json"));

    pipeline.run();
  }
}
