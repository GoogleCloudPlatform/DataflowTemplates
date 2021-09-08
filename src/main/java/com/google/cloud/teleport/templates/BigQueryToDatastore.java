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

import com.google.cloud.teleport.templates.common.BigQueryConverters.BigQueryReadOptions;
import com.google.cloud.teleport.templates.common.BigQueryConverters.BigQueryToEntity;
import com.google.cloud.teleport.templates.common.DatastoreConverters.DatastoreWriteOptions;
import com.google.cloud.teleport.templates.common.DatastoreConverters.WriteEntities;
import com.google.cloud.teleport.templates.common.ErrorConverters.ErrorWriteOptions;
import com.google.cloud.teleport.templates.common.ErrorConverters.LogErrors;
import com.google.datastore.v1.Entity;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;

/**
 * Dataflow template which reads BigQuery data and writes it to Datastore. The source data can be
 * either a BigQuery table or a SQL query.
 */
public class BigQueryToDatastore {

  /** Custom PipelineOptions. */
  public interface BigQueryToDatastoreOptions
      extends BigQueryReadOptions, DatastoreWriteOptions, ErrorWriteOptions {}

  /**
   * Runs a pipeline which reads data from BigQuery and writes it to Datastore.
   *
   * @param args arguments to the pipeline
   */
  public static void main(String[] args) {

    BigQueryToDatastoreOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(BigQueryToDatastoreOptions.class);

    Pipeline pipeline = Pipeline.create(options);

    // Read from BigQuery and convert data to Datastore Entity format with 2 possible outcomes,
    // success or failure, based on the possibility to create valid Entity keys from BQ data
    TupleTag<Entity> successTag = new TupleTag<Entity>() {};
    TupleTag<String> failureTag = new TupleTag<String>("failures") {};
    PCollectionTuple entities =
        pipeline.apply(
            BigQueryToEntity.newBuilder()
                .setQuery(options.getReadQuery())
                .setUniqueNameColumn(options.getReadIdColumn())
                .setEntityKind(options.getDatastoreWriteEntityKind())
                .setNamespace(options.getDatastoreWriteNamespace())
                .setSuccessTag(successTag)
                .setFailureTag(failureTag)
                .build());

    // Write on GCS data that could not be converted to valid Datastore entities
    entities.apply(
        LogErrors.newBuilder()
            .setErrorWritePath(options.getInvalidOutputPath())
            .setErrorTag(failureTag)
            .build());

    // Write valid entities to Datastore
    TupleTag<String> errorTag = new TupleTag<String>("errors") {};
    entities
        .get(successTag)
        .apply(
            WriteEntities.newBuilder()
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
