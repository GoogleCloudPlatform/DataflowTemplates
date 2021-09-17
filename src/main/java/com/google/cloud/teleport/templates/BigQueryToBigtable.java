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

import com.google.cloud.bigtable.beam.CloudBigtableIO;
import com.google.cloud.bigtable.beam.CloudBigtableTableConfiguration;
import com.google.cloud.teleport.templates.common.BigQueryConverters.BigQueryReadOptions;
import com.google.cloud.teleport.templates.common.BigQueryConverters.BigQueryToEntity;
import com.google.cloud.teleport.templates.common.BigQueryConverters.BigQueryToMutation;
import com.google.cloud.teleport.templates.common.BigtableConverters.BigtableWriteOptions;
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
 * Dataflow template which reads BigQuery data and writes it to Bigtable. The source data can be
 * either a BigQuery table or a SQL query.
 */
public class BigQueryToBigtable {

  /**
   * Custom PipelineOptions.
   */
  public interface BigQueryToBigtableOptions extends BigQueryReadOptions, BigtableWriteOptions {

  }

  /**
   * Runs a pipeline which reads data from BigQuery and writes it to Bigtable.
   *
   * @param args arguments to the pipeline
   */
  public static void main(String[] args) {


    BigQueryToBigtableOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(BigQueryToBigtableOptions.class);
    CloudBigtableTableConfiguration bigtableTableConfig =
        new CloudBigtableTableConfiguration.Builder()
            .withProjectId(options.getBigtableWriteProjectId())
            .withInstanceId(options.getBigtableWriteInstanceId())
            .withTableId(options.getBigtableWriteTableId())
            .build();

    Pipeline pipeline = Pipeline.create(options);

    pipeline.apply(
        BigQueryToMutation.newBuilder()
            .setQuery(options.getReadQuery())
            .setColumnFamily(options.getBigtableWriteColumnFamily())
            .setRowkey(options.getReadIdColumn())
            .build())
        .apply(CloudBigtableIO.writeToTable(bigtableTableConfig));

    pipeline.run();
  }
}
