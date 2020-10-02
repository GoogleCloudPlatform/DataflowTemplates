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

import com.google.cloud.teleport.templates.common.BigQueryConverters;
import com.google.cloud.teleport.templates.common.DatastoreConverters.DatastoreReadOptions;
import com.google.cloud.teleport.templates.common.DatastoreConverters.ReadJsonEntities;
import com.google.cloud.teleport.templates.common.JavascriptTextTransformer.JavascriptTextTransformerOptions;
import com.google.cloud.teleport.templates.common.JavascriptTextTransformer.TransformTextViaJavascript;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;

/**
 * Dataflow template which copies Datastore Entities to a BigQuery table.
 */
public class DatastoreToBigQuery {
  public interface DatastoreToBigQueryOptions
      extends PipelineOptions, DatastoreReadOptions, JavascriptTextTransformerOptions {
    @Description("The BigQuery table spec to write the output to")
    ValueProvider<String> getOutputTableSpec();

    void setOutputTableSpec(ValueProvider<String> value);

    @Validation.Required
    @Description("Temporary directory for BigQuery loading process")
    ValueProvider<String> getBigQueryLoadingTemporaryDirectory();

    void setBigQueryLoadingTemporaryDirectory(ValueProvider<String> directory);
  }

  /**
   * Runs a pipeline which reads in Entities from Datastore, passes in the JSON encoded Entities
   * to a Javascript UDF that returns JSON that conforms to the BigQuery TableRow spec and writes
   * the TableRows to BigQuery.
   *
   * @param args arguments to the pipeline
   */
  public static void main(String[] args) {
    DatastoreToBigQueryOptions options = PipelineOptionsFactory.fromArgs(args)
        .withValidation()
        .as(DatastoreToBigQueryOptions.class);

    Pipeline pipeline = Pipeline.create(options);

    pipeline
        .apply(
            ReadJsonEntities.newBuilder()
                .setGqlQuery(options.getDatastoreReadGqlQuery())
                .setProjectId(options.getDatastoreReadProjectId())
                .setNamespace(options.getDatastoreReadNamespace())
                .build())
        .apply(
            TransformTextViaJavascript.newBuilder()
                .setFileSystemPath(options.getJavascriptTextTransformGcsPath())
                .setFunctionName(options.getJavascriptTextTransformFunctionName())
                .build())
        .apply(BigQueryConverters.jsonToTableRow())
        .apply(
            "WriteBigQuery",
            BigQueryIO.writeTableRows()
                .withoutValidation()
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
                .to(options.getOutputTableSpec())
                .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
                .withWriteDisposition(WriteDisposition.WRITE_TRUNCATE)
                .withCustomGcsTempLocation(options.getBigQueryLoadingTemporaryDirectory()));

    pipeline.run();
  }
}
