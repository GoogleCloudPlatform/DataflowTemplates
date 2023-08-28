/*
 * Copyright (C) 2021 Google LLC
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
package com.google.cloud.teleport.v2.elasticsearch.templates;

import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.v2.common.UncaughtExceptionLogger;
import com.google.cloud.teleport.v2.elasticsearch.options.BigQueryToElasticsearchOptions;
import com.google.cloud.teleport.v2.elasticsearch.transforms.WriteToElasticsearch;
import com.google.cloud.teleport.v2.transforms.BigQueryConverters.ReadBigQueryTableRows;
import com.google.cloud.teleport.v2.transforms.BigQueryConverters.TableRowToJsonFn;
import com.google.cloud.teleport.v2.transforms.JavascriptTextTransformer.TransformTextViaJavascript;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;

/**
 * The {@link BigQueryToElasticsearch} pipeline exports data from a BigQuery table to Elasticsearch.
 *
 * <p>Check out <a
 * href="https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/v2/googlecloud-to-elasticsearch/README_BigQuery_to_Elasticsearch.md">README</a>
 * for instructions on how to use or modify this template.
 */
@Template(
    name = "BigQuery_to_Elasticsearch",
    category = TemplateCategory.BATCH,
    displayName = "BigQuery to Elasticsearch",
    description =
        "The BigQuery to Elasticsearch template is a batch pipeline that ingests data from a BigQuery table into Elasticsearch as documents. "
            + "The template can either read the entire table or read specific records using a supplied query.",
    optionsClass = BigQueryToElasticsearchOptions.class,
    flexContainerName = "bigquery-to-elasticsearch",
    documentation =
        "https://cloud.google.com/dataflow/docs/guides/templates/provided/bigquery-to-elasticsearch",
    contactInformation = "https://cloud.google.com/support",
    preview = true,
    requirements = {
      "The source BigQuery table must exist.",
      "A Elasticsearch host on a Google Cloud instance or on Elastic Cloud with Elasticsearch version 7.0 or above and should be accessible from the Dataflow worker machines.",
    })
public class BigQueryToElasticsearch {
  /**
   * Main entry point for pipeline execution.
   *
   * @param args Command line arguments to the pipeline.
   */
  public static void main(String[] args) {
    UncaughtExceptionLogger.register();

    BigQueryToElasticsearchOptions options =
        PipelineOptionsFactory.fromArgs(args)
            .withValidation()
            .as(BigQueryToElasticsearchOptions.class);

    run(options);
  }

  /**
   * Runs the pipeline with the supplied options.
   *
   * @param options The execution parameters to the pipeline.
   * @return The result of the pipeline execution.
   */
  private static PipelineResult run(BigQueryToElasticsearchOptions options) {

    // Create the pipeline.
    Pipeline pipeline = Pipeline.create(options);
    /*
     * Steps: 1) Read records from BigQuery via BigQueryIO.
     *        2) Create json string from Table Row.
     *        3) Write records to Elasticsearch.
     *
     *
     * Step #1: Read from BigQuery. If a query is provided then it is used to get the TableRows.
     */
    pipeline
        .apply(
            "ReadFromBigQuery",
            ReadBigQueryTableRows.newBuilder()
                .setOptions(options.as(BigQueryToElasticsearchOptions.class))
                .build())

        /*
         * Step #2: Convert table rows to JSON documents.
         */
        .apply("TableRowsToJsonDocument", ParDo.of(new TableRowToJsonFn()))

        /*
         * Step #3: Apply UDF functions (if specified)
         */
        .apply(
            TransformTextViaJavascript.newBuilder()
                .setFileSystemPath(options.getJavascriptTextTransformGcsPath())
                .setFunctionName(options.getJavascriptTextTransformFunctionName())
                .build())

        /*
         * Step #4: Write converted records to Elasticsearch
         */
        .apply(
            "WriteToElasticsearch",
            WriteToElasticsearch.newBuilder()
                .setOptions(options.as(BigQueryToElasticsearchOptions.class))
                .build());

    return pipeline.run();
  }
}
