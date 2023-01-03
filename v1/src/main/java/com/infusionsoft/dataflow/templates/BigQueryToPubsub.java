/*
 * Copyright (C) 2022 Google LLC
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
package com.infusionsoft.dataflow.templates;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.teleport.templates.common.BigQueryConverters.BigQueryReadOptions;
import com.google.cloud.teleport.templates.common.PubsubConverters.PubsubWriteOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.extensions.jackson.AsJsons;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

/**
 * An template that reads BigQuery and sends its results to a Pubsub topic. Used primarily for
 * sending Smart List data. Requires a query and topic. {@code Example (minimum) gcloud command to
 * run the template: gcloud dataflow jobs run <job_name> \
 * --service-account-email=<service_account_email> \
 * --gcs-location=gs://<bucket>/<template_file_path> \ --parameters
 * ^:^pubsubWriteTopic=projects/<project_id>/topics/<topic_name>:readQuery='<query>' }
 */
public class BigQueryToPubsub {

  /**
   * Options supported by {@link BigQueryToPubsub}.
   *
   * <p>Inherits standard configuration options.
   */
  public interface Options extends PipelineOptions, PubsubWriteOptions, BigQueryReadOptions {}

  /**
   * Main entry point for executing the pipeline.
   *
   * @param args The command-line arguments to the pipeline.
   */
  public static void main(String[] args) {

    // Parse the user options passed from the command-line
    Options options = PipelineOptionsFactory.fromArgs(args).as(Options.class);
    run(options);
  }

  /**
   * Runs the pipeline with the supplied options.
   *
   * @param options The execution parameters to the pipeline.
   * @return The result of the pipeline execution.
   */
  public static PipelineResult run(Options options) {
    // Create the pipeline
    Pipeline pipeline = Pipeline.create(options);
    pipeline
        .apply(
            "BigQuery Read",
            BigQueryIO.readTableRows()
                .fromQuery(options.getReadQuery())
                .withoutValidation()
                .withTemplateCompatibility()
                .usingStandardSql())
        // Transforms TableRow into JSON-formatted String
        .apply("JSON Transform", AsJsons.of(TableRow.class))
        // Send each String to PubSub
        .apply("Write Events", PubsubIO.writeStrings().to(options.getPubsubWriteTopic()));

    // Execute the pipeline and return the result.
    return pipeline.run();
  }
}
