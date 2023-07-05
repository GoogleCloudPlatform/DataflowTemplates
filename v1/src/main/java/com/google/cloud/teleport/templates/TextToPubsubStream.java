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
import com.google.cloud.teleport.templates.TextToPubsub.Options;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Watch;
import org.joda.time.Duration;

/**
 * The {@code TextToPubsubStream} is a streaming version of the {@code TextToPubsub} pipeline that
 * publishes records to Cloud Pub/Sub from a set of files. The pipeline continuously polls for new
 * files, reads them row-by-row and publishes each record as a string message. The polling interval
 * is fixed and equals to 10 seconds. At the moment, publishing messages with attributes is
 * unsupported.
 *
 * <p>Check out <a
 * href="https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/v1/README_Stream_GCS_Text_to_Cloud_PubSub.md">README</a>
 * for instructions on how to use or modify this template.
 */
@Template(
    name = "Stream_GCS_Text_to_Cloud_PubSub",
    category = TemplateCategory.STREAMING,
    displayName = "Text Files on Cloud Storage to Pub/Sub",
    description =
        "A pipeline that polls every 10 seconds for new text files stored in Cloud Storage and outputs each line to a Pub/Sub topic.",
    optionsClass = Options.class,
    documentation =
        "https://cloud.google.com/dataflow/docs/guides/templates/provided/text-to-pubsub-stream",
    contactInformation = "https://cloud.google.com/support")
public class TextToPubsubStream extends TextToPubsub {
  private static final Duration DEFAULT_POLL_INTERVAL = Duration.standardSeconds(10);

  /**
   * Main entry-point for the pipeline. Reads in the command-line arguments, parses them, and
   * executes the pipeline.
   *
   * @param args Arguments passed in from the command-line.
   */
  public static void main(String[] args) {

    // Parse the user options passed from the command-line
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

    run(options);
  }

  /**
   * Executes the pipeline with the provided execution parameters.
   *
   * @param options The execution parameters.
   */
  public static PipelineResult run(Options options) {
    // Create the pipeline.
    Pipeline pipeline = Pipeline.create(options);

    /*
     * Steps:
     *  1) Read from the text source.
     *  2) Write each text record to Pub/Sub
     */
    pipeline
        .apply(
            "Read Text Data",
            TextIO.read()
                .from(options.getInputFilePattern())
                .watchForNewFiles(DEFAULT_POLL_INTERVAL, Watch.Growth.never()))
        .apply("Write to PubSub", PubsubIO.writeStrings().to(options.getOutputTopic()));

    return pipeline.run();
  }
}
