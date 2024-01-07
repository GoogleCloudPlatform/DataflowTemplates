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
import com.google.cloud.teleport.metadata.TemplateParameter;
import com.google.cloud.teleport.templates.TextToPubsub.Options;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.options.ValueProvider;

/**
 * The {@code TextToPubsub} pipeline publishes records to Cloud Pub/Sub from a set of files. The
 * pipeline reads each file row-by-row and publishes each record as a string message. At the moment,
 * publishing messages with attributes is unsupported.
 *
 * <p>Check out <a
 * href="https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/v1/README_GCS_Text_to_Cloud_PubSub.md">README</a>
 * for instructions on how to use or modify this template.
 */
@Template(
    name = "GCS_Text_to_Cloud_PubSub",
    category = TemplateCategory.BATCH,
    displayName = "Cloud Storage Text File to Pub/Sub (Batch)",
    description = {
      "This template creates a batch pipeline that reads records from text files stored in Cloud Storage and publishes them to a Pub/Sub topic. "
          + "The template can be used to publish records in a newline-delimited file containing JSON records or CSV file to a Pub/Sub topic for real-time processing. "
          + "You can use this template to replay data to Pub/Sub.\n",
      "This template does not set any timestamp on the individual records. The event time is equal to the publishing time during execution. "
          + "If your pipeline relies on an accurate event time for processing, you must not use this pipeline."
    },
    optionsClass = Options.class,
    documentation =
        "https://cloud.google.com/dataflow/docs/guides/templates/provided/cloud-storage-to-pubsub",
    contactInformation = "https://cloud.google.com/support",
    requirements = {
      "The files to read need to be in newline-delimited JSON or CSV format. Records spanning multiple lines in the source files might cause issues downstream because each line within the files will be published as a message to Pub/Sub.",
      "The Pub/Sub topic must exist before running the pipeline."
    })
public class TextToPubsub {

  /** The custom options supported by the pipeline. Inherits standard configuration options. */
  public interface Options extends PipelineOptions {
    @TemplateParameter.Text(
        order = 1,
        description = "Cloud Storage Input File(s)",
        helpText = "Path of the file pattern glob to read from.",
        regexes = {"^gs:\\/\\/[^\\n\\r]+$"},
        example = "gs://your-bucket/path/*.txt")
    @Required
    ValueProvider<String> getInputFilePattern();

    void setInputFilePattern(ValueProvider<String> value);

    @TemplateParameter.PubsubTopic(
        order = 2,
        description = "Output Pub/Sub topic",
        helpText =
            "The name of the topic to which data should published, in the format of 'projects/your-project-id/topics/your-topic-name'",
        example = "projects/your-project-id/topics/your-topic-name")
    @Required
    ValueProvider<String> getOutputTopic();

    void setOutputTopic(ValueProvider<String> value);
  }

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
        .apply("Read Text Data", TextIO.read().from(options.getInputFilePattern()))
        .apply("Write to PubSub", PubsubIO.writeStrings().to(options.getOutputTopic()));

    return pipeline.run();
  }
}
