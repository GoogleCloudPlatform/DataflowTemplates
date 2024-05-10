/*
 * Copyright (C) 2023 Google LLC
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
package com.google.cloud.teleport.v2.templates;

import static com.google.cloud.teleport.v2.utils.DurationUtils.parseDuration;

import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.metadata.TemplateParameter;
import com.google.cloud.teleport.v2.avro.AvroPubsubMessageRecord;
import com.google.cloud.teleport.v2.io.WindowedFilenamePolicy;
import com.google.cloud.teleport.v2.options.WindowedFilenamePolicyOptions;
import com.google.cloud.teleport.v2.templates.PubsubToAvro.Options;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.extensions.avro.io.AvroIO;
import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;

/**
 * This pipeline ingests incoming data from a Cloud Pub/Sub topic and outputs the raw data into
 * windowed Avro files at the specified output directory.
 *
 * <p>Files output will have the following schema:
 *
 * <pre>
 *   {
 *      "type": "record",
 *      "name": "AvroPubsubMessageRecord",
 *      "namespace": "com.google.cloud.teleport.v2.avro",
 *      "fields": [
 *        {"name": "message", "type": {"type": "array", "items": "bytes"}},
 *        {"name": "attributes", "type": {"type": "map", "values": "string"}},
 *        {"name": "timestamp", "type": "long"}
 *      ]
 *   }
 * </pre>
 *
 * <p>Check out <a
 * href="https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/v1/README_Cloud_PubSub_to_Avro.md">README</a>
 * for instructions on how to use or modify this template.
 */
@Template(
    name = "Cloud_PubSub_to_Avro_Flex",
    category = TemplateCategory.STREAMING,
    displayName = "Pub/Sub to Avro Files on Cloud Storage",
    description =
        "The Pub/Sub to Avro files on Cloud Storage template is a streaming pipeline that reads data from a "
            + "Pub/Sub topic and writes Avro files into the specified Cloud Storage bucket.",
    optionsClass = Options.class,
    flexContainerName = "pubsub-to-avro",
    documentation =
        "https://cloud.google.com/dataflow/docs/guides/templates/provided/pubsub-to-avro",
    contactInformation = "https://cloud.google.com/support",
    requirements = {"The input Pub/Sub topic must exist prior to pipeline execution."},
    streaming = true,
    supportsAtLeastOnce = true)
public class PubsubToAvro {

  /**
   * Options supported by the pipeline.
   *
   * <p>Inherits standard configuration options.
   */
  public interface Options
      extends PipelineOptions, StreamingOptions, WindowedFilenamePolicyOptions {
    @TemplateParameter.PubsubSubscription(
        order = 1,
        groupName = "Source",
        optional = true,
        description = "Pub/Sub input subscription",
        helpText =
            "Pub/Sub subscription to read the input from, in the format of"
                + " 'projects/your-project-id/subscriptions/your-subscription-name'",
        example = "projects/your-project-id/subscriptions/your-subscription-name")
    String getInputSubscription();

    void setInputSubscription(String value);

    @TemplateParameter.PubsubTopic(
        order = 2,
        groupName = "Source",
        optional = true,
        description = "Pub/Sub input topic",
        helpText =
            "Pub/Sub topic to read the input from, in the format of "
                + "'projects/your-project-id/topics/your-topic-name'")
    String getInputTopic();

    void setInputTopic(String value);

    @TemplateParameter.GcsWriteFolder(
        order = 3,
        groupName = "Target",
        optional = false,
        description = "Output file directory in Cloud Storage",
        helpText =
            "The path and filename prefix for writing output files. Must end with a slash. DateTime"
                + " formatting is used to parse directory path for date & time formatters.")
    @Required
    String getOutputDirectory();

    void setOutputDirectory(String value);

    @TemplateParameter.Text(
        order = 4,
        groupName = "Target",
        optional = true,
        description = "Output filename prefix of the files to write",
        helpText = "The prefix to place on each windowed file.",
        regexes = "^[a-zA-Z\\-]+$")
    @Default.String("output")
    String getOutputFilenamePrefix();

    void setOutputFilenamePrefix(String value);

    @TemplateParameter.Text(
        order = 5,
        groupName = "Target",
        optional = true,
        description = "Output filename suffix of the files to write",
        helpText =
            "The suffix to place on each windowed file. Typically a file extension such "
                + "as .txt or .csv.")
    @Default.String("")
    String getOutputFilenameSuffix();

    void setOutputFilenameSuffix(String value);

    @TemplateParameter.GcsWriteFolder(
        order = 6,
        groupName = "Target",
        optional = false,
        description = "Temporary Avro write directory",
        helpText = "Directory for temporary Avro files.")
    @Required
    String getAvroTempDirectory();

    void setAvroTempDirectory(String value);
  }

  /**
   * Main entry point for executing the pipeline.
   *
   * @param args The command-line arguments to the pipeline.
   */
  public static void main(String[] args) {

    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    options.setStreaming(true);

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

    PCollection<PubsubMessage> messages = null;

    /*
     * Steps:
     *   1) Read messages from PubSub
     *   2) Window the messages into minute intervals specified by the executor.
     *   3) Output the windowed data into Avro files, one per window by default.
     */

    if (options.getInputSubscription() != null) {
      messages =
          pipeline.apply(
              "Read PubSub Events",
              PubsubIO.readMessagesWithAttributes()
                  .fromSubscription(options.getInputSubscription()));
    } else {
      messages =
          pipeline.apply(
              "Read PubSub Events",
              PubsubIO.readMessagesWithAttributes().fromTopic(options.getInputTopic()));
    }
    messages
        .apply("Map to Archive", ParDo.of(new PubsubMessageToArchiveDoFn()))
        .apply(
            options.getWindowDuration() + " Window",
            Window.into(FixedWindows.of(parseDuration(options.getWindowDuration()))))

        // Apply windowed file writes. Use a NestedValueProvider because the filename
        // policy requires a resourceId generated from the input value at runtime.
        .apply(
            "Write File(s)",
            AvroIO.write(AvroPubsubMessageRecord.class)
                .to(
                    WindowedFilenamePolicy.writeWindowedFiles()
                        .withOutputDirectory(options.getOutputDirectory())
                        .withOutputFilenamePrefix(options.getOutputFilenamePrefix())
                        .withShardTemplate(options.getOutputShardTemplate())
                        .withSuffix(options.getOutputFilenameSuffix())
                        .withYearPattern(options.getYearPattern())
                        .withMonthPattern(options.getMonthPattern())
                        .withDayPattern(options.getDayPattern())
                        .withHourPattern(options.getHourPattern())
                        .withMinutePattern(options.getMinutePattern()))
                .withTempDirectory(
                    FileBasedSink.convertToFileResourceIfPossible(options.getOutputDirectory()))
                .withWindowedWrites()
                .withNumShards(options.getNumShards()));

    // Execute the pipeline and return the result.
    return pipeline.run();
  }

  /**
   * Converts an incoming {@link PubsubMessage} to the {@link AvroPubsubMessageRecord} class by
   * copying its fields and the timestamp of the message.
   */
  static class PubsubMessageToArchiveDoFn extends DoFn<PubsubMessage, AvroPubsubMessageRecord> {
    @ProcessElement
    public void processElement(ProcessContext context) {
      PubsubMessage message = context.element();
      context.output(
          new AvroPubsubMessageRecord(
              message.getPayload(), message.getAttributeMap(), context.timestamp().getMillis()));
    }
  }
}
