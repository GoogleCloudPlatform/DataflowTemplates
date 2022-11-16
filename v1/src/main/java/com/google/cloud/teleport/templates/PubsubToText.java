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

import com.google.cloud.teleport.io.WindowedFilenamePolicy;
import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.metadata.TemplateParameter;
import com.google.cloud.teleport.options.WindowedFilenamePolicyOptions;
import com.google.cloud.teleport.templates.PubsubToText.Options;
import com.google.cloud.teleport.util.DualInputNestedValueProvider;
import com.google.cloud.teleport.util.DualInputNestedValueProvider.TranslatorInput;
import com.google.cloud.teleport.util.DurationUtils;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.NestedValueProvider;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;

/**
 * This pipeline ingests incoming data from a Cloud Pub/Sub topic and outputs the raw data into
 * windowed files at the specified output directory.
 *
 * <p>Example Usage:
 *
 * <pre>
 * # Set the pipeline vars
 * PIPELINE_NAME=PubsubToText
 * PROJECT_ID=PROJECT ID HERE
 * PIPELINE_BUCKET=TEMPLATE STORAGE BUCKET NAME HERE
 * OUTPUT_BUCKET=JOB OUTPUT BUCKET NAME HERE
 * PIPELINE_FOLDER=gs://${PIPELINE_BUCKET}/dataflow/pipelines/pubsub-to-gcs-text
 * USE_SUBSCRIPTION=true or false depending on whether the pipeline should read
 *                  from a Pub/Sub Subscription or a Pub/Sub Topic.
 *
 * # Set the runner
 * RUNNER=DataflowRunner
 *
 * # Build the template
 * mvn compile exec:java \
 * -Dexec.mainClass=com.google.cloud.teleport.templates.${PIPELINE_NAME} \
 * -Dexec.cleanupDaemonThreads=false \
 * -Dexec.args=" \
 * --project=${PROJECT_ID} \
 * --stagingLocation=${PIPELINE_FOLDER}/staging \
 * --tempLocation=${PIPELINE_FOLDER}/temp \
 * --templateLocation=${PIPELINE_FOLDER}/template \
 * --runner=${RUNNER} \
 * --useSubscription=${USE_SUBSCRIPTION}"
 *
 * # Execute the template
 * JOB_NAME=pubsub-to-bigquery-$USER-`date +"%Y%m%d-%H%M%S%z"`
 *
 * # Execute a pipeline to read from a Topic.
 * gcloud dataflow jobs run ${JOB_NAME} \
 * --gcs-location=${PIPELINE_FOLDER}/template \
 * --zone=us-east1-d \
 * --parameters \
 * "inputTopic=projects/${PROJECT_ID}/topics/input-topic-name,\
 * userTempLocation=gs://${OUTPUT_BUCKET}/tmp/,\
 * windowDuration=5m,\
 * numShards=1,\
 * outputDirectory=gs://${OUTPUT_BUCKET}/output/,\
 * outputFilenamePrefix=windowed-file,\
 * outputFilenameSuffix=.txt"
 *
 * # Execute a pipeline to read from a Subscription.
 * gcloud dataflow jobs run ${JOB_NAME} \
 * --gcs-location=${PIPELINE_FOLDER}/template \
 * --zone=us-east1-d \
 * --parameters \
 * "inputSubscription=projects/${PROJECT_ID}/subscriptions/input-subscription-name,\
 * windowDuration=5m,\
 * numShards=1,\
 * userTempLocation=gs://${OUTPUT_BUCKET}/tmp/,\
 * outputDirectory=gs://${OUTPUT_BUCKET}/output/,\
 * outputFilenamePrefix=windowed-file,\
 * outputFilenameSuffix=.txt"
 * </pre>
 */
@Template(
    name = "Cloud_PubSub_to_GCS_Text",
    category = TemplateCategory.STREAMING,
    displayName = "Pub/Sub to Text Files on Cloud Storage",
    description =
        "Streaming pipeline. Reads records from Pub/Sub and writes them to Cloud Storage, creating a text file for each five minute window. Note that this pipeline assumes no newlines in the body of the Pub/Sub message and thus each message becomes a single line in the output file.",
    optionsClass = Options.class,
    contactInformation = "https://cloud.google.com/support")
public class PubsubToText {

  /**
   * Options supported by the pipeline.
   *
   * <p>Inherits standard configuration options.
   */
  public interface Options
      extends PipelineOptions, StreamingOptions, WindowedFilenamePolicyOptions {

    @TemplateParameter.PubsubSubscription(
        order = 1,
        optional = true,
        description = "Pub/Sub input subscription",
        helpText =
            "Pub/Sub subscription to read the input from, in the format of 'projects/your-project-id/subscriptions/your-subscription-name'",
        example = "projects/your-project-id/subscriptions/your-subscription-name")
    ValueProvider<String> getInputSubscription();

    void setInputSubscription(ValueProvider<String> value);

    @TemplateParameter.PubsubTopic(
        order = 2,
        optional = true,
        description = "Pub/Sub input topic",
        helpText =
            "Pub/Sub topic to read the input from, in the format of "
                + "'projects/your-project-id/topics/your-topic-name'")
    ValueProvider<String> getInputTopic();

    void setInputTopic(ValueProvider<String> value);

    @Description(
        "This determines whether the template reads from a Pub/Sub subscription or a topic")
    @Default.Boolean(false)
    Boolean getUseSubscription();

    void setUseSubscription(Boolean value);

    @TemplateParameter.GcsWriteFolder(
        order = 3,
        description = "Output file directory in Cloud Storage",
        helpText =
            "The path and filename prefix for writing output files. Must end with a slash. DateTime formatting is used to parse directory path for date & time formatters.")
    @Required
    ValueProvider<String> getOutputDirectory();

    void setOutputDirectory(ValueProvider<String> value);

    @TemplateParameter.GcsWriteFolder(
        order = 4,
        optional = true,
        description = "User provided temp location",
        helpText =
            "The user provided directory to output temporary files to. Must end with a slash.")
    ValueProvider<String> getUserTempLocation();

    void setUserTempLocation(ValueProvider<String> value);

    @TemplateParameter.Text(
        order = 5,
        description = "Output filename prefix of the files to write",
        helpText = "The prefix to place on each windowed file.")
    @Default.String("output")
    @Required
    ValueProvider<String> getOutputFilenamePrefix();

    void setOutputFilenamePrefix(ValueProvider<String> value);

    @TemplateParameter.Text(
        order = 6,
        optional = true,
        description = "Output filename suffix of the files to write",
        helpText =
            "The suffix to place on each windowed file. Typically a file extension such "
                + "as .txt or .csv.")
    @Default.String("")
    ValueProvider<String> getOutputFilenameSuffix();

    void setOutputFilenameSuffix(ValueProvider<String> value);
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

    PCollection<String> messages = null;

    /*
     * Steps:
     *   1) Read string messages from PubSub
     *   2) Window the messages into minute intervals specified by the executor.
     *   3) Output the windowed files to GCS
     */
    if (options.getUseSubscription()) {
      messages =
          pipeline.apply(
              "Read PubSub Events",
              PubsubIO.readStrings().fromSubscription(options.getInputSubscription()));
    } else {
      messages =
          pipeline.apply(
              "Read PubSub Events", PubsubIO.readStrings().fromTopic(options.getInputTopic()));
    }
    messages
        .apply(
            options.getWindowDuration() + " Window",
            Window.into(FixedWindows.of(DurationUtils.parseDuration(options.getWindowDuration()))))

        // Apply windowed file writes. Use a NestedValueProvider because the filename
        // policy requires a resourceId generated from the input value at runtime.
        .apply(
            "Write File(s)",
            TextIO.write()
                .withWindowedWrites()
                .withNumShards(options.getNumShards())
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
                    NestedValueProvider.of(
                        maybeUseUserTempLocation(
                            options.getUserTempLocation(), options.getOutputDirectory()),
                        (SerializableFunction<String, ResourceId>)
                            input -> FileBasedSink.convertToFileResourceIfPossible(input))));

    // Execute the pipeline and return the result.
    return pipeline.run();
  }

  /**
   * Utility method for using optional parameter userTempLocation as TempDirectory. This is useful
   * when output bucket is locked and temporary data cannot be deleted.
   *
   * @param userTempLocation user provided temp location
   * @param outputLocation user provided outputDirectory to be used as the default temp location
   * @return userTempLocation if available, otherwise outputLocation is returned.
   */
  private static ValueProvider<String> maybeUseUserTempLocation(
      ValueProvider<String> userTempLocation, ValueProvider<String> outputLocation) {
    return DualInputNestedValueProvider.of(
        userTempLocation,
        outputLocation,
        new SerializableFunction<TranslatorInput<String, String>, String>() {
          @Override
          public String apply(TranslatorInput<String, String> input) {
            return (input.getX() != null) ? input.getX() : input.getY();
          }
        });
  }
}
