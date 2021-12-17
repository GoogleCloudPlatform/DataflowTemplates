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

import com.google.cloud.teleport.avro.AvroPubsubMessageRecord;
import com.google.cloud.teleport.io.WindowedFilenamePolicy;
import com.google.cloud.teleport.options.WindowedFilenamePolicyOptions;
import com.google.cloud.teleport.util.DurationUtils;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.NestedValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
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
 *      "namespace": "com.google.cloud.teleport.avro",
 *      "fields": [
 *        {"name": "message", "type": {"type": "array", "items": "bytes"}},
 *        {"name": "attributes", "type": {"type": "map", "values": "string"}},
 *        {"name": "timestamp", "type": "long"}
 *      ]
 *   }
 * </pre>
 *
 * <p>Example Usage:
 *
 * <pre>
 * # Set the pipeline vars
 * PIPELINE_NAME=PubsubToAvro
 * PROJECT_ID=PROJECT ID HERE
 * PIPELINE_BUCKET=TEMPLATE STORAGE BUCKET NAME HERE
 * OUTPUT_BUCKET=JOB OUTPUT BUCKET NAME HERE
 * USE_SUBSCRIPTION=true or false depending on whether the pipeline should read
 *                  from a Pub/Sub Subscription or a Pub/Sub Topic.
 * PIPELINE_FOLDER=gs://${PIPELINE_BUCKET}/dataflow/pipelines/pubsub-to-gcs-avro
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
 * windowDuration=5m,\
 * numShards=1,\
 * userTempLocation=gs://${OUTPUT_BUCKET}/tmp/,\
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
 * outputFilenameSuffix=.avro"
 * </pre>
 */
public class PubsubToAvro {

  /**
   * Options supported by the pipeline.
   *
   * <p>Inherits standard configuration options.
   */
  public interface Options
      extends PipelineOptions, StreamingOptions, WindowedFilenamePolicyOptions {
    @Description(
        "The Cloud Pub/Sub subscription to consume from. "
            + "The name should be in the format of "
            + "projects/<project-id>/subscriptions/<subscription-name>.")
    ValueProvider<String> getInputSubscription();

    void setInputSubscription(ValueProvider<String> value);

    @Description("The Cloud Pub/Sub topic to read from.")
    ValueProvider<String> getInputTopic();

    void setInputTopic(ValueProvider<String> value);

    @Description(
        "This determines whether the template reads from " + "a pub/sub subscription or a topic")
    @Default.Boolean(false)
    Boolean getUseSubscription();

    void setUseSubscription(Boolean value);

    @Description("The directory to output files to. Must end with a slash.")
    @Required
    ValueProvider<String> getOutputDirectory();

    void setOutputDirectory(ValueProvider<String> value);

    @Description("The filename prefix of the files to write to.")
    @Default.String("output")
    ValueProvider<String> getOutputFilenamePrefix();

    void setOutputFilenamePrefix(ValueProvider<String> value);

    @Description("The suffix of the files to write.")
    @Default.String("")
    ValueProvider<String> getOutputFilenameSuffix();

    void setOutputFilenameSuffix(ValueProvider<String> value);

    @Description("The Avro Write Temporary Directory. Must end with /")
    @Required
    ValueProvider<String> getAvroTempDirectory();

    void setAvroTempDirectory(ValueProvider<String> value);
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

    if (options.getUseSubscription()) {
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
            Window.into(FixedWindows.of(DurationUtils.parseDuration(options.getWindowDuration()))))

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
                    NestedValueProvider.of(
                        options.getAvroTempDirectory(),
                        (SerializableFunction<String, ResourceId>)
                            input -> FileBasedSink.convertToFileResourceIfPossible(input)))
                /*.withTempDirectory(FileSystems.matchNewResource(
                options.getAvroTempDirectory(),
                Boolean.TRUE))
                */
                .withWindowedWrites()
                .withNumShards(options.getNumShards()));

    // Execute the pipeline and return the result.
    return pipeline.run();
  }

  /**
   * Converts an incoming {@link PubsubMessage} to the {@link AvroPubsubMessageRecord} class by
   * copying it's fields and the timestamp of the message.
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
