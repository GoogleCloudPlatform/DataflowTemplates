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

import com.google.cloud.teleport.io.WindowedFilenamePolicy;
import com.google.cloud.teleport.util.DurationUtils;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.TextIO;
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

/**
 * <p>
 * This pipeline ingests incoming data from a Cloud Pub/Sub topic and outputs
 * the raw data into windowed files at the specified output directory. By
 * default, the pipeline receives each PubsubMessage, interprets the data as a
 * String, and writes the String to a file. Using the option
 * {@code --format=json}, the pipeline receives each PubsubMessage, serializes
 * the PubsubMessage to a JSON String, and appends the JSON String to a
 * newline-delimited JSON file. The JSON string contains a {@code data}
 * attribute (a string that is some set of Base64 encoded bytes) and an
 * optional {@code attributes} object. The {@code attributes} object contains
 * a set of arbitrary attribute-string pairs.
 * </p>
 *
 * <p>
 * Example String Usage:
 * </p>
 *
 * <pre>
 * mvn compile exec:java \
 -Dexec.mainClass=com.google.cloud.teleport.templates.PubsubToText \
 -Dexec.cleanupDaemonThreads=false \
 -Dexec.args=" \
 --project=${PROJECT_ID} \
 --stagingLocation=gs://${BUCKET}/PubsubToText/staging \
 --tempLocation=gs://${BUCKET}/PubsubToText/temp \
 --runner=DataflowRunner \
 --windowDuration=2m \
 --numShards=1 \
 --inputTopic=projects/${PROJECT_ID}/topics/PubsubToText \
 --outputDirectory=gs://${BUCKET}/PubsubToText/output/ \
 --outputFilenamePrefix=windowed-file \
 --outputFilenameSuffix=.txt"
 * </pre>
 *
 * <p>
 * Example JSON Usage:
 * </p>
 *
 * <pre>
 * mvn compile exec:java \
 -Dexec.mainClass=com.google.cloud.teleport.templates.PubsubToText \
 -Dexec.cleanupDaemonThreads=false \
 -Dexec.args=" \
 --project=${PROJECT_ID} \
 --stagingLocation=gs://${BUCKET}/PubsubToText/staging \
 --tempLocation=gs://${BUCKET}/PubsubToText/temp \
 --runner=DataflowRunner \
 --windowDuration=2m \
 --numShards=1 \
 --inputTopic=projects/${PROJECT_ID}/topics/PubsubToText \
 --format=json \
 --outputDirectory=gs://${BUCKET}/PubsubToText/output/ \
 --outputFilenamePrefix=windowed-file \
 --outputFilenameSuffix=.json"
 * </pre>
 *
 * <pre>
 * gcloud \
 --project "${PROJECT_ID}" \
 pubsub \
 topics \
 publish \
 PubsubToText \
 --attribute=foo=bar \
 --message="Hello, world."
 *
 * gcloud \
 --project "${PROJECT_ID}" \
 pubsub \
 topics \
 publish \
 PubsubToText \
 --attribute=foo=baz \
 --message="Goodbye, world."
 * </pre>
 *
 * <p>
 * gs://${BUCKET}/PubsubToText/output/windowed-file....json
 * </p>
 *
 * <pre>
 * {"attributes":{"foo":"bar"},"data":"SGVsbG8sIHdvcmxkLg"}
 * {"attributes":{"foo":"baz"},"data":"R29vZGJ5ZSwgd29ybGQu"}
 * </pre>
 *
 * <p>
 * Decode data:
 * </p>
 *
 * <pre>
 * $ echo SGVsbG8sIHdvcmxkLg== | base64 -d
 * Hello, world.
 *
 * $ echo R29vZGJ5ZSwgd29ybGQu | base64 -d
 * Goodbye, world.
 * </pre>
 */
public class PubsubToText {

  /**
   * Options supported by the pipeline.
   *
   * <p>Inherits standard configuration options.</p>
   */
  public interface Options extends PipelineOptions, StreamingOptions {
    @Description("The Cloud Pub/Sub topic to read from.")
    @Required
    ValueProvider<String> getInputTopic();
    void setInputTopic(ValueProvider<String> value);

    @Description("The directory to output files to. Must end with a slash.")
    @Required
    ValueProvider<String> getOutputDirectory();
    void setOutputDirectory(ValueProvider<String> value);

    @Description("The filename prefix of the files to write to.")
    @Default.String("output")
    @Required
    ValueProvider<String> getOutputFilenamePrefix();
    void setOutputFilenamePrefix(ValueProvider<String> value);

    @Description("The suffix of the files to write.")
    @Default.String("")
    ValueProvider<String> getOutputFilenameSuffix();
    void setOutputFilenameSuffix(ValueProvider<String> value);

    @Description("The shard template of the output file. Specified as repeating sequences "
        + "of the letters 'S' or 'N' (example: SSS-NNN). These are replaced with the "
        + "shard number, or number of shards respectively")
    @Default.String("W-P-SS-of-NN")
    ValueProvider<String> getOutputShardTemplate();
    void setOutputShardTemplate(ValueProvider<String> value);

    @Description("The maximum number of output shards produced when writing.")
    @Default.Integer(1)
    Integer getNumShards();
    void setNumShards(Integer value);

    @Description("The window duration in which data will be written. Defaults to 5m. "
        + "Allowed formats are: "
        + "Ns (for seconds, example: 5s), "
        + "Nm (for minutes, example: 12m), "
        + "Nh (for hours, example: 2h).")
    @Default.String("5m")
    String getWindowDuration();
    void setWindowDuration(String value);

    @Description("Output file format (e.g., string, json).")
    @Default.String("string")
    String getFormat();
    void setFormat(String value);
  }

  /**
   * Main entry point for executing the pipeline.
   * @param args  The command-line arguments to the pipeline.
   */
  public static void main(String[] args) {

    Options options = PipelineOptionsFactory
        .fromArgs(args)
        .withValidation()
        .as(Options.class);

    options.setStreaming(true);

    run(options);
  }

  /**
   * Runs the pipeline with the supplied options.
   *
   * @param options The execution parameters to the pipeline.
   * @return  The result of the pipeline execution.
   */
  public static PipelineResult run(Options options) {
    // Create the pipeline
    Pipeline pipeline = Pipeline.create(options);

    String format = options.getFormat();

    switch (format) {
      case "string":
        /*
         * Steps:
         *   1) Read string messages from PubSub
         *   2) Window the messages into minute intervals specified by the executor.
         *   3) Output the windowed files to GCS
         */
        pipeline
            .apply("Read PubSub Events", PubsubIO.readStrings().fromTopic(options.getInputTopic()))
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
                        new WindowedFilenamePolicy(
                            options.getOutputDirectory(),
                            options.getOutputFilenamePrefix(),
                            options.getOutputShardTemplate(),
                            options.getOutputFilenameSuffix()))
                    .withTempDirectory(NestedValueProvider.of(
                        options.getOutputDirectory(),
                        (SerializableFunction<String, ResourceId>) input ->
                            FileBasedSink.convertToFileResourceIfPossible(input))));

        break;
      case "json":
        /*
         * Steps:
         *   1) Read PubsubMessages from PubSub
         *   2) Serialize PubsubMessage to JSON string
         *   3) Window the messages into minute intervals specified by the executor.
         *   4) Output the windowed files to GCS
         */
        pipeline
            .apply("Read PubSub Events", PubsubIO.readMessagesWithAttributes().fromTopic(options.getInputTopic()))
            .apply("Serialize PubsubMessage to JSON", ParDo.of(new PubsubMessageToJson()))
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
                        new WindowedFilenamePolicy(
                            options.getOutputDirectory(),
                            options.getOutputFilenamePrefix(),
                            options.getOutputShardTemplate(),
                            options.getOutputFilenameSuffix()))
                    .withTempDirectory(NestedValueProvider.of(
                        options.getOutputDirectory(),
                        (SerializableFunction<String, ResourceId>) input ->
                            FileBasedSink.convertToFileResourceIfPossible(input))));

        break;
    }

    // Execute the pipeline and return the result.
    return pipeline.run();
  }

  static class PubsubMessageToJson extends DoFn<PubsubMessage, String> {

    private static final Gson GSON =
        new GsonBuilder()
            .disableHtmlEscaping()
            .create()
        ;

    @ProcessElement
    public void processElement(@Element PubsubMessage pubsubMessage, OutputReceiver<String> out) {

      String string =
          GSON.toJson(
              new com.google.api.services.pubsub.model.PubsubMessage()
                  .encodeData(
                      pubsubMessage.getPayload()
                  )
                  .setAttributes(
                      pubsubMessage.getAttributeMap()
                  )
          );

      out.output(string);
    }
  }
}
