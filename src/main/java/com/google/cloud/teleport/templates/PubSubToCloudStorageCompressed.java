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
import com.google.cloud.teleport.util.DualInputNestedValueProvider;
import com.google.cloud.teleport.util.DualInputNestedValueProvider.TranslatorInput;
import com.google.cloud.teleport.util.DurationUtils;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.Compression;
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

/**
 * This pipeline ingests incoming data from a Cloud Pub/Sub subscription and
 * outputs the compressed data into windowed files at the specified output
 * directory.
 *
 * <p> Example Usage:
 *
 * <p>
 * <pre>
 * PROJECT_ID="project-id"
 * JOB_NAME="pubsub-to-cloudstorage"
 * PIPELINE_FOLDER="gs://${PROJECT_ID}/dataflow/pipelines/${JOB_NAME}"
 * mvn compile exec:java \
 *  -Dexec.mainClass=com.thecontrolgroup.events.templates.PubSubToCloudStorageCompressed \
 *  -Dexec.cleanupDaemonThreads=false \
 *  -Dexec.args=" \
 *  --templateLocation=gs://${PROJECT_ID}/dataflow/templates/${JOB_NAME} \
 *  --project=${PROJECT_ID} \
 *  --stagingLocation=${PIPELINE_FOLDER}/staging \
 *  --tempLocation=${PIPELINE_FOLDER}/temp \
 *  --runner=DataflowRunner \
 *  --windowDuration=1m \
 *  --numShards=1 \
 *  --inputSubscription=projects/${PROJECT_ID}/subscriptions/${JOB_NAME} \
 *  --userTempLocation=gs://${PROJECT_ID}/tmp/ \
 *  --outputDirectory=gs://${PROJECT_ID}/archive/YYYY/MM/DD/HH/ \
 *  --outputFilenamePrefix=events- \
 *  --outputFilenameSuffix=.ndjson \
 *  --compression=GZIP \
 *  --region=us-east4"
 * </pre> 
 * </p>
 */
public class PubSubToCloudStorageCompressed {

  /**
   * Options supported by the pipeline.
   *
   * <p>Inherits standard configuration options.</p>
   */
  public interface Options extends PipelineOptions, StreamingOptions {
    @Description("The Cloud Pub/Sub subscription to read from.")
    @Required
    ValueProvider<String> getInputSubscription();
    void setInputSubscription(ValueProvider<String> value);

    @Description("The directory to output files to. Must end with a slash.")
    @Required
    ValueProvider<String> getOutputDirectory();
    void setOutputDirectory(ValueProvider<String> value);

    @Description("The directory to output temporary files to. Must end with a slash.")
    ValueProvider<String> getUserTempLocation();
    void setUserTempLocation(ValueProvider<String> value);

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

    @Description("The compression algorithm to use on the matched files.")
    @Required
    Compression getCompression();

    void setCompression(Compression value);
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

    /*
     * Steps:
     *   1) Read string messages from PubSub
     *   2) Window the messages into minute intervals specified by the executor.
     *   3) Output the windowed files to GCS
     */
    pipeline
        .apply("ReadPubSubSubscription", PubsubIO.readStrings().fromSubscription(options.getInputSubscription()))
        .apply(
            options.getWindowDuration() + " Window",
            Window.into(FixedWindows.of(DurationUtils.parseDuration(options.getWindowDuration()))))

        // Apply windowed file writes. Use a NestedValueProvider because the filename
        // policy requires a resourceId generated from the input value at runtime.
        .apply(
            "Write File(s)",
            TextIO.write()
                .withWindowedWrites()
                .withCompression(options.getCompression())
                .withNumShards(options.getNumShards())
                .to(
                    new WindowedFilenamePolicy(
                        options.getOutputDirectory(),
                        options.getOutputFilenamePrefix(),
                        options.getOutputShardTemplate(),
                        options.getOutputFilenameSuffix()))
                .withTempDirectory(NestedValueProvider.of(
                    maybeUseUserTempLocation(
                        options.getUserTempLocation(),
                        options.getOutputDirectory()),
                    (SerializableFunction<String, ResourceId>) input ->
                        FileBasedSink.convertToFileResourceIfPossible(input))));

    // Execute the pipeline and return the result.
    return pipeline.run();
  }

  /**
   * Utility method for using optional parameter userTempLocation as TempDirectory.
   * This is useful when output bucket is locked and temporary data cannot be deleted.
   *
   * @param userTempLocation user provided temp location
   * @param outputLocation user provided outputDirectory to be used as the default temp location
   * @return userTempLocation if available, otherwise outputLocation is returned.
   */
  private static ValueProvider<String> maybeUseUserTempLocation(
      ValueProvider<String> userTempLocation,
      ValueProvider<String> outputLocation) {
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