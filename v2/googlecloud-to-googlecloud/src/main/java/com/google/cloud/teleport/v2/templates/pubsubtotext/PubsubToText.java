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
package com.google.cloud.teleport.v2.templates.pubsubtotext;

import com.google.cloud.teleport.v2.io.WindowedFilenamePolicy;
import com.google.cloud.teleport.v2.options.WindowedFilenamePolicyOptions;
import com.google.cloud.teleport.v2.utils.DurationUtils;
import com.google.common.base.Strings;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.options.Validation.Required;
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
 * export PROJECT=<project id>
 * export TEMPLATE_MODULE=googlecloud-to-googlecloud
 * export TEMPLATE_NAME=pubsub-to-text
 * export BUCKET_NAME=gs://<bucket name>
 * export TARGET_GCR_IMAGE=gcr.io/${PROJECT}/${TEMPLATE_NAME}-image
 * export BASE_CONTAINER_IMAGE=gcr.io/dataflow-templates-base/java11-template-launcher-base
 * export BASE_CONTAINER_IMAGE_VERSION=latest
 * export APP_ROOT=/template/${TEMPLATE_NAME}
 * export COMMAND_SPEC=${APP_ROOT}/resources/${TEMPLATE_NAME}-command-spec.json
 * export TEMPLATE_IMAGE_SPEC=${BUCKET_NAME}/images/${TEMPLATE_NAME}-image-spec.json
 *
 * gcloud config set project ${PROJECT}
 *
 * # Build and push image to Google Container Repository
 * mvn package \
 *   -Dimage=${TARGET_GCR_IMAGE} \
 *   -Dbase-container-image=${BASE_CONTAINER_IMAGE} \
 *   -Dbase-container-image.version=${BASE_CONTAINER_IMAGE_VERSION} \
 *   -Dapp-root=${APP_ROOT} \
 *   -Dcommand-spec=${COMMAND_SPEC} \
 *   -Djib.applicationCache=/tmp/jib-cache \
 *   -am -pl ${TEMPLATE_MODULE}
 *
 * # Create and upload image spec
 * echo '{
 *  "image":"'${TARGET_GCR_IMAGE}'",
 *  "metadata":{
 *    "name":"Pub/Sub to text",
 *    "description":"Write Pub/Sub messages to GCS text files.",
 *    "parameters":[
 *        {
 *            "name":"inputSubscription",
 *            "label":"Pub/Sub subscription to read from",
 *            "paramType":"TEXT",
 *            "isOptional":true
 *        },
 *        {
 *            "name":"inputTopic",
 *            "label":"Pub/Sub topic to read from",
 *            "paramType":"TEXT",
 *            "isOptional":true
 *        },
 *        {
 *            "name":"outputDirectory",
 *            "label":"Directory to output files to",
 *            "paramType":"TEXT",
 *            "isOptional":false
 *        },
 *        {
 *            "name":"outputFilenamePrefix",
 *            "label":"The filename prefix of the files to write to",
 *            "paramType":"TEXT",
 *            "isOptional":false
 *        },
 *        {
 *            "name":"outputFilenameSuffix",
 *            "label":"The suffix of the files to write to",
 *            "paramType":"TEXT",
 *            "isOptional":true
 *        },
 *        {
 *            "name":"userTempLocation",
 *            "label":"The directory to output temporary files to",
 *            "paramType":"TEXT",
 *            "isOptional":true
 *        }
 *    ]
 *  },
 *  "sdk_info":{"language":"JAVA"}
 * }' > image_spec.json
 * gsutil cp image_spec.json ${TEMPLATE_IMAGE_SPEC}
 * rm image_spec.json
 *
 * # Run template
 * export JOB_NAME="${TEMPLATE_MODULE}-`date +%Y%m%d-%H%M%S-%N`"
 * gcloud beta dataflow flex-template run ${JOB_NAME} \
 *       --project=${PROJECT} --region=us-central1 \
 *       --template-file-gcs-location=${TEMPLATE_IMAGE_SPEC} \
 *       --parameters inputTopic=<topic>,outputDirectory=<directory>,outputFilenamePrefix=<prefix>
 * </pre>
 */
public class PubsubToText {

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
    String getInputSubscription();

    void setInputSubscription(String value);

    @Description("The Cloud Pub/Sub topic to read from.")
    String getInputTopic();

    void setInputTopic(String value);

    @Description("The directory to output files to. Must end with a slash.")
    @Required
    String getOutputDirectory();

    void setOutputDirectory(String value);

    @Description("The directory to output temporary files to. Must end with a slash.")
    String getUserTempLocation();

    void setUserTempLocation(String value);

    @Description("The filename prefix of the files to write to.")
    @Default.String("output")
    @Required
    String getOutputFilenamePrefix();

    void setOutputFilenamePrefix(String value);

    @Description("The suffix of the files to write.")
    @Default.String("")
    String getOutputFilenameSuffix();

    void setOutputFilenameSuffix(String value);
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
    boolean useInputSubscription = !Strings.isNullOrEmpty(options.getInputSubscription());
    boolean useInputTopic = !Strings.isNullOrEmpty(options.getInputTopic());
    if (useInputSubscription == useInputTopic) {
      throw new IllegalArgumentException(
          "Either input topic or input subscription must be provided, but not both.");
    }

    // Create the pipeline
    Pipeline pipeline = Pipeline.create(options);

    PCollection<String> messages = null;

    /*
     * Steps:
     *   1) Read string messages from PubSub
     *   2) Window the messages into minute intervals specified by the executor.
     *   3) Output the windowed files to GCS
     */
    if (useInputSubscription) {
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
                    FileBasedSink.convertToFileResourceIfPossible(
                        maybeUseUserTempLocation(
                            options.getUserTempLocation(), options.getOutputDirectory()))));

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
  private static String maybeUseUserTempLocation(String userTempLocation, String outputLocation) {
    return !Strings.isNullOrEmpty(userTempLocation) ? userTempLocation : outputLocation;
  }
}
