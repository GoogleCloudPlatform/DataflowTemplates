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

import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.metadata.TemplateParameter;
import com.google.cloud.teleport.v2.common.UncaughtExceptionLogger;
import com.google.cloud.teleport.v2.io.WindowedFilenamePolicy;
import com.google.cloud.teleport.v2.options.WindowedFilenamePolicyOptions;
import com.google.cloud.teleport.v2.templates.pubsubtotext.PubsubToText.Options;
import com.google.cloud.teleport.v2.utils.DurationUtils;
import com.google.common.base.Strings;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Default;
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
 * export PROJECT={project id}
 * export TEMPLATE_MODULE=googlecloud-to-googlecloud
 * export TEMPLATE_NAME=pubsub-to-text
 * export BUCKET_NAME=gs://{bucket name}
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
 *       --parameters inputTopic={topic},outputDirectory={directory},outputFilenamePrefix={prefix}
 * </pre>
 */
@Template(
    name = "Cloud_PubSub_to_GCS_Text_Flex",
    category = TemplateCategory.STREAMING,
    displayName = "Pub/Sub Subscription or Topic to Text Files on Cloud Storage",
    description =
        "Streaming pipeline. Reads records from Pub/Sub Subscription or Topic and writes them to"
            + " Cloud Storage, creating a text file for each five minute window. Note that this"
            + " pipeline assumes no newlines in the body of the Pub/Sub message and thus each"
            + " message becomes a single line in the output file.",
    optionsClass = Options.class,
    flexContainerName = "pubsub-to-text",
    documentation =
        "https://cloud.google.com/dataflow/docs/guides/templates/provided/pubsub-topic-subscription-to-text",
    contactInformation = "https://cloud.google.com/support")
public class PubsubToText {

  /**
   * Options supported by the pipeline.
   *
   * <p>Inherits standard configuration options.
   */
  public interface Options
      extends PipelineOptions, StreamingOptions, WindowedFilenamePolicyOptions {

    @TemplateParameter.PubsubTopic(
        order = 1,
        optional = true,
        description = "Pub/Sub input topic",
        helpText =
            "Pub/Sub topic to read the input from, in the format of "
                + "'projects/your-project-id/topics/your-topic-name'",
        example = "projects/your-project-id/topics/your-topic-name")
    String getInputTopic();

    void setInputTopic(String value);

    @TemplateParameter.PubsubSubscription(
        order = 2,
        optional = true,
        description = "Pub/Sub input subscription",
        helpText =
            "Pub/Sub subscription to read the input from, in the format of"
                + " 'projects/your-project-id/subscriptions/your-subscription-name'",
        example = "projects/your-project-id/subscriptions/your-subscription-name")
    String getInputSubscription();

    void setInputSubscription(String value);

    @TemplateParameter.GcsWriteFolder(
        order = 3,
        description = "Output file directory in Cloud Storage",
        helpText =
            "The path and filename prefix for writing output files. Must end with a slash. DateTime"
                + " formatting is used to parse directory path for date & time formatters.",
        example = "gs://your-bucket/your-path")
    @Required
    String getOutputDirectory();

    void setOutputDirectory(String value);

    @TemplateParameter.GcsWriteFolder(
        order = 4,
        optional = true,
        description = "User provided temp location",
        helpText =
            "The user provided directory to output temporary files to. Must end with a slash.")
    String getUserTempLocation();

    void setUserTempLocation(String value);

    @TemplateParameter.Text(
        order = 5,
        optional = true,
        description = "Output filename prefix of the files to write",
        helpText = "The prefix to place on each windowed file.",
        example = "output-")
    @Default.String("output")
    @Required
    String getOutputFilenamePrefix();

    void setOutputFilenamePrefix(String value);

    @TemplateParameter.Text(
        order = 6,
        optional = true,
        description = "Output filename suffix of the files to write",
        helpText =
            "The suffix to place on each windowed file. Typically a file extension such "
                + "as .txt or .csv.",
        example = ".txt")
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
    UncaughtExceptionLogger.register();

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

        // Apply windowed file writes
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
