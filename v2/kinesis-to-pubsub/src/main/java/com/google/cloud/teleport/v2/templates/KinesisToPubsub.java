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

import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.google.cloud.secretmanager.v1.SecretVersionName;
import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.v2.options.KinesisToPubsubOptions;
import com.google.cloud.teleport.v2.transforms.KinesisDataTransforms;
import com.google.cloud.teleport.v2.utils.SecretManagerUtils;
import javax.annotation.Nonnull;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.kinesis.KinesisIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link KinesisToPubsub} is a streaming pipeline which reads data from AWS Kinesis Data stream
 * and publishes the same to a Pub/Sub Topic. The input Kinesis data stream and output Pub/Sub topic
 * are both provided by the user.<br>
 * The AWS client maybe connected using access key and AWS secret key, and both are assumed to be
 * stored and protected as secrets in GCP secret manager. This pipeline expects the user to provide
 * the secret key id and secret version of both the AWS keys, assuming that the secrets have been
 * created in the project in which this pipeline will be executed. The service accounts running this
 * pipeline must also have the permissions needed to access and extract the secret values.<br>
 *
 * <p><b>Pipeline Requirements</b> * *
 *
 * <ul>
 *   <li>First Secret ID containing AWS Key ID
 *   <li>Second Secret ID containing AWS Secret Access Key
 *   <li>AWS Region
 *   <li>Data format in which data will be sent
 *   <li>Name of the Kinesis Data stream
 *   <li>Name of the Pub/Sub Topic to which data needs to be written
 * </ul>
 *
 * <p>Check out <a
 * href="https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/v2/kinesis-to-pubsub/README_Kinesis_To_Pubsub.md">README</a>
 * for instructions on how to use or modify this template.
 */
@Template(
    name = "Kinesis_To_Pubsub",
    category = TemplateCategory.STREAMING,
    displayName = "Kinesis To Pubsub",
    description = "A pipeline which sends Kinesis Datastream records into a Pubsub topic.",
    optionsClass = KinesisToPubsubOptions.class,
    flexContainerName = "kinesis-to-pubsub")
public class KinesisToPubsub {
  private static final Logger logger = LoggerFactory.getLogger(KinesisToPubsub.class);

  /**
   * The main entry-point for pipeline execution.
   *
   * @param args command-line args passed by the executor.
   */
  public static void main(String[] args) {
    KinesisToPubsubOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(KinesisToPubsubOptions.class);
    run(options);
  }

  /**
   * Validation of options.
   *
   * @param options the execution options.
   */
  public static void validateOptions(KinesisToPubsubOptions options) {
    if (options.getKinesisDataStream().isBlank()
        || options.getSecretId1().isBlank()
        || options.getSecretId2().isBlank()) {
      throw new IllegalArgumentException(
          "No information to retrieve Kinesis credentials was provided.");
    }

    if (!(SecretVersionName.isParsableFrom(options.getSecretId1()))) {
      throw new IllegalArgumentException(
          "Provided Secret must be in the form"
              + " projects/{project}/secrets/{secret}/versions/{secret_version}");
    }

    if (!(SecretVersionName.isParsableFrom(options.getSecretId2()))) {
      throw new IllegalArgumentException(
          "Provided Secret must be in the form"
              + " projects/{project}/secrets/{secret}/versions/{secret_version}");
    }
  }

  /**
   * Runs a pipeline which reads message from Kinesis and writes them to Pubsub.
   *
   * @param options the execution options.
   * @return the pipeline result.
   */
  public static PipelineResult run(@Nonnull KinesisToPubsubOptions options) {
    // Validate the options
    validateOptions(options);

    String awsKeyId = SecretManagerUtils.getSecret(options.getSecretId1());
    String awsSecretAccessKey = SecretManagerUtils.getSecret(options.getSecretId2());

    // Get the name of the kinesis data stream
    String kinesisDatastreamName = options.getKinesisDataStream();

    // Create the pipeline
    Pipeline pipeline = Pipeline.create(options);

    /*
     * Steps:
     *  1) Read messages in from Kinesis
     *  2) Extract the string message using transform
     *  3) Write records to Pub/Sub
     */
    pipeline
        .apply(
            "Read Kinesis Datastream",
            KinesisIO.read()
                .withStreamName(kinesisDatastreamName)
                .withAWSClientsProvider(
                    awsKeyId, awsSecretAccessKey, Regions.fromName(options.getAwsRegion()))
                .withInitialPositionInStream(InitialPositionInStream.LATEST))
        .apply("Extract String message", ParDo.of(new KinesisDataTransforms.ExtractStringFn()))
        .apply("PubsubSink", PubsubIO.writeStrings().to(options.getOutputPubsubTopic()));

    return pipeline.run();
  }
}
