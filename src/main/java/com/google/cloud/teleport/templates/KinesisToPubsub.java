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

import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.google.cloud.secretmanager.v1.AccessSecretVersionResponse;
import com.google.cloud.secretmanager.v1.SecretManagerServiceClient;
import com.google.common.base.Splitter;
import java.io.IOException;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.io.kinesis.KinesisIO;
import org.apache.beam.sdk.io.kinesis.KinesisRecord;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A template that copies messages from Kinesis to Pub/Sub with custom attributes.
 *
 * <p>Example Usage:
 *
 * <pre>
 * {@code mvn compile exec:java \
 * -Dexec.mainClass=com.google.cloud.teleport.templates.KinesisToPubsub \
 * -Dexec.args=" \
 * --project=${PROJECT_ID} \
 * --stagingLocation=gs://${STAGING_BUCKET}/dataflow/pipelines/${PIPELINE_FOLDER}/staging \
 * --tempLocation=gs://${STAGING_BUCKET}/dataflow/pipelines/${PIPELINE_FOLDER}/temp \
 * --runner=DataflowRunner/DirectRunner \
 * --AWSAccessKey=projects/${PROJECT_ID}/secrets/${AWS_ACCESS_KEY}/versions/${AWS_ACCESS_KEY_VERSION} \
 * --AWSSecretKey=projects/${PROJECT_ID}/secrets/${AWS_SECRET_KEY}/versions/${AWS_SECRET_KEY_VERSION} \
 * --AWSRegion=${AWS_REGION} \
 * --AWSEndpointURL=${AWS_ENDPOINT_URL} \
 * --AWSCBORFlag=${AWS_CBOR_FLAG} \
 * --AWSKinesisStreamName=${AWS_KINESIS_STREAM_NAME} \
 * --outputTopic=projects/${PROJECT_ID}/topics/${TOPIC_NAME} \
 * --customAttributes=key=value,key2=value2,..."
 * }
 * </pre>
 */
public class KinesisToPubsub {
  private static final Logger LOG = LoggerFactory.getLogger(KinesisToPubsub.class);

  /** The custom options supported by the pipeline. Inherits standard configuration options. */
  public interface Options extends PipelineOptions {
    @Description("AWS Access Key (from Secret Manager, in complete format)")
    @Required
    String getAWSAccessKey();

    void setAWSAccessKey(String value);

    @Description("AWS Secret Key (from Secret Manager, in complete format)")
    @Required
    String getAWSSecretKey();

    void setAWSSecretKey(String value);

    @Description("AWS Region")
    @Required
    String getAWSRegion();

    void setAWSRegion(String value);

    @Description("AWS Endpoint URL (optional)")
    String getAWSEndpointURL();

    void setAWSEndpointURL(String value);

    @Description("AWS CBOR Flag (default to FALSE)")
    String getAWSCBORFlag();

    void setAWSCBORFlag(String value);

    @Description("Kinesis input stream name")
    @Required
    String getAWSKinesisStreamName();

    void setAWSKinesisStreamName(String value);

    @Description("Pub/Sub topic name (in complete format)")
    @Required
    ValueProvider<String> getOutputTopic();

    void setOutputTopic(ValueProvider<String> value);

    @Description("Pub/Sub custom attributes (format: key=value,key2=value2,...)")
    @Required
    String getCustomAttributes();

    void setCustomAttributes(String value);
  }

  /** Main transformation logic here. */
  private static class RecordToPubsubMessageFn extends DoFn<KinesisRecord, PubsubMessage> {
    private Map<String, String> attributes;

    public RecordToPubsubMessageFn(Map<String, String> customAttributes) {
      attributes = customAttributes;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      PubsubMessage content = new PubsubMessage(c.element().getDataAsBytes(), attributes);

      c.output(content);
    }
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
    // Set AWS CBOR Flag (https://github.com/aws/aws-sdk-java/issues/2493)
    if (options.getAWSCBORFlag().equalsIgnoreCase("TRUE")) {
      System.setProperty(
          com.amazonaws.SDKGlobalConfiguration.AWS_CBOR_DISABLE_SYSTEM_PROPERTY,
          options.getAWSCBORFlag());
    }

    // Set attributes
    Map<String, String> customAttributes = stringToMap(options.getCustomAttributes());

    // Access secret manager
    String accessKey = getSecretValue(options.getAWSAccessKey());
    String secretKey = getSecretValue(options.getAWSSecretKey());

    // Create the pipeline.
    Pipeline pipeline = Pipeline.create(options);

    pipeline
        .apply(
            "Read from Kinesis",
            KinesisIO.read()
                .withAWSClientsProvider(
                    accessKey,
                    secretKey,
                    Regions.fromName(options.getAWSRegion()),
                    options.getAWSEndpointURL())
                .withStreamName(options.getAWSKinesisStreamName())
                .withInitialPositionInStream(InitialPositionInStream.LATEST))
        .apply(
            "KinesisRecord to PubsubMessage with custom attributes",
            ParDo.of(new RecordToPubsubMessageFn(customAttributes)))
        .apply("Write to PubSub", PubsubIO.writeMessages().to(options.getOutputTopic()));

    return pipeline.run();
  }

  private static Map<String, String> stringToMap(String string) {
    try {
      return Splitter.on(",").withKeyValueSeparator("=").split(string);
    } catch (Exception e) {
      LOG.error(
          "Found error with message: "
              + e.getMessage()
              + ". Replaced custom attributes with default key and value");
      return Map.of("default_key", "default_value");
    }
  }

  private static String getSecretValue(String secretName) {
    String secretValue = "";

    try (SecretManagerServiceClient client = SecretManagerServiceClient.create()) {
      AccessSecretVersionResponse response = client.accessSecretVersion(secretName);
      secretValue = response.getPayload().getData().toStringUtf8();
    } catch (IOException e) {
      LOG.error("Found error with message: " + e.getMessage() + ". Exiting the application");
      System.exit(0);
    }

    return secretValue;
  }
}
