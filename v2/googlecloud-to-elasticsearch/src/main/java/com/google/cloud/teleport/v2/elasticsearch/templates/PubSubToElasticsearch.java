/*
 * Copyright (C) 2021 Google LLC
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
package com.google.cloud.teleport.v2.elasticsearch.templates;

import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.v2.coders.FailsafeElementCoder;
import com.google.cloud.teleport.v2.common.UncaughtExceptionLogger;
import com.google.cloud.teleport.v2.elasticsearch.options.PubSubToElasticsearchOptions;
import com.google.cloud.teleport.v2.elasticsearch.transforms.FailedPubsubMessageToPubsubTopicFn;
import com.google.cloud.teleport.v2.elasticsearch.transforms.ProcessEventMetadata;
import com.google.cloud.teleport.v2.elasticsearch.transforms.PubSubMessageToJsonDocument;
import com.google.cloud.teleport.v2.elasticsearch.transforms.WriteToElasticsearch;
import com.google.cloud.teleport.v2.elasticsearch.utils.ElasticsearchIndex;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessageWithAttributesCoder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link PubSubToElasticsearch} pipeline is a streaming pipeline which ingests data in JSON
 * format from PubSub, applies a Javascript UDF if provided and writes the resulting records to
 * Elasticsearch. If the element fails to be processed then it is written to an error output table
 * in BigQuery.
 *
 * <p>Check out <a
 * href="https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/v2/googlecloud-to-elasticsearch/README_PubSub_to_Elasticsearch.md">README</a>
 * for instructions on how to use or modify this template.
 */
@Template(
    name = "PubSub_to_Elasticsearch",
    category = TemplateCategory.STREAMING,
    displayName = "Pub/Sub to Elasticsearch",
    description = {
      "The Pub/Sub to Elasticsearch template is a streaming pipeline that reads messages from a Pub/Sub subscription, executes a user-defined function (UDF), and writes them to Elasticsearch as documents. "
          + "The Dataflow template uses Elasticsearch's <a href=\"https://www.elastic.co/guide/en/elasticsearch/reference/master/data-streams.html\">data streams</a> feature to store time series data across multiple indices while giving you a single named resource for requests. "
          + "Data streams are well-suited for logs, metrics, traces, and other continuously generated data stored in Pub/Sub.\n",
      "The template creates a datastream named <code>logs-gcp.DATASET-NAMESPACE</code>, where:\n"
          + "- <code>DATASET</code> is the value of the <code>dataset</code> template parameter, or <code>pubsub</code> if not specified.\n"
          + "- <code>NAMESPACE</code> is the value of the <code>namespace</code> template parameter, or <code>default</code> if not specified."
    },
    optionsClass = PubSubToElasticsearchOptions.class,
    skipOptions = "index", // Template just ignores what is sent as "index"
    flexContainerName = "pubsub-to-elasticsearch",
    documentation =
        "https://cloud.google.com/dataflow/docs/guides/templates/provided/pubsub-to-elasticsearch",
    contactInformation = "https://cloud.google.com/support",
    preview = true,
    requirements = {
      "The source Pub/Sub subscription must exist and the messages must be encoded in a valid JSON format.",
      "A publicly reachable Elasticsearch host on a Google Cloud instance or on Elastic Cloud with Elasticsearch version 7.0 or above. See <a href=\"https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/v2/googlecloud-to-elasticsearch/docs/PubSubToElasticsearch/README.md#google-cloud-integration-for-elastic\">Google Cloud Integration for Elastic</a> for more details.",
      "A Pub/Sub topic for error output.",
    })
public class PubSubToElasticsearch {

  /** The tag for the main output of the json transformation. */
  public static final TupleTag<FailsafeElement<PubsubMessage, String>> TRANSFORM_OUT =
      new TupleTag<FailsafeElement<PubsubMessage, String>>() {};

  /** The tag for the error output table of the json to table row transform. */
  public static final TupleTag<FailsafeElement<PubsubMessage, String>> TRANSFORM_ERROR_OUTPUT_OUT =
      new TupleTag<FailsafeElement<PubsubMessage, String>>() {};

  /** Pubsub message/string coder for pipeline. */
  public static final FailsafeElementCoder<PubsubMessage, String> CODER =
      FailsafeElementCoder.of(PubsubMessageWithAttributesCoder.of(), StringUtf8Coder.of());

  /** String/String Coder for FailsafeElement. */
  public static final FailsafeElementCoder<String, String> FAILSAFE_ELEMENT_CODER =
      FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of());

  /** The log to output status messages to. */
  private static final Logger LOG = LoggerFactory.getLogger(PubSubToElasticsearch.class);

  /**
   * Main entry point for executing the pipeline.
   *
   * @param args The command-line arguments to the pipeline.
   */
  public static void main(String[] args) {
    UncaughtExceptionLogger.register();

    // Parse the user options passed from the command-line.
    PubSubToElasticsearchOptions pubSubToElasticsearchOptions =
        PipelineOptionsFactory.fromArgs(args)
            .withValidation()
            .as(PubSubToElasticsearchOptions.class);

    pubSubToElasticsearchOptions.setIndex(
        new ElasticsearchIndex(
                pubSubToElasticsearchOptions.getDataset(),
                pubSubToElasticsearchOptions.getNamespace())
            .getIndex());

    validateOptions(pubSubToElasticsearchOptions);
    run(pubSubToElasticsearchOptions);
  }

  public static void validateOptions(PubSubToElasticsearchOptions options) {
    switch (options.getApiKeySource()) {
      case "PLAINTEXT":
        return;
      case "KMS":
        // validate that the encryption key is provided.
        if (StringUtils.isEmpty(options.getApiKeyKMSEncryptionKey())) {
          throw new IllegalArgumentException(
              "If apiKeySource is set to KMS, apiKeyKMSEncryptionKey should be provided.");
        }
        return;
      case "SECRET_MANAGER":
        // validate that secretId is provided.
        if (StringUtils.isEmpty(options.getApiKeySecretId())) {
          throw new IllegalArgumentException(
              "If apiKeySource is set to SECRET_MANAGER, apiKeySecretId should be provided.");
        }
    }
  }

  /**
   * Runs the pipeline with the supplied options.
   *
   * @param options The execution parameters to the pipeline.
   * @return The result of the pipeline execution.
   */
  public static PipelineResult run(PubSubToElasticsearchOptions options) {

    // Create the pipeline
    Pipeline pipeline = Pipeline.create(options);

    // Register the coders for pipeline
    CoderRegistry coderRegistry = pipeline.getCoderRegistry();

    coderRegistry.registerCoderForType(
        FAILSAFE_ELEMENT_CODER.getEncodedTypeDescriptor(), FAILSAFE_ELEMENT_CODER);

    coderRegistry.registerCoderForType(CODER.getEncodedTypeDescriptor(), CODER);

    /*
     * Steps: 1) Read PubSubMessage with attributes from input PubSub subscription.
     *        2) Apply Javascript UDF if provided.
     *        3) Index Json string to output ES index.
     *
     */
    LOG.info("Reading from subscription: " + options.getInputSubscription());

    PCollectionTuple convertedPubsubMessages =
        pipeline
            /*
             * Step #1: Read from a PubSub subscription.
             */
            .apply(
                "ReadPubSubSubscription",
                PubsubIO.readMessagesWithAttributes()
                    .fromSubscription(options.getInputSubscription()))
            /*
             * Step #2: Transform the PubsubMessages into Json documents.
             */
            .apply(
                "ConvertMessageToJsonDocument",
                PubSubMessageToJsonDocument.newBuilder()
                    .setJavascriptTextTransformFunctionName(
                        options.getJavascriptTextTransformFunctionName())
                    .setJavascriptTextTransformGcsPath(options.getJavascriptTextTransformGcsPath())
                    .build());

    /*
     * Step #3a: Write Json documents into Elasticsearch using {@link ElasticsearchTransforms.WriteToElasticsearch}.
     */
    convertedPubsubMessages
        .get(TRANSFORM_OUT)
        .apply(
            "GetJsonDocuments",
            MapElements.into(TypeDescriptors.strings()).via(FailsafeElement::getPayload))
        .apply("Insert metadata", new ProcessEventMetadata())
        .apply(
            "WriteToElasticsearch",
            WriteToElasticsearch.newBuilder()
                .setOptions(options.as(PubSubToElasticsearchOptions.class))
                .build());

    /*
     * Step 3b: Write elements that failed processing to error output PubSub topic via {@link PubSubIO}.
     */
    convertedPubsubMessages
        .get(TRANSFORM_ERROR_OUTPUT_OUT)
        .apply(ParDo.of(new FailedPubsubMessageToPubsubTopicFn()))
        .apply("writeFailureMessages", PubsubIO.writeMessages().to(options.getErrorOutputTopic()));

    // Execute the pipeline and return the result.
    return pipeline.run();
  }
}
