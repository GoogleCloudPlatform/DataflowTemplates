/*
 * Copyright (C) 2021 Google Inc.
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
package com.google.cloud.teleport.v2.elasticsearch.templates;

import com.google.cloud.teleport.v2.coders.FailsafeElementCoder;
import com.google.cloud.teleport.v2.elasticsearch.options.ElasticsearchWriteOptions;
import com.google.cloud.teleport.v2.elasticsearch.options.PubSubToElasticsearchOptions;
import com.google.cloud.teleport.v2.elasticsearch.transforms.PubSubMessageToJsonDocument;
import com.google.cloud.teleport.v2.elasticsearch.transforms.WriteToElasticsearch;
import com.google.cloud.teleport.v2.transforms.ErrorConverters;
import com.google.cloud.teleport.v2.utils.SchemaUtils;
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
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link PubSubToElasticsearch} pipeline is a streaming pipeline which ingests data in JSON
 * format from PubSub, applies a Javascript UDF if provided and writes the resulting records to
 * Elasticsearch. If the element fails to be processed then it is written to a deadletter table in
 * BigQuery.
 *
 * <p><b>Pipeline Requirements</b>
 *
 * <ul>
 *   <li>The PubSub subscription exist
 *   <li>The Elasticsearch host(s) is/are up and running
 * </ul>
 *
 * <p><b>Example Usage</b>
 *
 * <pre>
 * # Set the pipeline vars
 * PROJECT_NAME=my-project
 * BUCKET_NAME=my-bucket
 * INPUT_SUBSCRIPTION=my-subscription
 * TEMPLATES_LAUNCH_API="${API_ROOT_URL}/v1b3/projects/${PROJECT_NAME}/templates:launch"
 * ELASTICSEARCH_INDEX_NAME=my-index
 * ELASTICSEARCH_HOSTNAME=my-host:port
 * DOCUMENT_TYPE=my-doc
 *
 * # Set containerization vars
 * IMAGE_NAME=my-image-name
 * TARGET_GCR_IMAGE=gcr.io/${PROJECT}/${IMAGE_NAME}
 * BASE_CONTAINER_IMAGE=my-base-container-image
 * BASE_CONTAINER_IMAGE_VERSION=my-base-container-image-version
 * APP_ROOT=/path/to/app-root
 * COMMAND_SPEC=/path/to/command-spec
 *
 * # Build and upload image
 * mvn clean package \
 * -Dimage=${TARGET_GCR_IMAGE} \
 * -Dbase-container-image=${BASE_CONTAINER_IMAGE} \
 * -Dbase-container-image.version=${BASE_CONTAINER_IMAGE_VERSION} \
 * -Dapp-root=${APP_ROOT} \
 * -Dcommand-spec=${COMMAND_SPEC}
 *
 * # Create an image spec in GCS that contains the path to the image
 * {
 *   "docker_template_spec": {
 *      "docker_image": $TARGET_GCR_IMAGE
 *    }
 * }
 *
 * # Execute template:
 * API_ROOT_URL="https://dataflow.googleapis.com"
 * TEMPLATES_LAUNCH_API="${API_ROOT_URL}/v1b3/projects/${PROJECT_NAME}/templates:launch"
 * JOB_NAME="pubsub-to-elasticsearch-`date +%Y%m%d-%H%M%S-%N`"
 *
 * time curl -X POST \
 *     -H "Content-Type: application/json" \
 *     -H "Authorization: Bearer $(gcloud auth print-access-token)" \
 *     "${TEMPLATES_LAUNCH_API}"`\
 *     `"?validateOnly=false"` \
 *     `"&dynamicTemplate.gcsPath=gs://${BUCKET_NAME}/pubsub-to-elasticsearch-image-spec.json"` \
 *     `"&dynamicTemplate.stagingLocation=gs://${BUCKET_NAME}/staging" \
 *     -d
 *     '{
 *         "jobName":"$JOB_NAME",
 *         "parameters": {
 *             "inputSubscription":"$INPUT_SUBSCRIPTION",
 *             "outputIndex":"$ELASTICSEARCH_INDEX_NAME",
 *             "elasticsearchAddresses":"$ELASTICSEARCH_HOSTNAME",
 *             "documentType":"$DOCUMENT_TYPE"
 *         }
 *     }'
 * </pre>
 */
public class PubSubToElasticsearch {

  /** The tag for the main output of the json transformation. */
  public static final TupleTag<FailsafeElement<PubsubMessage, String>> TRANSFORM_OUT =
      new TupleTag<FailsafeElement<PubsubMessage, String>>() {};

  /** The tag for the dead-letter output of the json to table row transform. */
  public static final TupleTag<FailsafeElement<PubsubMessage, String>> TRANSFORM_DEADLETTER_OUT =
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

    // Parse the user options passed from the command-line.
    PubSubToElasticsearchOptions pubSubToElasticsearchOptions =
        PipelineOptionsFactory.fromArgs(args)
            .withValidation()
            .as(PubSubToElasticsearchOptions.class);

    run(pubSubToElasticsearchOptions);
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
        .apply(
            "WriteToElasticsearch",
            WriteToElasticsearch.newBuilder()
                .setOptions(options.as(ElasticsearchWriteOptions.class))
                .build());

    /*
     * Step 3b: Write elements that failed processing to deadletter table via {@link BigQueryIO}.
     */
    convertedPubsubMessages
        .get(TRANSFORM_DEADLETTER_OUT)
        .apply(
            "WriteTransformFailuresToBigQuery",
            ErrorConverters.WritePubsubMessageErrors.newBuilder()
                .setErrorRecordsTable(options.getDeadletterTable())
                .setErrorRecordsTableSchema(SchemaUtils.DEADLETTER_SCHEMA)
                .build());

    // Execute the pipeline and return the result.
    return pipeline.run();
  }

}
