/*
 * Copyright (C) 2019 Google Inc.
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
package com.google.cloud.teleport.v2.templates;

import com.google.auto.value.AutoValue;
import com.google.cloud.teleport.v2.coders.FailsafeElementCoder;
import com.google.cloud.teleport.v2.transforms.ElasticsearchTransforms.WriteToElasticsearch;
import com.google.cloud.teleport.v2.transforms.ElasticsearchTransforms.WriteToElasticsearchOptions;
import com.google.cloud.teleport.v2.transforms.ErrorConverters;
import com.google.cloud.teleport.v2.transforms.JavascriptTextTransformer.FailsafeJavascriptUdf;
import com.google.cloud.teleport.v2.transforms.JavascriptTextTransformer.JavascriptTextTransformerOptions;
import com.google.cloud.teleport.v2.utils.SchemaUtils;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonSyntaxException;
import java.nio.charset.StandardCharsets;
import javax.annotation.Nullable;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessageWithAttributesCoder;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.guava.v20_0.com.google.common.base.Throwables;
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
                .setOptions(options.as(WriteToElasticsearchOptions.class))
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

  /**
   * The {@link PubSubToElasticsearchOptions} class provides the custom execution options passed by
   * the executor at the command-line.
   *
   * <p>Inherits standard configuration options, options from {@link
   * JavascriptTextTransformerOptions}, and options from {@link WriteToElasticsearchOptions}.
   */
  public interface PubSubToElasticsearchOptions
      extends JavascriptTextTransformerOptions, PipelineOptions, WriteToElasticsearchOptions {

    @Description(
        "The Cloud Pub/Sub subscription to consume from. "
            + "The name should be in the format of "
            + "projects/<project-id>/subscriptions/<subscription-name>.")
    String getInputSubscription();

    void setInputSubscription(String inputSubscription);

    @Description(
        "The dead-letter table to output to within BigQuery in <project-id>:<dataset>.<table> "
            + "format.")
    @Required
    String getDeadletterTable();

    void setDeadletterTable(String deadletterTable);
  }

  /**
   * The {@link PubSubMessageToJsonDocument} class is a {@link PTransform} which transforms incoming
   * {@link PubsubMessage} objects into JSON objects for insertion into Elasticsearch while applying
   * an optional UDF to the input. The executions of the UDF and transformation to Json objects is
   * done in a fail-safe way by wrapping the element with it's original payload inside the {@link
   * FailsafeElement} class. The {@link PubSubMessageToJsonDocument} transform will output a {@link
   * PCollectionTuple} which contains all output and dead-letter {@link PCollection}.
   *
   * <p>The {@link PCollectionTuple} output will contain the following {@link PCollection}:
   *
   * <ul>
   *   <li>{@link PubSubToElasticsearch#TRANSFORM_OUT} - Contains all records successfully converted
   *       to JSON objects.
   *   <li>{@link PubSubToElasticsearch#TRANSFORM_DEADLETTER_OUT} - Contains all {@link
   *       FailsafeElement} records which couldn't be converted to table rows.
   * </ul>
   */
  @AutoValue
  abstract static class PubSubMessageToJsonDocument
      extends PTransform<PCollection<PubsubMessage>, PCollectionTuple> {

    public static Builder newBuilder() {
      return new AutoValue_PubSubToElasticsearch_PubSubMessageToJsonDocument.Builder();
    }

    @Nullable
    public abstract String javascriptTextTransformGcsPath();

    @Nullable
    public abstract String javascriptTextTransformFunctionName();

    @Override
    public PCollectionTuple expand(PCollection<PubsubMessage> input) {

      // Map the incoming messages into FailsafeElements so we can recover from failures
      // across multiple transforms.
      PCollection<FailsafeElement<PubsubMessage, String>> failsafeElements =
          input.apply("MapToRecord", ParDo.of(new PubsubMessageToFailsafeElementFn()));

      // If a Udf is supplied then use it to parse the PubSubMessages.
      if (javascriptTextTransformGcsPath() != null) {
        return failsafeElements.apply(
            "InvokeUDF",
            FailsafeJavascriptUdf.<PubsubMessage>newBuilder()
                .setFileSystemPath(javascriptTextTransformGcsPath())
                .setFunctionName(javascriptTextTransformFunctionName())
                .setSuccessTag(TRANSFORM_OUT)
                .setFailureTag(TRANSFORM_DEADLETTER_OUT)
                .build());
      } else {
        return failsafeElements.apply(
            "ProcessPubSubMessages",
            ParDo.of(new ProcessFailsafePubSubFn())
                .withOutputTags(TRANSFORM_OUT, TupleTagList.of(TRANSFORM_DEADLETTER_OUT)));
      }
    }

    /** Builder for {@link PubSubMessageToJsonDocument}. */
    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setJavascriptTextTransformGcsPath(
          String javascriptTextTransformGcsPath);

      public abstract Builder setJavascriptTextTransformFunctionName(
          String javascriptTextTransformFunctionName);

      public abstract PubSubMessageToJsonDocument build();
    }
  }

  /**
   * The {@link ProcessFailsafePubSubFn} class processes a {@link FailsafeElement} containing an {@link PubsubMessage}
   * and a String of the message's payload {@link PubsubMessage#getPayload()} into a {@link FailsafeElement} of
   * the original {@link PubsubMessage} and a JSON string that has been processed with {@link Gson}.
   *
   * If {@link PubsubMessage#getAttributeMap()} is not empty then the message attributes will be serialized along with
   * the message payload.
   *
   */
  static class ProcessFailsafePubSubFn
          extends DoFn<FailsafeElement<PubsubMessage, String>, FailsafeElement<PubsubMessage, String>> {

    private static final Counter successCounter =
            Metrics.counter(
                    PubSubMessageToJsonDocument.class, "successful-messages-processed");

    private static Gson gson = new Gson();

    private static final Counter failedCounter =
            Metrics.counter(
                    PubSubMessageToJsonDocument.class, "failed-messages-processed");

    @ProcessElement
    public void processElement(ProcessContext context) {
      PubsubMessage pubsubMessage = context.element().getOriginalPayload();

      JsonObject messageObject = new JsonObject();

      try {
        if (pubsubMessage.getPayload().length > 0) {
          messageObject = gson.fromJson(new String(pubsubMessage.getPayload()), JsonObject.class);
        }

        // If message attributes are present they will be serialized along with the message payload
        if (pubsubMessage.getAttributeMap() != null) {
          pubsubMessage.getAttributeMap().forEach(messageObject::addProperty);
        }

        context.output(
                FailsafeElement.of(pubsubMessage, messageObject.toString()));
        successCounter.inc();

      } catch (JsonSyntaxException e) {
        context.output(
                TRANSFORM_DEADLETTER_OUT,
                FailsafeElement.of(context.element())
                        .setErrorMessage(e.getMessage())
                        .setStacktrace(Throwables.getStackTraceAsString(e)));
        failedCounter.inc();
      }
    }
  }

  /**
   * The {@link PubsubMessageToFailsafeElementFn} wraps an incoming {@link PubsubMessage} with the
   * {@link FailsafeElement} class so errors can be recovered from and the original message can be
   * output to a error records table.
   */
  static class PubsubMessageToFailsafeElementFn
      extends DoFn<PubsubMessage, FailsafeElement<PubsubMessage, String>> {
    @ProcessElement
    public void processElement(ProcessContext context) {
      PubsubMessage message = context.element();
      context.output(
          FailsafeElement.of(message, new String(message.getPayload(), StandardCharsets.UTF_8)));
    }
  }
}
