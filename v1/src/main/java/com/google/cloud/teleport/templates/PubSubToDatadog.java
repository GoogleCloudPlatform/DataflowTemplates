/*
 * Copyright (C) 2019 Google LLC
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

import com.google.cloud.teleport.coders.FailsafeElementCoder;
import com.google.cloud.teleport.datadog.DatadogEvent;
import com.google.cloud.teleport.datadog.DatadogEventCoder;
import com.google.cloud.teleport.datadog.DatadogIO;
import com.google.cloud.teleport.datadog.DatadogWriteError;
import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.templates.PubSubToDatadog.PubSubToDatadogOptions;
import com.google.cloud.teleport.templates.common.DatadogConverters;
import com.google.cloud.teleport.templates.common.DatadogConverters.DatadogOptions;
import com.google.cloud.teleport.templates.common.ErrorConverters;
import com.google.cloud.teleport.templates.common.JavascriptTextTransformer.FailsafeJavascriptUdf;
import com.google.cloud.teleport.templates.common.JavascriptTextTransformer.JavascriptTextTransformerOptions;
import com.google.cloud.teleport.templates.common.PubsubConverters.PubsubReadSubscriptionOptions;
import com.google.cloud.teleport.templates.common.PubsubConverters.PubsubWriteDeadletterTopicOptions;
import com.google.cloud.teleport.util.DatadogApiKeyNestedValueProvider;
import com.google.cloud.teleport.values.FailsafeElement;
import com.google.common.annotations.VisibleForTesting;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonSyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link PubSubToDatadog} pipeline is a streaming pipeline which ingests data from Cloud
 * Pub/Sub, executes a UDF, converts the output to {@link DatadogEvent}s and writes those records
 * into Datadog's Logs API. Any errors which occur in the execution of the UDF, conversion to
 * {@link DatadogEvent} or writing to Logs API will be streamed into a Pub/Sub topic.
 *
 * <p><b>Pipeline Requirements</b>
 *
 * <ul>
 *   <li>The source Pub/Sub subscription exists.
 *   <li>Logs API is routable from the VPC where the Dataflow job executes.
 *   <li>Deadletter topic exists.
 * </ul>
 *
 * <p>Check out <a
 * href="https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/v1/README_Cloud_PubSub_to_Datadog.md">README</a>
 * for instructions on how to use or modify this template.
 */
@Template(
    name = "Cloud_PubSub_to_Datadog",
    category = TemplateCategory.STREAMING,
    displayName = "Pub/Sub to Datadog",
    description =
        "A pipeline that reads from a Pub/Sub subscription and writes to Datadog's Logs API.",
    optionsClass = PubSubToDatadogOptions.class,
    optionsOrder = {
      PubsubReadSubscriptionOptions.class,
      DatadogOptions.class,
      JavascriptTextTransformerOptions.class,
      PubsubWriteDeadletterTopicOptions.class
    },
    documentation =
        "https://cloud.google.com/dataflow/docs/guides/templates/provided/pubsub-to-datadog",
    contactInformation = "https://cloud.google.com/support")
public class PubSubToDatadog {

  /** String/String Coder for FailsafeElement. */
  public static final FailsafeElementCoder<String, String> FAILSAFE_ELEMENT_CODER =
      FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of());

  /** Counter to track inbound messages from source. */
  private static final Counter INPUT_MESSAGES_COUNTER =
      Metrics.counter(PubSubToDatadog.class, "inbound-pubsub-messages");

  /** The tag for successful {@link DatadogEvent} conversion. */
  private static final TupleTag<DatadogEvent> DATADOG_EVENT_OUT = new TupleTag<DatadogEvent>() {};

  /** The tag for failed {@link DatadogEvent} conversion. */
  private static final TupleTag<FailsafeElement<String, String>> DATADOG_EVENT_DEADLETTER_OUT =
      new TupleTag<FailsafeElement<String, String>>() {};

  /** The tag for the main output for the UDF. */
  private static final TupleTag<FailsafeElement<String, String>> UDF_OUT =
      new TupleTag<FailsafeElement<String, String>>() {};

  /** The tag for the dead-letter output of the udf. */
  private static final TupleTag<FailsafeElement<String, String>> UDF_DEADLETTER_OUT =
      new TupleTag<FailsafeElement<String, String>>() {};

  /** GSON to process a {@link PubsubMessage}. */
  private static final Gson GSON = new Gson();

  /** Logger for class. */
  private static final Logger LOG = LoggerFactory.getLogger(PubSubToDatadog.class);

  private static final Boolean DEFAULT_INCLUDE_PUBSUB_MESSAGE = false;

  @VisibleForTesting protected static final String PUBSUB_MESSAGE_ATTRIBUTE_FIELD = "attributes";
  @VisibleForTesting protected static final String PUBSUB_MESSAGE_DATA_FIELD = "data";
  private static final String PUBSUB_MESSAGE_ID_FIELD = "messageId";

  /**
   * The main entry-point for pipeline execution. This method will start the pipeline but will not
   * wait for it's execution to finish. If blocking execution is required, use the {@link
   * PubSubToDatadog#run(PubSubToDatadogOptions)} method to start the pipeline and invoke {@code
   * result.waitUntilFinish()} on the {@link PipelineResult}.
   *
   * @param args The command-line args passed by the executor.
   */
  public static void main(String[] args) {

    PubSubToDatadogOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(PubSubToDatadogOptions.class);

    run(options);
  }

  /**
   * Runs the pipeline to completion with the specified options. This method does not wait until the
   * pipeline is finished before returning. Invoke {@code result.waitUntilFinish()} on the result
   * object to block until the pipeline is finished running if blocking programmatic execution is
   * required.
   *
   * @param options The execution options.
   * @return The pipeline result.
   */
  public static PipelineResult run(PubSubToDatadogOptions options) {

    Pipeline pipeline = Pipeline.create(options);

    // Register coders.
    CoderRegistry registry = pipeline.getCoderRegistry();
    registry.registerCoderForClass(DatadogEvent.class, DatadogEventCoder.of());
    registry.registerCoderForType(
        FAILSAFE_ELEMENT_CODER.getEncodedTypeDescriptor(), FAILSAFE_ELEMENT_CODER);

    /*
     * Steps:
     *  1) Read messages in from Pub/Sub
     *  2) Convert message to FailsafeElement for processing.
     *  3) Apply user provided UDF (if any) on the input strings.
     *  4) Convert successfully transformed messages into DatadogEvent objects
     *  5) Write DatadogEvents to Datadog's Logs API.
     *  5a) Wrap write failures into a FailsafeElement.
     *  6) Collect errors from UDF transform (#3), DatadogEvent transform (#4)
     *     and writing to Datadog Logs API (#5) and stream into a Pub/Sub deadletter topic.
     */

    // 1) Read messages in from Pub/Sub
    PCollection<String> stringMessages =
        pipeline.apply(
            "ReadMessages",
            new ReadMessages(options.getInputSubscription(), options.getIncludePubsubMessage()));

    // 2) Convert message to FailsafeElement for processing.
    PCollectionTuple transformedOutput =
        stringMessages
            .apply(
                "ConvertToFailsafeElement",
                MapElements.into(FAILSAFE_ELEMENT_CODER.getEncodedTypeDescriptor())
                    .via(input -> FailsafeElement.of(input, input)))

            // 3) Apply user provided UDF (if any) on the input strings.
            .apply(
                "ApplyUDFTransformation",
                FailsafeJavascriptUdf.<String>newBuilder()
                    .setFileSystemPath(options.getJavascriptTextTransformGcsPath())
                    .setFunctionName(options.getJavascriptTextTransformFunctionName())
                    .setLoggingEnabled(ValueProvider.StaticValueProvider.of(true))
                    .setSuccessTag(UDF_OUT)
                    .setFailureTag(UDF_DEADLETTER_OUT)
                    .build());

    // 4) Convert successfully transformed messages into DatadogEvent objects
    PCollectionTuple convertToEventTuple =
        transformedOutput
            .get(UDF_OUT)
            .apply(
                "ConvertToDatadogEvent",
                DatadogConverters.failsafeStringToDatadogEvent(
                    DATADOG_EVENT_OUT, DATADOG_EVENT_DEADLETTER_OUT));

    // 5) Write DatadogEvents to Datadog's Logs API.
    PCollection<DatadogWriteError> writeErrors =
        convertToEventTuple
            .get(DATADOG_EVENT_OUT)
            .apply(
                "WriteToDatadog",
                DatadogIO.writeBuilder()
                    .withApiKey(
                        new DatadogApiKeyNestedValueProvider(
                            options.getApiKeySecretId(),
                            options.getApiKeyKMSEncryptionKey(),
                            options.getApiKey(),
                            options.getApiKeySource()))
                    .withSite(options.getSite())
                    .withBatchCount(options.getBatchCount())
                    .withParallelism(options.getParallelism())
                    .build());

    // 5a) Wrap write failures into a FailsafeElement.
    PCollection<FailsafeElement<String, String>> wrappedDatadogWriteErrors =
        writeErrors.apply(
            "WrapDatadogWriteErrors",
            ParDo.of(
                new DoFn<DatadogWriteError, FailsafeElement<String, String>>() {

                  @ProcessElement
                  public void processElement(ProcessContext context) {
                    DatadogWriteError error = context.element();
                    FailsafeElement<String, String> failsafeElement =
                        FailsafeElement.of(error.payload(), error.payload());

                    if (error.statusMessage() != null) {
                      failsafeElement.setErrorMessage(error.statusMessage());
                    }

                    if (error.statusCode() != null) {
                      failsafeElement.setErrorMessage(
                          String.format("Datadog write status code: %d", error.statusCode()));
                    }
                    context.output(failsafeElement);
                  }
                }));

    // 6) Collect errors from UDF transform (#4), DatadogEvent transform (#5)
    //     and writing to Datadog Logs API (#6) and stream into a Pub/Sub deadletter topic.
    PCollectionList.of(
            ImmutableList.of(
                convertToEventTuple.get(DATADOG_EVENT_DEADLETTER_OUT),
                wrappedDatadogWriteErrors,
                transformedOutput.get(UDF_DEADLETTER_OUT)))
        .apply("FlattenErrors", Flatten.pCollections())
        .apply(
            "WriteFailedRecords",
            ErrorConverters.WriteStringMessageErrorsToPubSub.newBuilder()
                .setErrorRecordsTopic(options.getOutputDeadletterTopic())
                .build());

    return pipeline.run();
  }

  /**
   * The {@link PubSubToDatadogOptions} class provides the custom options passed by the executor at
   * the command line.
   */
  public interface PubSubToDatadogOptions
      extends DatadogOptions,
          PubsubReadSubscriptionOptions,
          PubsubWriteDeadletterTopicOptions,
          JavascriptTextTransformerOptions {}

  /**
   * A {@link PTransform} that reads messages from a Pub/Sub subscription, increments a counter and
   * returns a {@link PCollection} of {@link String} messages.
   */
  private static class ReadMessages extends PTransform<PBegin, PCollection<String>> {
    private final ValueProvider<String> subscriptionName;
    private final ValueProvider<Boolean> inputIncludePubsubMessageFlag;
    private Boolean includePubsubMessage;

    ReadMessages(
        ValueProvider<String> subscriptionName,
        ValueProvider<Boolean> inputIncludePubsubMessageFlag) {
      this.subscriptionName = subscriptionName;
      this.inputIncludePubsubMessageFlag = inputIncludePubsubMessageFlag;
    }

    @Override
    public PCollection<String> expand(PBegin input) {
      return input
          .apply(
              "ReadPubsubMessage",
              PubsubIO.readMessagesWithAttributes().fromSubscription(subscriptionName))
          .apply(
              "ExtractMessageIfRequired",
              ParDo.of(
                  new DoFn<PubsubMessage, String>() {

                    @Setup
                    public void setup() {
                      if (inputIncludePubsubMessageFlag != null) {
                        includePubsubMessage = inputIncludePubsubMessageFlag.get();
                      }
                      includePubsubMessage =
                          MoreObjects.firstNonNull(
                              includePubsubMessage, DEFAULT_INCLUDE_PUBSUB_MESSAGE);
                      LOG.info("includePubsubMessage set to: {}", includePubsubMessage);
                    }

                    @ProcessElement
                    public void processElement(ProcessContext context) {
                      if (includePubsubMessage) {
                        context.output(formatPubsubMessage(context.element()));
                      } else {
                        context.output(
                            new String(context.element().getPayload(), StandardCharsets.UTF_8));
                      }
                    }
                  }))
          .apply(
              "CountMessages",
              ParDo.of(
                  new DoFn<String, String>() {
                    @ProcessElement
                    public void processElement(ProcessContext context) {
                      INPUT_MESSAGES_COUNTER.inc();
                      context.output(context.element());
                    }
                  }));
    }
  }

  /**
   * Utility method that formats {@link org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage} according
   * to the model defined in {@link com.google.pubsub.v1.PubsubMessage}.
   *
   * @param pubsubMessage {@link org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage}
   * @return JSON String that adheres to the model defined in {@link
   *     com.google.pubsub.v1.PubsubMessage}
   */
  @VisibleForTesting
  protected static String formatPubsubMessage(PubsubMessage pubsubMessage) {
    JsonObject messageJson = new JsonObject();

    String payload = new String(pubsubMessage.getPayload(), StandardCharsets.UTF_8);
    try {
      JsonObject data = GSON.fromJson(payload, JsonObject.class);
      messageJson.add(PUBSUB_MESSAGE_DATA_FIELD, data);
    } catch (JsonSyntaxException e) {
      messageJson.addProperty(PUBSUB_MESSAGE_DATA_FIELD, payload);
    }

    JsonObject attributes = getAttributesJson(pubsubMessage.getAttributeMap());
    messageJson.add(PUBSUB_MESSAGE_ATTRIBUTE_FIELD, attributes);

    if (pubsubMessage.getMessageId() != null) {
      messageJson.addProperty(PUBSUB_MESSAGE_ID_FIELD, pubsubMessage.getMessageId());
    }

    return messageJson.toString();
  }

  /**
   * Constructs a {@link JsonObject} from a {@link Map} of Pub/Sub attributes.
   *
   * @param attributesMap {@link Map} of Pub/Sub attributes
   * @return {@link JsonObject} of Pub/Sub attributes
   */
  private static JsonObject getAttributesJson(Map<String, String> attributesMap) {
    JsonObject attributesJson = new JsonObject();
    for (String key : attributesMap.keySet()) {
      attributesJson.addProperty(key, attributesMap.get(key));
    }

    return attributesJson;
  }
}
