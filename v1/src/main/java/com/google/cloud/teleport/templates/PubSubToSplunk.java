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
import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.splunk.SplunkEvent;
import com.google.cloud.teleport.splunk.SplunkEventCoder;
import com.google.cloud.teleport.splunk.SplunkIO;
import com.google.cloud.teleport.splunk.SplunkWriteError;
import com.google.cloud.teleport.templates.PubSubToSplunk.PubSubToSplunkOptions;
import com.google.cloud.teleport.templates.common.ErrorConverters;
import com.google.cloud.teleport.templates.common.JavascriptTextTransformer.FailsafeJavascriptUdf;
import com.google.cloud.teleport.templates.common.JavascriptTextTransformer.JavascriptTextTransformerOptions;
import com.google.cloud.teleport.templates.common.PubsubConverters.PubsubReadSubscriptionOptions;
import com.google.cloud.teleport.templates.common.PubsubConverters.PubsubWriteDeadletterTopicOptions;
import com.google.cloud.teleport.templates.common.SplunkConverters;
import com.google.cloud.teleport.templates.common.SplunkConverters.SplunkOptions;
import com.google.cloud.teleport.util.TokenNestedValueProvider;
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
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link PubSubToSplunk} pipeline is a streaming pipeline which ingests data from Cloud
 * Pub/Sub, executes a UDF, converts the output to {@link SplunkEvent}s and writes those records
 * into Splunk's HEC endpoint. Any errors which occur in the execution of the UDF, conversion to
 * {@link SplunkEvent} or writing to HEC will be streamed into a Pub/Sub topic.
 *
 * <p><b>Pipeline Requirements</b>
 *
 * <ul>
 *   <li>The source Pub/Sub subscription exists.
 *   <li>HEC end-point is routable from the VPC where the Dataflow job executes.
 *   <li>Deadletter topic exists.
 * </ul>
 *
 * <p>Check out <a
 * href="https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/v1/README_Cloud_PubSub_to_Splunk.md">README</a>
 * for instructions on how to use or modify this template.
 */
@Template(
    name = "Cloud_PubSub_to_Splunk",
    category = TemplateCategory.STREAMING,
    displayName = "Pub/Sub to Splunk",
    description = {
      "The Pub/Sub to Splunk template is a streaming pipeline that reads messages from a Pub/Sub subscription and writes the message payload to Splunk via Splunk's HTTP Event Collector (HEC). The most common use case of this template is to export logs to Splunk. "
          + "To see an example of the underlying workflow, see <a href=\"https://cloud.google.com/architecture/deploying-production-ready-log-exports-to-splunk-using-dataflow\">Deploying production-ready log exports to Splunk using Dataflow</a>.\n",
      "Before writing to Splunk, you can also apply a JavaScript user-defined function to the message payload. "
          + "Any messages that experience processing failures are forwarded to a Pub/Sub unprocessed topic for further troubleshooting and reprocessing.\n",
      "As an extra layer of protection for your HEC token, you can also pass in a Cloud KMS key along with the base64-encoded HEC token parameter encrypted with the Cloud KMS key. "
          + "See the <a href=\"https://cloud.google.com/kms/docs/reference/rest/v1/projects.locations.keyRings.cryptoKeys/encrypt\">Cloud KMS API encryption endpoint</a> for additional details on encrypting your HEC token parameter."
    },
    optionsClass = PubSubToSplunkOptions.class,
    optionsOrder = {
      PubsubReadSubscriptionOptions.class,
      SplunkOptions.class,
      JavascriptTextTransformerOptions.class,
      PubsubWriteDeadletterTopicOptions.class
    },
    documentation =
        "https://cloud.google.com/dataflow/docs/guides/templates/provided/pubsub-to-splunk",
    contactInformation = "https://cloud.google.com/support",
    preview = true,
    requirements = {
      "The source Pub/Sub subscription must exist prior to running the pipeline.",
      "The Pub/Sub unprocessed topic must exist prior to running the pipeline.",
      "The Splunk HEC endpoint must be accessible from the Dataflow workers' network.",
      "The Splunk HEC token must be generated and available."
    })
public class PubSubToSplunk {

  /** String/String Coder for FailsafeElement. */
  public static final FailsafeElementCoder<String, String> FAILSAFE_ELEMENT_CODER =
      FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of());

  /** Counter to track inbound messages from source. */
  private static final Counter INPUT_MESSAGES_COUNTER =
      Metrics.counter(PubSubToSplunk.class, "inbound-pubsub-messages");

  /** The tag for successful {@link SplunkEvent} conversion. */
  private static final TupleTag<SplunkEvent> SPLUNK_EVENT_OUT = new TupleTag<SplunkEvent>() {};

  /** The tag for failed {@link SplunkEvent} conversion. */
  private static final TupleTag<FailsafeElement<String, String>> SPLUNK_EVENT_DEADLETTER_OUT =
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
  private static final Logger LOG = LoggerFactory.getLogger(PubSubToSplunk.class);

  private static final Boolean DEFAULT_INCLUDE_PUBSUB_MESSAGE = false;

  @VisibleForTesting protected static final String PUBSUB_MESSAGE_ATTRIBUTE_FIELD = "attributes";
  @VisibleForTesting protected static final String PUBSUB_MESSAGE_DATA_FIELD = "data";
  private static final String PUBSUB_MESSAGE_ID_FIELD = "messageId";

  /**
   * The main entry-point for pipeline execution. This method will start the pipeline but will not
   * wait for it's execution to finish. If blocking execution is required, use the {@link
   * PubSubToSplunk#run(PubSubToSplunkOptions)} method to start the pipeline and invoke {@code
   * result.waitUntilFinish()} on the {@link PipelineResult}.
   *
   * @param args The command-line args passed by the executor.
   */
  public static void main(String[] args) {

    PubSubToSplunkOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(PubSubToSplunkOptions.class);

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
  public static PipelineResult run(PubSubToSplunkOptions options) {

    Pipeline pipeline = Pipeline.create(options);

    // Register coders.
    CoderRegistry registry = pipeline.getCoderRegistry();
    registry.registerCoderForClass(SplunkEvent.class, SplunkEventCoder.of());
    registry.registerCoderForType(
        FAILSAFE_ELEMENT_CODER.getEncodedTypeDescriptor(), FAILSAFE_ELEMENT_CODER);

    /*
     * Steps:
     *  1) Read messages in from Pub/Sub
     *  2) Convert message to FailsafeElement for processing.
     *  3) Apply user provided UDF (if any) on the input strings.
     *  4) Convert successfully transformed messages into SplunkEvent objects
     *  5) Write SplunkEvents to Splunk's HEC end point.
     *  5a) Wrap write failures into a FailsafeElement.
     *  6) Collect errors from UDF transform (#3), SplunkEvent transform (#4)
     *     and writing to Splunk HEC (#5) and stream into a Pub/Sub deadletter topic.
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
                    .setFunctionReload(options.getJavascriptFunctionReload())
                    .setReloadIntervalMinutes(options.getJavascriptReloadIntervalMinutes())
                    .setLoggingEnabled(ValueProvider.StaticValueProvider.of(true))
                    .setSuccessTag(UDF_OUT)
                    .setFailureTag(UDF_DEADLETTER_OUT)
                    .build());

    // 4) Convert successfully transformed messages into SplunkEvent objects
    PCollectionTuple convertToEventTuple =
        transformedOutput
            .get(UDF_OUT)
            .apply(
                "ConvertToSplunkEvent",
                SplunkConverters.failsafeStringToSplunkEvent(
                    SPLUNK_EVENT_OUT, SPLUNK_EVENT_DEADLETTER_OUT));

    // 5) Write SplunkEvents to Splunk's HEC end point.
    PCollection<SplunkWriteError> writeErrors =
        convertToEventTuple
            .get(SPLUNK_EVENT_OUT)
            .apply(
                "WriteToSplunk",
                SplunkIO.writeBuilder()
                    .withToken(
                        new TokenNestedValueProvider(
                            options.getTokenSecretId(),
                            options.getTokenKMSEncryptionKey(),
                            options.getToken(),
                            options.getTokenSource()))
                    .withUrl(options.getUrl())
                    .withBatchCount(options.getBatchCount())
                    .withParallelism(options.getParallelism())
                    .withDisableCertificateValidation(options.getDisableCertificateValidation())
                    .withRootCaCertificatePath(options.getRootCaCertificatePath())
                    .withEnableBatchLogs(options.getEnableBatchLogs())
                    .withEnableGzipHttpCompression(options.getEnableGzipHttpCompression())
                    .build());

    // 5a) Wrap write failures into a FailsafeElement.
    PCollection<FailsafeElement<String, String>> wrappedSplunkWriteErrors =
        writeErrors.apply(
            "WrapSplunkWriteErrors",
            ParDo.of(
                new DoFn<SplunkWriteError, FailsafeElement<String, String>>() {

                  @ProcessElement
                  public void processElement(ProcessContext context) {
                    SplunkWriteError error = context.element();
                    FailsafeElement<String, String> failsafeElement =
                        FailsafeElement.of(error.payload(), error.payload());

                    if (error.statusMessage() != null) {
                      failsafeElement.setErrorMessage(error.statusMessage());
                    }

                    if (error.statusCode() != null) {
                      failsafeElement.setErrorMessage(
                          String.format("Splunk write status code: %d", error.statusCode()));
                    }
                    context.output(failsafeElement);
                  }
                }));

    // 6) Collect errors from UDF transform (#4), SplunkEvent transform (#5)
    //     and writing to Splunk HEC (#6) and stream into a Pub/Sub deadletter topic.
    PCollectionList.of(
            ImmutableList.of(
                convertToEventTuple.get(SPLUNK_EVENT_DEADLETTER_OUT),
                wrappedSplunkWriteErrors,
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
   * The {@link PubSubToSplunkOptions} class provides the custom options passed by the executor at
   * the command line.
   */
  public interface PubSubToSplunkOptions
      extends SplunkOptions,
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
