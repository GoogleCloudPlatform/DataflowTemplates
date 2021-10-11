package com.google.cloud.teleport.templates;

import com.google.cloud.teleport.coders.FailsafeElementCoder;
import com.google.cloud.teleport.sentinel.SentinelEvent;
import com.google.cloud.teleport.sentinel.SentinelEventCoder;
import com.google.cloud.teleport.sentinel.SentinelIO;
import com.google.cloud.teleport.sentinel.SentinelWriteError;
import com.google.cloud.teleport.templates.common.ErrorConverters;
import com.google.cloud.teleport.templates.common.JavascriptTextTransformer.FailsafeJavascriptUdf;
import com.google.cloud.teleport.templates.common.JavascriptTextTransformer.JavascriptTextTransformerOptions;
import com.google.cloud.teleport.templates.common.PubsubConverters.PubsubReadSubscriptionOptions;
import com.google.cloud.teleport.templates.common.PubsubConverters.PubsubWriteDeadletterTopicOptions;
import com.google.cloud.teleport.templates.common.SentinelConverters;
import com.google.cloud.teleport.templates.common.SentinelConverters.SentinelOptions;
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
 * The {@link PubSubToSentinel} pipeline is a streaming pipeline which ingests data from Cloud
 * Pub/Sub, executes a UDF, converts the output to {@link SentinelEvent}s and writes those records
 * into Sentinel's http endpoint. Any errors which occur in the execution of the UDF, conversion to
 * {@link SentinelEvent} or writing to http will be streamed into a Pub/Sub topic.
 *
 * <p><b>Pipeline Requirements</b>
 *
 * <ul>
 *   <li>The source Pub/Sub subscription exists.
 *   <li>http end-point is routable from the VPC where the Dataflow job executes.
 *   <li>Deadletter topic exists.
 * </ul>
 *
 * <p><b>Example Usage</b>
 *
 * <pre>
 * # Set the pipeline vars
 * PROJECT_ID=PROJECT ID HERE
 * BUCKET_NAME=BUCKET NAME HERE
 * PIPELINE_FOLDER=gs://${BUCKET_NAME}/dataflow/pipelines/pubsub-to-bigquery
 * USE_SUBSCRIPTION=true or false depending on whether the pipeline should read
 *                  from a Pub/Sub Subscription or a Pub/Sub Topic.
 *
 * # Set the runner
 * RUNNER=DataflowRunner
 *
 * # Build the template
 * mvn compile exec:java \
 * -Dexec.mainClass=com.google.cloud.teleport.templates.PubSubToSentinel \
 * -Dexec.cleanupDaemonThreads=false \
 * -Dexec.args=" \
 * --project=${PROJECT_ID} \
 * --stagingLocation=${PIPELINE_FOLDER}/staging \
 * --tempLocation=${PIPELINE_FOLDER}/temp \
 * --templateLocation=${PIPELINE_FOLDER}/template/PubSubToSentinel \
 * --runner=${RUNNER}
 * "
 *
 * # Execute the template
 * JOB_NAME=pubsub-to-sentinel-$USER-`date +"%Y%m%d-%H%M%S%z"`
 * BATCH_COUNT=1000
 * PARALLELISM=2
 *
 * # Execute the templated pipeline:
 * gcloud dataflow jobs run ${JOB_NAME} \
 * --gcs-location=${PIPELINE_FOLDER}/template/PubSubToSentinel \
 * --zone=us-east1-d \
 * --parameters \
 * "inputSubscription=projects/${PROJECT_ID}/subscriptions/input-subscription-name,\
 * token=my-sentinel-token,\
 * customerId=my-sentinel-customer-id,\
 * logTableName=my-sentinel-log-table-name,\
 * url=https://{workspace_id}.ods.opinsights.azure.com/api/logs?api-version=2016-04-01,\
 * batchCount=${BATCH_COUNT},\
 * parallelism=${PARALLELISM},\
 * disableCertificateValidation=false,\
 * outputDeadletterTopic=projects/${PROJECT_ID}/topics/deadletter-topic-name,\
 * </pre>
 */
public class PubSubToSentinel {
    
  /** String/String Coder for FailsafeElement. */
    public static final FailsafeElementCoder<String, String> FAILSAFE_ELEMENT_CODER =
      FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of());

    /** Counter to track inbound messages from source. */
    private static final Counter INPUT_MESSAGES_COUNTER =
        Metrics.counter(PubSubToSentinel.class, "inbound-pubsub-messages");

    /** The tag for successful {@link SentinelEvent} conversion. */
    private static final TupleTag<SentinelEvent> SENTINEL_EVENT_OUT = new TupleTag<SentinelEvent>() {};

    /** The tag for failed {@link SentinelEvent} conversion. */
    private static final TupleTag<FailsafeElement<String, String>> SENTINEL_EVENT_DEADLETTER_OUT =
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
    private static final Logger LOG = LoggerFactory.getLogger(PubSubToSentinel.class);

    private static final Boolean DEFAULT_INCLUDE_PUBSUB_MESSAGE = false;

    @VisibleForTesting protected static final String PUBSUB_MESSAGE_ATTRIBUTE_FIELD = "attributes";
    @VisibleForTesting protected static final String PUBSUB_MESSAGE_DATA_FIELD = "data";
    private static final String PUBSUB_MESSAGE_ID_FIELD = "messageId";    

      /**
   * The main entry-point for pipeline execution. This method will start the pipeline but will not
   * wait for it's execution to finish. If blocking execution is required, use the {@link
   * PubSubToSentinel#run(PubSubToSentinelOptions)} method to start the pipeline and invoke {@code
   * result.waitUntilFinish()} on the {@link PipelineResult}.
   *
   * @param args The command-line args passed by the executor.
   */
  public static void main(String[] args) {

    PubSubToSentinelOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(PubSubToSentinelOptions.class);

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
  public static PipelineResult run(PubSubToSentinelOptions options) {

    Pipeline pipeline = Pipeline.create(options);

    // Register coders.
    CoderRegistry registry = pipeline.getCoderRegistry();
    registry.registerCoderForClass(SentinelEvent.class, SentinelEventCoder.of());
    registry.registerCoderForType(
        FAILSAFE_ELEMENT_CODER.getEncodedTypeDescriptor(), FAILSAFE_ELEMENT_CODER);

    /*
     * Steps:
     *  1) Read messages in from Pub/Sub
     *  2) Convert message to FailsafeElement for processing.
     *  3) Apply user provided UDF (if any) on the input strings.
     *  4) Convert successfully transformed messages into SentinelEvent objects
     *  5) Write SentinelEvents to Sentinel's  end point.
     *  5a) Wrap write failures into a FailsafeElement.
     *  6) Collect errors from UDF transform (#3), SentinelEvent transform (#4)
     *     and writing to Sentinel endpoint (#5) and stream into a Pub/Sub deadletter topic.
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

    // 4) Convert successfully transformed messages into SentinelEvent objects
    PCollectionTuple convertToEventTuple =
        transformedOutput
            .get(UDF_OUT)
            .apply(
                "ConvertToSentinelEvent",
                SentinelConverters.failsafeStringToSentinelEvent(
                    SENTINEL_EVENT_OUT, SENTINEL_EVENT_DEADLETTER_OUT));

    // 5) Write SentinelEvents to Sentinel's end point.
    PCollection<SentinelWriteError> writeErrors =
        convertToEventTuple
            .get(SENTINEL_EVENT_OUT)
            .apply(
                "WriteToSentinel",
                SentinelIO.writeBuilder()
                    .withToken(options.getToken())
                    .withUrl(options.getUrl())
                    .withCustomerId(options.getCustomerId())
                    .withLogTableName(options.getLogTableName())
                    .withBatchCount(options.getBatchCount())
                    .withParallelism(options.getParallelism())
                    .withDisableCertificateValidation(options.getDisableCertificateValidation())
                    .build());

    // 5a) Wrap write failures into a FailsafeElement.
    PCollection<FailsafeElement<String, String>> wrappedSentinelWriteErrors =
        writeErrors.apply(
            "WrapSentinelWriteErrors",
            ParDo.of(
                new DoFn<SentinelWriteError, FailsafeElement<String, String>>() {

                  @ProcessElement
                  public void processElement(ProcessContext context) {
                    SentinelWriteError error = context.element();
                    FailsafeElement<String, String> failsafeElement =
                        FailsafeElement.of(error.payload(), error.payload());

                    if (error.statusMessage() != null) {
                      failsafeElement.setErrorMessage(error.statusMessage());
                    }

                    if (error.statusCode() != null) {
                      failsafeElement.setErrorMessage(
                          String.format("Sentinel write status code: %d", error.statusCode()));
                    }
                    context.output(failsafeElement);
                  }
                }));

    // 6) Collect errors from UDF transform (#4), SentinelEvent transform (#5)
    //     and writing to Sentinel endpoint  (#6) and stream into a Pub/Sub deadletter topic.
    PCollectionList.of(
            ImmutableList.of(
                convertToEventTuple.get(SENTINEL_EVENT_DEADLETTER_OUT),
                wrappedSentinelWriteErrors,
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
   * The {@link PubSubToSentinelOptions} class provides the custom options passed by the executor at
   * the command line.
   */
  public interface PubSubToSentinelOptions
      extends SentinelOptions,
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
