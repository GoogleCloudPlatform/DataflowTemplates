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

import static com.google.cloud.teleport.v2.templates.PubSubToRedis.RedisSinkType.HASH_SINK;
import static com.google.cloud.teleport.v2.templates.PubSubToRedis.RedisSinkType.LOGGING_SINK;
import static com.google.cloud.teleport.v2.templates.PubSubToRedis.RedisSinkType.STREAMS_SINK;
import static com.google.cloud.teleport.v2.templates.PubSubToRedis.RedisSinkType.STRING_SINK;

import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.metadata.TemplateParameter;
import com.google.cloud.teleport.metadata.TemplateParameter.TemplateEnumOption;
import com.google.cloud.teleport.v2.coders.FailsafeElementCoder;
import com.google.cloud.teleport.v2.common.UncaughtExceptionLogger;
import com.google.cloud.teleport.v2.templates.io.RedisHashIO;
import com.google.cloud.teleport.v2.templates.transforms.MessageTransformation;
import com.google.cloud.teleport.v2.transforms.FailsafeElementTransforms.ConvertFailsafeElementToPubsubMessage;
import com.google.cloud.teleport.v2.transforms.JavascriptTextTransformer;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import com.google.common.base.Strings;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessageWithAttributesCoder;
import org.apache.beam.sdk.io.redis.RedisConnectionConfiguration;
import org.apache.beam.sdk.io.redis.RedisIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.commons.lang3.ArrayUtils;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link PubSubToRedis} pipeline is a streaming pipeline which ingests data in Bytes from
 * PubSub, and inserts resulting records as KV in Redis.
 *
 * <p><b>Pipeline Requirements</b>
 *
 * <ul>
 *   <li>The PubSub topic and subscriptions exist
 *   <li>The Redis is up and running
 * </ul>
 *
 * <p><b>Example Usage</b>
 *
 * <pre>
 * # Set the pipeline vars
 * PROJECT_NAME=my-project
 * BUCKET_NAME=my-bucket
 * INPUT_SUBSCRIPTION=my-subscription
 * REDIS_HOST=my-host
 * REDIS_PORT=my-port
 * REDIS_PASSWORD=my-pwd
 *
 * mvn compile exec:java \
 *  -Dexec.mainClass=com.google.cloud.teleport.v2.templates.PubSubToRedis \
 *  -Dexec.cleanupDaemonThreads=false \
 *  -Dexec.args=" \
 *  --project=${PROJECT_NAME} \
 *  --stagingLocation=gs://${BUCKET_NAME}/staging \
 *  --tempLocation=gs://${BUCKET_NAME}/temp \
 *  --runner=DataflowRunner \
 *  --inputSubscription=${INPUT_SUBSCRIPTION} \
 *  --redisHost=${REDIS_HOST}
 *  --redisPort=${REDIS_PORT}
 *  --redisPassword=${REDIS_PASSWORD}"
 * </pre>
 */
@Template(
    name = "Cloud_PubSub_to_Redis",
    category = TemplateCategory.STREAMING,
    displayName = "Pub/Sub to Redis",
    description = {
      "The Pub/Sub to Redis template is a streaming pipeline that reads messages from a Pub/Sub subscription and "
          + "writes the message payload to Redis. The most common use case of this template is to export logs to Redis "
          + "Enterprise for advanced search-based log analysis in real time.",
      "Before writing to Redis, you can apply a JavaScript user-defined function to the message payload. Any "
          + "messages that experience processing failures are forwarded to a Pub/Sub unprocessed topic for further "
          + "troubleshooting and reprocessing.",
      "For added security, enable an SSL connection when setting up your database endpoint connection."
    },
    optionsClass = PubSubToRedis.PubSubToRedisOptions.class,
    flexContainerName = "pubsub-to-redis",
    contactInformation = "https://github.com/GoogleCloudPlatform/DataflowTemplates/issues",
    documentation =
        "https://cloud.google.com/dataflow/docs/guides/templates/provided/pubsub-to-redis",
    requirements = {
      "The source Pub/Sub subscription must exist prior to running the pipeline.",
      "The Pub/Sub unprocessed topic must exist prior to running the pipeline.",
      "The Redis database endpoint must be accessible from the Dataflow workers' subnetwork.",
    },
    preview = true,
    streaming = true,
    supportsAtLeastOnce = true)
public class PubSubToRedis {
  /*
   * Options supported by {@link PubSubToRedis}
   *
   * <p>Inherits standard configuration options.
   */

  /** The log to output status messages to. */
  private static final Logger LOG = LoggerFactory.getLogger(PubSubToRedis.class);

  /** The tag for the main output for the UDF. */
  public static final TupleTag<FailsafeElement<PubsubMessage, String>> UDF_OUT =
      new TupleTag<FailsafeElement<PubsubMessage, String>>() {};

  /** The tag for the dead-letter output of the udf. */
  public static final TupleTag<FailsafeElement<PubsubMessage, String>> UDF_DEADLETTER_OUT =
      new TupleTag<FailsafeElement<PubsubMessage, String>>() {};

  /** Pubsub message/string coder for FailsafeElement. */
  public static final FailsafeElementCoder<PubsubMessage, String> FAILSAFE_ELEMENT_CODER =
      FailsafeElementCoder.of(PubsubMessageWithAttributesCoder.of(), StringUtf8Coder.of());

  /**
   * The {@link PubSubToRedisOptions} class provides the custom execution options passed by the
   * executor at the command-line.
   *
   * <p>Inherits standard configuration options.
   */
  public interface PubSubToRedisOptions
      extends JavascriptTextTransformer.JavascriptTextTransformerOptions {
    @TemplateParameter.PubsubSubscription(
        order = 1,
        groupName = "Source",
        description = "Pub/Sub input subscription",
        helpText = "The Pub/Sub subscription to read the input from.",
        example = "projects/<PROJECT_ID>/subscriptions/<SUBSCRIPTION_ID>")
    String getInputSubscription();

    void setInputSubscription(String value);

    @TemplateParameter.Text(
        order = 2,
        groupName = "Target",
        description = "Redis DB Host",
        helpText = "The Redis database host.",
        example = "your.cloud.db.redislabs.com")
    @Default.String("127.0.0.1")
    @Validation.Required
    String getRedisHost();

    void setRedisHost(String redisHost);

    @TemplateParameter.Integer(
        order = 3,
        groupName = "Target",
        description = "Redis DB Port",
        helpText = "The Redis database port.",
        example = "12345")
    @Default.Integer(6379)
    @Validation.Required
    int getRedisPort();

    void setRedisPort(int redisPort);

    @TemplateParameter.Password(
        order = 4,
        groupName = "Target",
        description = "Redis DB Password",
        helpText = "The Redis database password. Defaults to `empty`.")
    @Default.String("")
    @Validation.Required
    String getRedisPassword();

    void setRedisPassword(String redisPassword);

    @TemplateParameter.Boolean(
        order = 5,
        optional = true,
        description = "Redis ssl enabled",
        helpText = "The Redis database SSL parameter.")
    @Default.Boolean(false)
    @UnknownKeyFor
    @NonNull
    @Initialized
    ValueProvider<@UnknownKeyFor @NonNull @Initialized Boolean> getSslEnabled();

    void setSslEnabled(ValueProvider<Boolean> sslEnabled);

    @TemplateParameter.Enum(
        order = 6,
        optional = true,
        enumOptions = {
          @TemplateEnumOption("STRING_SINK"),
          @TemplateEnumOption("HASH_SINK"),
          @TemplateEnumOption("STREAMS_SINK"),
          @TemplateEnumOption("LOGGING_SINK")
        },
        description = "Redis sink to write",
        helpText =
            "The Redis sink. Supported values are `STRING_SINK, HASH_SINK, STREAMS_SINK, and LOGGING_SINK`.",
        example = "STRING_SINK")
    @Default.Enum("STRING_SINK")
    RedisSinkType getRedisSinkType();

    void setRedisSinkType(RedisSinkType redisSinkType);

    @TemplateParameter.Integer(
        order = 7,
        optional = true,
        description = "Redis connection timeout in milliseconds",
        helpText = "The Redis connection timeout in milliseconds. ",
        example = "2000")
    @Default.Integer(2000)
    int getConnectionTimeout();

    void setConnectionTimeout(int timeout);

    @TemplateParameter.Long(
        order = 8,
        optional = true,
        parentName = "redisSinkType",
        parentTriggerValues = {"HASH_SINK", "LOGGING_SINK"},
        description =
            "Hash key expiration time in sec (ttl), supported only for HASH_SINK and LOGGING_SINK",
        helpText =
            "The key expiration time in seconds. The `ttl` default for `HASH_SINK` is -1, which means it never expires.")
    @Default.Long(-1L)
    Long getTtl();

    void setTtl(Long ttl);

    @TemplateParameter.PubsubTopic(
        order = 9,
        description = "Output deadletter Pub/Sub topic",
        helpText =
            "The Pub/Sub topic to forward unprocessable messages to. Messages that fail UDF transformation are forwarded here.",
        example = "projects/<PROJECT_ID>/topics/<TOPIC_NAME>")
    @Validation.Required
    String getDeadletterTopic();

    void setDeadletterTopic(String deadletterTopic);
  }

  /** Allowed list of sink types. */
  public enum RedisSinkType {
    HASH_SINK,
    LOGGING_SINK,
    STREAMS_SINK,
    STRING_SINK
  }

  /**
   * Main entry point for executing the pipeline.
   *
   * @param args The command-line arguments to the pipeline.
   */
  public static void main(String[] args) {
    UncaughtExceptionLogger.register();

    // Parse the user options passed from the command-line.
    PubSubToRedisOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(PubSubToRedisOptions.class);
    run(options);
  }

  /**
   * Runs the pipeline with the supplied options.
   *
   * @param options The execution parameters to the pipeline.
   * @return The result of the pipeline execution.
   */
  public static PipelineResult run(PubSubToRedisOptions options) {

    // Create the pipeline
    Pipeline pipeline = Pipeline.create(options);

    // Register the coders for pipeline
    CoderRegistry coderRegistry = pipeline.getCoderRegistry();
    coderRegistry.registerCoderForType(
        FAILSAFE_ELEMENT_CODER.getEncodedTypeDescriptor(), FAILSAFE_ELEMENT_CODER);

    RedisConnectionConfiguration redisConnectionConfiguration =
        RedisConnectionConfiguration.create()
            .withHost(options.getRedisHost())
            .withPort(options.getRedisPort())
            .withAuth(options.getRedisPassword())
            .withTimeout(options.getConnectionTimeout())
            .withSSL(options.getSslEnabled());

    /*
     * Steps: 1) Read PubSubMessage with attributes and messageId from input PubSub subscription.
     *        2) Apply JavaScript UDF transformation to message payload (if configured).
     *        3) Extract PubSubMessage message to appropriate Redis format.
     *        4) Write to Redis using the configured sink type.
     *
     */

    LOG.info(
        "Starting PubSub-To-Redis Pipeline. Reading from subscription: {}",
        options.getInputSubscription());

    PCollection<PubsubMessage> messages =
        pipeline.apply(
            "Read PubSub Events",
            MessageTransformation.readFromPubSub(options.getInputSubscription()));

    PCollection<PubsubMessage> maybeTransformed = applyUdf(messages, options);

    if (options.getRedisSinkType().equals(STRING_SINK)) {
      PCollection<String> pCollectionString =
          maybeTransformed.apply(
              "Map to Redis String", ParDo.of(new MessageTransformation.MessageToRedisString()));

      PCollection<KV<String, String>> kvStringCollection =
          pCollectionString.apply(
              "Transform to String KV",
              MapElements.into(
                      TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.strings()))
                  .via(record -> KV.of(MessageTransformation.key, record)));

      kvStringCollection.apply(
          "Write to " + STRING_SINK.name(),
          RedisIO.write()
              .withMethod(RedisIO.Write.Method.SET)
              .withConnectionConfiguration(redisConnectionConfiguration));
    }
    if (options.getRedisSinkType().equals(HASH_SINK)) {
      PCollection<KV<String, KV<String, String>>> pCollectionHash =
          maybeTransformed.apply(
              "Map to Redis Hash", ParDo.of(new MessageTransformation.MessageToRedisHash()));

      pCollectionHash.apply(
          "Write to " + HASH_SINK.name(),
          RedisHashIO.write()
              .withConnectionConfiguration(redisConnectionConfiguration)
              .withTtl(options.getTtl()));
    }
    if (options.getRedisSinkType().equals(LOGGING_SINK)) {
      PCollection<KV<String, KV<String, String>>> pCollectionHash =
          maybeTransformed.apply(
              "Map to Redis Logs", ParDo.of(new MessageTransformation.MessageToRedisLogs()));

      pCollectionHash.apply(
          "Write to " + LOGGING_SINK.name(),
          RedisHashIO.write()
              .withConnectionConfiguration(redisConnectionConfiguration)
              .withTtl(options.getTtl()));
    }
    if (options.getRedisSinkType().equals(STREAMS_SINK)) {
      PCollection<KV<String, Map<String, String>>> pCollectionStreams =
          maybeTransformed.apply(
              "Map to Redis Streams", ParDo.of(new MessageTransformation.MessageToRedisStreams()));

      pCollectionStreams.apply(
          "Write to " + STREAMS_SINK.name(),
          RedisIO.writeStreams().withConnectionConfiguration(redisConnectionConfiguration));
    }
    // Execute the pipeline and return the result.
    return pipeline.run();
  }

  /**
   * Applies the JavaScript UDF to messages if configured, and writes UDF failures to the
   * dead-letter topic.
   *
   * <p>If no UDF is configured, returns the input messages unchanged.
   *
   * @param messages the input PubSub messages
   * @param options the pipeline options
   * @return the (possibly transformed) PubSub messages
   */
  // This follows the same pattern as PubsubProtoToBigQuery.runUdf
  static PCollection<PubsubMessage> applyUdf(
      PCollection<PubsubMessage> messages, PubSubToRedisOptions options) {

    boolean useJavascriptUdf = !Strings.isNullOrEmpty(options.getJavascriptTextTransformGcsPath());

    if (!useJavascriptUdf) {
      return messages;
    }

    if (Strings.isNullOrEmpty(options.getJavascriptTextTransformFunctionName())) {
      throw new IllegalArgumentException(
          "JavaScript function name cannot be null or empty if file is set");
    }

    // Map incoming messages to FailsafeElement so we can recover from failures
    // across multiple transforms.
    PCollection<FailsafeElement<PubsubMessage, String>> failsafeElements =
        messages.apply("MapToRecord", ParDo.of(new PubsubMessageToFailsafeElementFn()));

    PCollectionTuple udfResult =
        failsafeElements.apply(
            "InvokeUDF",
            JavascriptTextTransformer.FailsafeJavascriptUdf.<PubsubMessage>newBuilder()
                .setFileSystemPath(options.getJavascriptTextTransformGcsPath())
                .setFunctionName(options.getJavascriptTextTransformFunctionName())
                .setReloadIntervalMinutes(options.getJavascriptTextTransformReloadIntervalMinutes())
                .setSuccessTag(UDF_OUT)
                .setFailureTag(UDF_DEADLETTER_OUT)
                .build());

    // Write UDF failures to the dead-letter topic using the shared
    // ConvertFailsafeElementToPubsubMessage transform, following the pattern
    // from PubsubProtoToBigQuery.
    udfResult
        .get(UDF_DEADLETTER_OUT)
        .setCoder(FAILSAFE_ELEMENT_CODER)
        .apply(
            "Get UDF Failures",
            ConvertFailsafeElementToPubsubMessage.<PubsubMessage, String>builder()
                .setOriginalPayloadSerializeFn(msg -> ArrayUtils.toObject(msg.getPayload()))
                .setErrorMessageAttributeKey("udfErrorMessage")
                .build())
        .apply("Write Failed UDF", writeUdfFailures(options));

    // Extract the successfully transformed messages
    return udfResult
        .get(UDF_OUT)
        .apply(
            "Extract Transformed Payload",
            ParDo.of(
                new DoFn<FailsafeElement<PubsubMessage, String>, PubsubMessage>() {
                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    FailsafeElement<PubsubMessage, String> element = c.element();
                    c.output(
                        new PubsubMessage(
                            element.getPayload().getBytes(java.nio.charset.StandardCharsets.UTF_8),
                            element.getOriginalPayload().getAttributeMap(),
                            element.getOriginalPayload().getMessageId()));
                  }
                }));
  }

  /**
   * Returns a {@link PubsubIO.Write} configured to write UDF failures to the dead-letter topic.
   *
   * <p>Follows the same pattern as {@code PubsubProtoToBigQuery.writeUdfFailures}.
   */
  private static PubsubIO.Write<PubsubMessage> writeUdfFailures(PubSubToRedisOptions options) {
    return PubsubIO.writeMessages().to(options.getDeadletterTopic());
  }

  /**
   * The {@link PubsubMessageToFailsafeElementFn} wraps an incoming {@link PubsubMessage} with the
   * {@link FailsafeElement} class so errors can be recovered from and the original message can be
   * output to a dead-letter output.
   */
  static class PubsubMessageToFailsafeElementFn
      extends DoFn<PubsubMessage, FailsafeElement<PubsubMessage, String>> {
    @ProcessElement
    public void processElement(ProcessContext context) {
      PubsubMessage message = context.element();
      context.output(
          FailsafeElement.of(
              message, new String(message.getPayload(), java.nio.charset.StandardCharsets.UTF_8)));
    }
  }
}
