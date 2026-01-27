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
import com.google.cloud.teleport.v2.transforms.JavascriptTextTransformer;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import com.google.common.base.Strings;
import java.util.HashMap;
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
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
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

  /** The tag for messages that fail during Redis transformation/write. */
  public static final TupleTag<PubsubMessage> REDIS_FAILURE_OUT = new TupleTag<PubsubMessage>() {};

  /** The tag for successful Redis String transformations. */
  public static final TupleTag<KV<String, String>> REDIS_STRING_OUT =
      new TupleTag<KV<String, String>>() {};

  /** The tag for successful Redis Hash transformations. */
  public static final TupleTag<KV<String, KV<String, String>>> REDIS_HASH_OUT =
      new TupleTag<KV<String, KV<String, String>>>() {};

  /** The tag for successful Redis Streams transformations. */
  public static final TupleTag<KV<String, Map<String, String>>> REDIS_STREAMS_OUT =
      new TupleTag<KV<String, Map<String, String>>>() {};

  /** Pubsub message/string coder for FailsafeElement. */
  public static final FailsafeElementCoder<PubsubMessage, String> FAILSAFE_ELEMENT_CODER =
      FailsafeElementCoder.of(PubsubMessageWithAttributesCoder.of(), StringUtf8Coder.of());

  /**
   * The {@link PubSubToRedisOptions} class provides the custom execution options passed by the
   * executor at the command-line.
   *
   * <p>Inherits standard configuration options, options from {@link
   * JavascriptTextTransformer.JavascriptTextTransformerOptions}.
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
        optional = true,
        description = "Dead-letter topic for failed messages",
        helpText =
            "The Pub/Sub topic to publish failed messages to. Messages that fail UDF transformation will be sent here.",
        example = "projects/<PROJECT_ID>/topics/<TOPIC_NAME>")
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

    PCollection<PubsubMessage> messages;

    RedisConnectionConfiguration redisConnectionConfiguration =
        RedisConnectionConfiguration.create()
            .withHost(options.getRedisHost())
            .withPort(options.getRedisPort())
            .withAuth(options.getRedisPassword())
            .withTimeout(options.getConnectionTimeout())
            .withSSL(options.getSslEnabled());

    boolean useJavascriptUdf = !Strings.isNullOrEmpty(options.getJavascriptTextTransformGcsPath());

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

    PCollection<PubsubMessage> readMessagesFromPubsub =
        pipeline.apply(
            "Read PubSub Events",
            MessageTransformation.readFromPubSub(options.getInputSubscription()));

    LOG.info("Messages read from PubSub subscription: {}", options.getInputSubscription());

    if (useJavascriptUdf) {
      LOG.info("Applying JavaScript UDF from: {}", options.getJavascriptTextTransformGcsPath());
      PCollectionTuple transformedMessages =
          readMessagesFromPubsub.apply(
              "Apply UDF",
              new PubSubMessageTransform(
                  options.getJavascriptTextTransformGcsPath(),
                  options.getJavascriptTextTransformFunctionName(),
                  options.getJavascriptTextTransformReloadIntervalMinutes()));

      // Extract the successfully transformed messages
      LOG.info("Extracting successfully transformed messages from UDF output");
      messages =
          transformedMessages
              .get(UDF_OUT)
              .apply(
                  "Extract Transformed Payload",
                  ParDo.of(
                      new DoFn<FailsafeElement<PubsubMessage, String>, PubsubMessage>() {
                        @ProcessElement
                        public void processElement(ProcessContext c) {
                          FailsafeElement<PubsubMessage, String> element = c.element();
                          LOG.debug(
                              "Successfully transformed message: {}",
                              element.getOriginalPayload().getMessageId());
                          // Create new PubsubMessage with transformed payload
                          PubsubMessage transformedMessage =
                              new PubsubMessage(
                                  element.getPayload().getBytes(),
                                  element.getOriginalPayload().getAttributeMap(),
                                  element.getOriginalPayload().getMessageId());
                          c.output(transformedMessage);
                        }
                      }));

      // Write dead-letter messages to topic or log them
      PCollection<FailsafeElement<PubsubMessage, String>> failedMessages =
          transformedMessages.get(UDF_DEADLETTER_OUT);

      if (!Strings.isNullOrEmpty(options.getDeadletterTopic())) {
        LOG.info("Writing failed messages to dead-letter topic: {}", options.getDeadletterTopic());
        failedMessages
            .apply(
                "Convert Failed to PubsubMessage",
                ParDo.of(
                    new DoFn<FailsafeElement<PubsubMessage, String>, PubsubMessage>() {
                      @ProcessElement
                      public void processElement(ProcessContext c) {
                        FailsafeElement<PubsubMessage, String> element = c.element();
                        // Add error information as attributes
                        Map<String, String> attributes = new HashMap<>();
                        attributes.putAll(element.getOriginalPayload().getAttributeMap());
                        attributes.put("error_message", element.getErrorMessage());
                        attributes.put(
                            "original_message_id", element.getOriginalPayload().getMessageId());

                        // Publish the ORIGINAL payload to DLQ for reprocessing
                        PubsubMessage deadLetterMessage =
                            new PubsubMessage(
                                element.getOriginalPayload().getPayload(), attributes);
                        c.output(deadLetterMessage);
                      }
                    }))
            .apply(
                "Write to Dead-letter Topic",
                PubsubIO.writeMessages().to(options.getDeadletterTopic()));
      } else {
        // Log dead-letter messages if no topic configured
        failedMessages.apply(
            "Log Failed Transformations",
            ParDo.of(
                new DoFn<FailsafeElement<PubsubMessage, String>, Void>() {
                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    FailsafeElement<PubsubMessage, String> element = c.element();
                    LOG.error(
                        "Failed to transform message {}: {}. Payload: {}",
                        element.getOriginalPayload().getMessageId(),
                        element.getErrorMessage(),
                        element.getPayload());
                  }
                }));
      }
    } else {
      LOG.info("No JavaScript UDF configured, using original messages");
      messages = readMessagesFromPubsub;
    }

    if (options.getRedisSinkType().equals(STRING_SINK)) {
      LOG.info(
          "Writing to Redis {} sink at {}:{}",
          STRING_SINK.name(),
          options.getRedisHost(),
          options.getRedisPort());

      PCollectionTuple stringResult =
          messages.apply(
              "Transform to Redis String with Error Handling",
              ParDo.of(new SafeRedisStringTransform())
                  .withOutputTags(REDIS_STRING_OUT, TupleTagList.of(REDIS_FAILURE_OUT)));

      stringResult
          .get(REDIS_STRING_OUT)
          .apply(
              "Write to " + STRING_SINK.name(),
              RedisIO.write()
                  .withMethod(RedisIO.Write.Method.SET)
                  .withConnectionConfiguration(redisConnectionConfiguration));

      writeRedisFailuresToDeadLetter(
          stringResult.get(REDIS_FAILURE_OUT),
          options.getDeadletterTopic(),
          "Redis String Transformation");
      LOG.info("String sink write operation configured");
    }
    if (options.getRedisSinkType().equals(HASH_SINK)) {
      LOG.info(
          "Writing to Redis {} sink at {}:{} with TTL: {}s",
          HASH_SINK.name(),
          options.getRedisHost(),
          options.getRedisPort(),
          options.getTtl());

      PCollectionTuple hashResult =
          messages.apply(
              "Transform to Redis Hash with Error Handling",
              ParDo.of(new SafeRedisHashTransform())
                  .withOutputTags(REDIS_HASH_OUT, TupleTagList.of(REDIS_FAILURE_OUT)));

      hashResult
          .get(REDIS_HASH_OUT)
          .apply(
              "Write to " + HASH_SINK.name(),
              RedisHashIO.write()
                  .withConnectionConfiguration(redisConnectionConfiguration)
                  .withTtl(options.getTtl()));

      writeRedisFailuresToDeadLetter(
          hashResult.get(REDIS_FAILURE_OUT),
          options.getDeadletterTopic(),
          "Redis Hash Transformation");
      LOG.info("Hash sink write operation configured");
    }
    if (options.getRedisSinkType().equals(LOGGING_SINK)) {
      LOG.info(
          "Writing to Redis {} sink at {}:{} with TTL: {}s",
          LOGGING_SINK.name(),
          options.getRedisHost(),
          options.getRedisPort(),
          options.getTtl());

      PCollectionTuple logsResult =
          messages.apply(
              "Transform to Redis Logs with Error Handling",
              ParDo.of(new SafeRedisLogsTransform())
                  .withOutputTags(REDIS_HASH_OUT, TupleTagList.of(REDIS_FAILURE_OUT)));

      logsResult
          .get(REDIS_HASH_OUT)
          .apply(
              "Write to " + LOGGING_SINK.name(),
              RedisHashIO.write()
                  .withConnectionConfiguration(redisConnectionConfiguration)
                  .withTtl(options.getTtl()));

      writeRedisFailuresToDeadLetter(
          logsResult.get(REDIS_FAILURE_OUT),
          options.getDeadletterTopic(),
          "Redis Logs Transformation");
      LOG.info("Logging sink write operation configured");
    }
    if (options.getRedisSinkType().equals(STREAMS_SINK)) {
      LOG.info(
          "Writing to Redis {} sink at {}:{}",
          STREAMS_SINK.name(),
          options.getRedisHost(),
          options.getRedisPort());

      PCollectionTuple streamsResult =
          messages.apply(
              "Transform to Redis Streams with Error Handling",
              ParDo.of(new SafeRedisStreamsTransform())
                  .withOutputTags(REDIS_STREAMS_OUT, TupleTagList.of(REDIS_FAILURE_OUT)));

      streamsResult
          .get(REDIS_STREAMS_OUT)
          .apply(
              "Write to " + STREAMS_SINK.name(),
              RedisIO.writeStreams().withConnectionConfiguration(redisConnectionConfiguration));

      writeRedisFailuresToDeadLetter(
          streamsResult.get(REDIS_FAILURE_OUT),
          options.getDeadletterTopic(),
          "Redis Streams Transformation");
      LOG.info("Streams sink write operation configured");
    }
    // Execute the pipeline and return the result.
    LOG.info("Pipeline configuration complete. Starting pipeline execution...");
    PipelineResult result = pipeline.run();
    LOG.info("Pipeline execution started successfully");
    return result;
  }

  /** Helper method to write Redis transformation failures to dead-letter topic. */
  private static void writeRedisFailuresToDeadLetter(
      PCollection<PubsubMessage> failures, String deadletterTopic, String transformationType) {
    if (!Strings.isNullOrEmpty(deadletterTopic)) {
      LOG.info(
          "Configuring dead-letter output for {} failures to topic: {}",
          transformationType,
          deadletterTopic);
      failures
          .apply(
              "Add Error Attributes for " + transformationType,
              ParDo.of(
                  new DoFn<PubsubMessage, PubsubMessage>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                      PubsubMessage original = c.element();
                      Map<String, String> attributes = new HashMap<>();
                      attributes.putAll(original.getAttributeMap());
                      attributes.put("error_step", transformationType);
                      attributes.put("error_timestamp", String.valueOf(System.currentTimeMillis()));
                      c.output(
                          new PubsubMessage(
                              original.getPayload(), attributes, original.getMessageId()));
                    }
                  }))
          .apply(
              "Write " + transformationType + " Failures to Dead-letter Topic",
              PubsubIO.writeMessages().to(deadletterTopic));
    } else {
      LOG.info("No dead-letter topic configured. Logging {} failures only.", transformationType);
      failures.apply(
          "Log " + transformationType + " Failures",
          ParDo.of(
              new DoFn<PubsubMessage, Void>() {
                @ProcessElement
                public void processElement(ProcessContext c) {
                  PubsubMessage msg = c.element();
                  LOG.error(
                      "Failed to transform message {} for Redis. Payload size: {} bytes",
                      msg.getMessageId(),
                      msg.getPayload().length);
                }
              }));
    }
  }

  /** Safe transformation to Redis String format with error handling. */
  static class SafeRedisStringTransform extends DoFn<PubsubMessage, KV<String, String>> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      try {
        PubsubMessage msg = c.element();
        String value = new String(msg.getPayload(), java.nio.charset.StandardCharsets.UTF_8);

        // Build key from attribute or use message ID as fallback
        String attributeKey = msg.getAttributeMap().get("key");
        String key;
        if (attributeKey != null && !attributeKey.isEmpty()) {
          key = attributeKey + ":" + msg.getMessageId();
        } else {
          key = msg.getMessageId();
        }

        c.output(KV.of(key, value));
      } catch (Exception e) {
        LOG.error("Failed to transform message to Redis String: {}", e.getMessage(), e);
        c.output(REDIS_FAILURE_OUT, c.element());
      }
    }
  }

  /** Safe transformation to Redis Hash format with error handling. */
  static class SafeRedisHashTransform extends DoFn<PubsubMessage, KV<String, KV<String, String>>> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      try {
        PubsubMessage msg = c.element();
        String key = msg.getAttributeMap().getOrDefault("key", msg.getMessageId());
        String field = msg.getAttributeMap().getOrDefault("field", "value");
        String value = new String(msg.getPayload(), java.nio.charset.StandardCharsets.UTF_8);

        c.output(KV.of(key, KV.of(field, value)));
      } catch (Exception e) {
        LOG.error("Failed to transform message to Redis Hash: {}", e.getMessage(), e);
        c.output(REDIS_FAILURE_OUT, c.element());
      }
    }
  }

  /** Safe transformation to Redis Logs format with error handling. */
  static class SafeRedisLogsTransform extends DoFn<PubsubMessage, KV<String, KV<String, String>>> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      try {
        PubsubMessage msg = c.element();
        String timestamp = String.valueOf(System.currentTimeMillis());
        String key = "logs:" + msg.getAttributeMap().getOrDefault("log_id", timestamp);
        String field = msg.getAttributeMap().getOrDefault("field", timestamp);
        String value = new String(msg.getPayload(), java.nio.charset.StandardCharsets.UTF_8);

        c.output(KV.of(key, KV.of(field, value)));
      } catch (Exception e) {
        LOG.error("Failed to transform message to Redis Logs: {}", e.getMessage(), e);
        c.output(REDIS_FAILURE_OUT, c.element());
      }
    }
  }

  /** Safe transformation to Redis Streams format with error handling. */
  static class SafeRedisStreamsTransform
      extends DoFn<PubsubMessage, KV<String, Map<String, String>>> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      try {
        PubsubMessage msg = c.element();
        String streamKey = msg.getAttributeMap().getOrDefault("stream", "default-stream");
        String payload = new String(msg.getPayload(), java.nio.charset.StandardCharsets.UTF_8);

        Map<String, String> fields = new HashMap<>();
        fields.put("payload", payload);
        fields.put("message_id", msg.getMessageId());
        msg.getAttributeMap().forEach(fields::put);
        c.output(KV.of(streamKey, fields));
      } catch (Exception e) {
        LOG.error("Failed to transform message to Redis Streams: {}", e.getMessage(), e);
        c.output(REDIS_FAILURE_OUT, c.element());
      }
    }
  }

  /**
   * The {@link PubSubMessageTransform} transform processes PubSubMessages using an optional UDF.
   *
   * <p>If a UDF is provided, it will be applied to the message payload. Messages that fail UDF
   * processing will be sent to the dead-letter output.
   */
  public static class PubSubMessageTransform
      extends PTransform<PCollection<PubsubMessage>, PCollectionTuple> {

    private final String javascriptTextTransformGcsPath;
    private final String javascriptTextTransformFunctionName;
    private final Integer javascriptTextTransformReloadIntervalMinutes;

    public PubSubMessageTransform(
        String javascriptTextTransformGcsPath,
        String javascriptTextTransformFunctionName,
        Integer javascriptTextTransformReloadIntervalMinutes) {
      this.javascriptTextTransformGcsPath = javascriptTextTransformGcsPath;
      this.javascriptTextTransformFunctionName = javascriptTextTransformFunctionName;
      this.javascriptTextTransformReloadIntervalMinutes =
          javascriptTextTransformReloadIntervalMinutes;
    }

    @Override
    public PCollectionTuple expand(PCollection<PubsubMessage> input) {
      // Map incoming messages to FailsafeElement
      PCollection<FailsafeElement<PubsubMessage, String>> failsafeElements =
          input.apply("MapToRecord", ParDo.of(new PubsubMessageToFailsafeElementFn()));

      // Apply UDF if provided
      if (javascriptTextTransformGcsPath != null) {
        return failsafeElements.apply(
            "InvokeUDF",
            JavascriptTextTransformer.FailsafeJavascriptUdf.<PubsubMessage>newBuilder()
                .setFileSystemPath(javascriptTextTransformGcsPath)
                .setFunctionName(javascriptTextTransformFunctionName)
                .setReloadIntervalMinutes(javascriptTextTransformReloadIntervalMinutes)
                .setSuccessTag(UDF_OUT)
                .setFailureTag(UDF_DEADLETTER_OUT)
                .build());
      } else {
        // No UDF - just pass through
        return PCollectionTuple.of(UDF_OUT, failsafeElements)
            .and(
                UDF_DEADLETTER_OUT,
                input.getPipeline().apply(Create.empty(FAILSAFE_ELEMENT_CODER)));
      }
    }
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
      LOG.debug(
          "Converting PubsubMessage to FailsafeElement. MessageId: {}, Payload size: {} bytes",
          message.getMessageId(),
          message.getPayload().length);
      context.output(
          FailsafeElement.of(
              message, new String(message.getPayload(), java.nio.charset.StandardCharsets.UTF_8)));
    }
  }
}
