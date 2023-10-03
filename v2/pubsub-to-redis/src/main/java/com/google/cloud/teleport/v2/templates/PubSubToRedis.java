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
import com.google.cloud.teleport.v2.common.UncaughtExceptionLogger;
import com.google.cloud.teleport.v2.templates.io.RedisHashIO;
import com.google.cloud.teleport.v2.templates.transforms.MessageTransformation;
import com.google.cloud.teleport.v2.transforms.JavascriptTextTransformer;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.io.redis.RedisConnectionConfiguration;
import org.apache.beam.sdk.io.redis.RedisIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
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
    preview = true)
public class PubSubToRedis {
  /*
   * Options supported by {@link PubSubToRedis}
   *
   * <p>Inherits standard configuration options.
   */

  /** The log to output status messages to. */
  private static final Logger LOG = LoggerFactory.getLogger(PubSubToRedis.class);

  /**
   * The {@link PubSubToRedisOptions} class provides the custom execution options passed by the
   * executor at the command-line.
   *
   * <p>Inherits standard configuration options, options from {@link
   * JavascriptTextTransformer.JavascriptTextTransformerOptions}.
   */
  public interface PubSubToRedisOptions
      extends JavascriptTextTransformer.JavascriptTextTransformerOptions, PipelineOptions {
    @TemplateParameter.PubsubSubscription(
        order = 1,
        description = "Pub/Sub input subscription",
        helpText =
            "Pub/Sub subscription to read the input from, in the format of"
                + " 'projects/your-project-id/subscriptions/your-subscription-name'",
        example = "projects/your-project-id/subscriptions/your-subscription-name")
    String getInputSubscription();

    void setInputSubscription(String value);

    @TemplateParameter.Text(
        order = 2,
        description = "Redis DB Host",
        helpText = "Redis database host.",
        example = "your.cloud.db.redislabs.com")
    @Default.String("127.0.0.1")
    @Validation.Required
    String getRedisHost();

    void setRedisHost(String redisHost);

    @TemplateParameter.Integer(
        order = 3,
        description = "Redis DB Port",
        helpText = "Redis database port.",
        example = "12345")
    @Default.Integer(6379)
    @Validation.Required
    int getRedisPort();

    void setRedisPort(int redisPort);

    @TemplateParameter.Text(
        order = 4,
        description = "Redis DB Password",
        helpText = "Redis database password.")
    @Default.String("")
    @Validation.Required
    String getRedisPassword();

    void setRedisPassword(String redisPassword);

    @TemplateParameter.Boolean(
        order = 5,
        optional = true,
        description = "Redis ssl enabled",
        helpText = "Redis database ssl parameter.")
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
            "Supported Redis sinks are STRING_SINK, HASH_SINK, STREAMS_SINK and LOGGING_SINK",
        example = "STRING_SINK")
    @Default.Enum("STRING_SINK")
    RedisSinkType getRedisSinkType();

    void setRedisSinkType(RedisSinkType redisSinkType);

    @TemplateParameter.Integer(
        order = 7,
        optional = true,
        description = "Redis connection timeout in milliseconds",
        helpText = "Redis connection timeout in milliseconds.",
        example = "2000")
    @Default.Integer(2000)
    int getConnectionTimeout();

    void setConnectionTimeout(int timeout);

    @TemplateParameter.Long(
        order = 8,
        optional = true,
        description = "Hash key expiration time in sec (ttl)",
        helpText =
            "Key expiration time in sec (ttl, default for HASH_SINK is -1 i.e. no expiration)")
    @Default.Long(-1L)
    Long getTtl();

    void setTtl(Long ttl);
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

    PCollection<PubsubMessage> input;

    RedisConnectionConfiguration redisConnectionConfiguration =
        RedisConnectionConfiguration.create()
            .withHost(options.getRedisHost())
            .withPort(options.getRedisPort())
            .withAuth(options.getRedisPassword())
            .withTimeout(options.getConnectionTimeout())
            .withSSL(options.getSslEnabled());

    /*
     * Steps: 1) Read PubSubMessage with attributes and messageId from input PubSub subscription.
     *        2) Extract PubSubMessage message to PCollection<String>.
     *        3) Transform PCollection<String> to PCollection<KV<String, String>> so it can be consumed by RedisIO
     *        4) Write to Redis using SET
     *
     */

    LOG.info(
        "Starting PubSub-To-Redis Pipeline. Reading from subscription: {}",
        options.getInputSubscription());

    input =
        pipeline.apply(
            "Read PubSub Events",
            MessageTransformation.readFromPubSub(options.getInputSubscription()));

    if (options.getRedisSinkType().equals(STRING_SINK)) {
      PCollection<String> pCollectionString =
          input.apply(
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
          input.apply(
              "Map to Redis Hash", ParDo.of(new MessageTransformation.MessageToRedisHash()));

      pCollectionHash.apply(
          "Write to " + HASH_SINK.name(),
          RedisHashIO.write()
              .withConnectionConfiguration(redisConnectionConfiguration)
              .withTtl(options.getTtl()));
    }
    if (options.getRedisSinkType().equals(LOGGING_SINK)) {
      PCollection<KV<String, KV<String, String>>> pCollectionHash =
          input.apply(
              "Map to Redis Logs", ParDo.of(new MessageTransformation.MessageToRedisLogs()));

      pCollectionHash.apply(
          "Write to " + LOGGING_SINK.name(),
          RedisHashIO.write()
              .withConnectionConfiguration(redisConnectionConfiguration)
              .withTtl(options.getTtl()));
    }
    if (options.getRedisSinkType().equals(STREAMS_SINK)) {
      PCollection<KV<String, Map<String, String>>> pCollectionStreams =
          input.apply(
              "Map to Redis Streams", ParDo.of(new MessageTransformation.MessageToRedisStreams()));

      pCollectionStreams.apply(
          "Write to " + STREAMS_SINK.name(),
          RedisIO.writeStreams().withConnectionConfiguration(redisConnectionConfiguration));
    }
    // Execute the pipeline and return the result.
    return pipeline.run();
  }
}
