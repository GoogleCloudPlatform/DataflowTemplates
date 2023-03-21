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
package com.google.cloud.teleport.v2.templates;

import com.google.cloud.teleport.v2.transforms.JavascriptTextTransformer;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.io.redis.RedisConnectionConfiguration;
import org.apache.beam.sdk.io.redis.RedisIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link PubSubToRedis} pipeline is a streaming pipeline which ingests data in Bytes from
 * PubSub, applies a Javascript UDF if provided and inserts resulting records as KV in Redis.
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
 *  --redisPort=${REDIS_PORT}"
 * </pre>
 */
public class PubSubToRedis {
  /**
   * Options supported by {@link PubSubToRedis}
   *
   * <p>Inherits standard configuration options.
   */

  /** The log to output status messages to. */
  private static final Logger LOG = LoggerFactory.getLogger(PubSubToRedis.class);

  private static String attributeKey;
  private static String messageId;
  private static String key;

  /**
   * The {@link PubSubToRedisOptions} class provides the custom execution options passed by the
   * executor at the command-line.
   *
   * <p>Inherits standard configuration options, options from {@link
   * JavascriptTextTransformer.JavascriptTextTransformerOptions}.
   */
  public interface PubSubToRedisOptions
      extends JavascriptTextTransformer.JavascriptTextTransformerOptions, PipelineOptions {
    @Description(
        "The Cloud Pub/Sub subscription to consume from. "
            + "The name should be in the format of "
            + "projects/<project-id>/subscriptions/<subscription-name>.")
    ValueProvider<String> getInputSubscription();

    void setInputSubscription(ValueProvider<String> value);

    @Description("The Cloud Pub/Sub topic to read from.")
    ValueProvider<String> getInputTopic();

    void setInputTopic(ValueProvider<String> value);

    @Description(
        "This determines whether the template reads from " + "a pub/sub subscription or a topic")
    @Default.Boolean(true)
    Boolean getUseSubscription();

    void setUseSubscription(Boolean value);

    @Description("Redis Host")
    @Default.String("127.0.0.1")
    ValueProvider<String> getRedisHost();

    void setRedisHost(ValueProvider<String> redisHost);

    @Description("Redis Port")
    @Default.Integer(6379)
    ValueProvider<Integer> getRedisPort();

    void setRedisPort(ValueProvider<Integer> redisPort);

    @Description("Redis Auth")
    @Default.String("")
    ValueProvider<String> getRedisAuth();

    void setRedisAuth(ValueProvider<String> redisAuth);

    @Description("Duration in seconds of a pane in a Global window")
    @Default.Long(5)
    Long getWindowDuration();

    void setWindowDuration(Long value);
  }

  /**
   * Main entry point for executing the pipeline.
   *
   * @param args The command-line arguments to the pipeline.
   */
  public static void main(String[] args) {

    // Parse the user options passed from the command-line.
    PubSubToRedisOptions pubSubToRedisOptions =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(PubSubToRedisOptions.class);

    // Create the pipeline
    Pipeline pipeline = Pipeline.create(pubSubToRedisOptions);

    PCollection<PubsubMessage> input;

    /*
     * Steps: 1) Read PubSubMessage with attributes and messageId from input PubSub subscription or Topic.
     *        2) Extract PubSubMessage message to PCollection<String>.
     *        3) Transform PCollection<String> to PCollection<KV<String, String>> so it can be consumed by RedisIO
     *        4) Write to Redis using SET
     *
     */

    LOG.info("Reading from subscription: " + pubSubToRedisOptions.getInputSubscription());

    if (pubSubToRedisOptions.getUseSubscription()) {
      input =
          pipeline.apply(
              "Read PubSub Events",
              PubsubIO.readMessagesWithAttributesAndMessageId()
                  .fromSubscription(pubSubToRedisOptions.getInputSubscription()));
    } else {
      input =
          pipeline.apply(
              "Read PubSub Events",
              PubsubIO.readMessagesWithAttributesAndMessageId()
                  .fromTopic(pubSubToRedisOptions.getInputTopic()));
    }

    // Create a PCollection from string a transform to pubsub message format
    input
        .apply(
            "Windowing pipeline with sessions window",
            Window.<PubsubMessage>into(new GlobalWindows())
                .triggering(
                    Repeatedly.forever(
                        AfterProcessingTime.pastFirstElementInPane()
                            .plusDelayOf(
                                Duration.standardSeconds(
                                    pubSubToRedisOptions.getWindowDuration()))))
                .discardingFiredPanes())
        .apply(
            "PubSubMessage payload extraction",
            ParDo.of(
                new DoFn<PubsubMessage, String>() {
                  @ProcessElement
                  public void processElement(
                      @Element PubsubMessage pubsubMessage, OutputReceiver<String> receiver) {
                    String element = new String(pubsubMessage.getPayload());
                    messageId = pubsubMessage.getMessageId();
                    LOG.info("PubSubMessage messageId: " + messageId);
                    LOG.info("PubSubMessage payload: " + element);
                    if (pubsubMessage.getAttribute("key") != null) {
                      attributeKey = pubsubMessage.getAttribute("key");
                      LOG.info("PubSubMessage attributeKey: " + attributeKey);
                      key = attributeKey + ":" + messageId;
                    } else {
                      attributeKey = "";
                      key = messageId;
                    }
                    receiver.output(element);
                  }
                }))
        .apply(
            "Transform to KV",
            MapElements.into(
                    TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.strings()))
                .via(record -> KV.of(key, record)))
        .apply(
            "Write to Redis",
            RedisIO.write()
                .withMethod(RedisIO.Write.Method.SET)
                .withConnectionConfiguration(
                    RedisConnectionConfiguration.create()
                        .withHost(pubSubToRedisOptions.getRedisHost())
                        .withPort(pubSubToRedisOptions.getRedisPort())
                        .withAuth(pubSubToRedisOptions.getRedisAuth())));

    // run the pipeline
    pipeline.run(pubSubToRedisOptions);
  }

  static class FormatAsPubSubMessage extends SimpleFunction<Long, PubsubMessage> {
    @Override
    public PubsubMessage apply(Long message) {
      return new PubsubMessage(String.valueOf(message).getBytes(), null);
    }
  }
}
