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

import static com.google.cloud.teleport.v2.kafka.utils.KafkaCommonUtils.getKafkaCredentialsFromVault;
import static com.google.cloud.teleport.v2.transforms.FormatMessageTransform.readFromPubsub;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.v2.coders.FailsafeElementCoder;
import com.google.cloud.teleport.v2.kafka.utils.SslProducerFactoryFn;
import com.google.cloud.teleport.v2.options.PubsubToKafkaOptions;
import com.google.cloud.teleport.v2.transforms.FormatMessageTransform;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link com.google.cloud.teleport.v2.templates.PubsubToKafka} streaming pipeline reading data
 * from Google Cloud PubSub and publishes to Kafka. PubSub Input topic, Kafka output topic,
 * Bootstrap server is specified by the user as template parameters. <br>
 * Kafka may be configured with SSL encrypted connection, in this case a Vault secret storage with
 * credentials should be provided. URL to credentials and Vault token are specified by the user as
 * template parameters.
 *
 * <p><b>Pipeline Requirements</b>
 *
 * <ul>
 *   <li>The PubSub input topic exists.
 *   <li>Kafka Bootstrap Server.
 *   <li>Kafka output Topic exists.
 *   <li>(Optional) An existing HashiCorp Vault
 *   <li>(Optional) A configured secure SSL connection for Kafka
 * </ul>
 *
 * <p>Check out <a
 * href="https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/v2/pubsub-to-kafka/README_PubSub_to_Kafka.md">README</a>
 * for instructions on how to use or modify this template.
 */
@Template(
    name = "PubSub_to_Kafka",
    category = TemplateCategory.STREAMING,
    displayName = "Pub/Sub to Kafka",
    description =
        "A streaming pipeline which inserts data from a Pub/Sub Topic and "
            + "transforms them using a JavaScript user-defined function (UDF), "
            + "and writes them to kafka topic",
    optionsClass = PubsubToKafkaOptions.class,
    flexContainerName = "pubsub-to-kafka",
    contactInformation = "https://cloud.google.com/support")
public class PubsubToKafka {

  /* Logger for class.*/
  private static final Logger LOG = LoggerFactory.getLogger(PubsubToKafka.class);

  /**
   * Main entry point for pipeline execution.
   *
   * @param args Command line arguments to the pipeline.
   */
  public static void main(String[] args) {
    PubsubToKafkaOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(PubsubToKafkaOptions.class);

    run(options);
  }

  /**
   * Runs a pipeline which reads message from Pub/Sub and writes to Kafka.
   *
   * @param options arguments to the pipeline
   */
  public static PipelineResult run(PubsubToKafkaOptions options) {
    String inputTopic = options.getInputTopic();

    checkArgument(inputTopic.trim().length() > 0, "Pub/Sub inputTopic cannot be an empty string.");

    String bootstrapServer = options.getBootstrapServer();

    checkArgument(
        bootstrapServer.trim().length() > 0, "bootstrapServers cannot be an empty string.");

    String outputTopic = options.getOutputTopic();

    checkArgument(outputTopic.trim().length() > 0, "Kafka Output Topic cannot be an empty string.");

    // Configure Kafka Topic properties
    Map<String, String> sslConfig = null;
    if (options.getSecretStoreUrl() != null && options.getVaultToken() != null) {
      Map<String, Map<String, String>> credentials =
          getKafkaCredentialsFromVault(options.getSecretStoreUrl(), options.getVaultToken());
      sslConfig = credentials.get(PubsubKafkaConstants.SSL_CREDENTIALS);
    } else {
      LOG.warn(
          "No information to retrieve Kafka credentials was provided. "
              + "Trying to initiate an unauthorized connection.");
    }

    // Create the pipeline
    Pipeline pipeline = Pipeline.create(options);
    // Register the coder for pipeline
    FailsafeElementCoder<String, String> coder =
        FailsafeElementCoder.of(
            NullableCoder.of(StringUtf8Coder.of()), NullableCoder.of(StringUtf8Coder.of()));

    CoderRegistry coderRegistry = pipeline.getCoderRegistry();
    coderRegistry.registerCoderForType(coder.getEncodedTypeDescriptor(), coder);

    TypeDescriptor<String> stringTypeDescriptor = TypeDescriptors.strings();

    LOG.info(
        "Starting PubSub-To-Kafka Pipeline with parameters bootstrap server:{} "
            + "input pubsub topic:{} output kafka topic:{} ",
        options.getBootstrapServer(),
        options.getInputTopic(),
        options.getOutputTopic());

    /*
     * Steps:
     *  1) Read messages in from Pub/Sub
     *  2) Transform message payload via UDF
     *  3) Write successful records out to Kafka
     *  4) Write failed records out to Pub/Sub dead-letter topic
     */
    PCollectionTuple appliedUdf =
        pipeline
            /* Step #1: Read messages in from PubSub */
            .apply("readFromPubsub", readFromPubsub(inputTopic))
            /* Step #2: Transform the PubSub Messages via UDF */
            .apply("applyUDF", new FormatMessageTransform.UdfProcess(options));

    if (sslConfig != null) {

      appliedUdf
          .get(PubsubKafkaConstants.UDF_OUT)
          .apply(
              "getSuccessUDFOutElements",
              MapElements.into(stringTypeDescriptor).via(FailsafeElement::getPayload))
          .setCoder(NullableCoder.of(StringUtf8Coder.of()))
          .apply(
              "writeSuccessMessages",
              KafkaIO.<Void, String>write()
                  .withBootstrapServers(options.getBootstrapServer())
                  .withTopic(options.getOutputTopic())
                  .withProducerFactoryFn(new SslProducerFactoryFn(sslConfig))
                  .withValueSerializer(StringSerializer.class)
                  .values());

    } else {

      appliedUdf
          .get(PubsubKafkaConstants.UDF_OUT)
          .apply(
              "getSuccessUDFOutElements",
              MapElements.into(stringTypeDescriptor).via(FailsafeElement::getPayload))
          .setCoder(NullableCoder.of(StringUtf8Coder.of()))
          .apply(
              "writeSuccessMessages",
              KafkaIO.<Void, String>write()
                  .withBootstrapServers(options.getBootstrapServer())
                  .withTopic(options.getOutputTopic())
                  .withValueSerializer(StringSerializer.class)
                  .values());
    }
    /* Step #4: Write failed messages out to Pub/Sub */
    if (options.getOutputDeadLetterTopic() != null) {
      appliedUdf
          .get(PubsubKafkaConstants.UDF_DEADLETTER_OUT)
          .apply(
              "getFailedMessages",
              MapElements.into(stringTypeDescriptor).via(FailsafeElement::getOriginalPayload))
          .setCoder(NullableCoder.of(StringUtf8Coder.of()))
          .apply(
              "writeFailureMessages",
              PubsubIO.writeStrings().to(options.getOutputDeadLetterTopic()));
    }

    return pipeline.run();
  }
}
