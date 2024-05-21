/*
 * Copyright (C) 2024 Google LLC
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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.v2.common.UncaughtExceptionLogger;
import com.google.cloud.teleport.v2.options.KafkaToKafkaOptions;
import java.io.IOException;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Template(
    name = "Kafka_to_Kafka",
    category = TemplateCategory.STREAMING,
    displayName = "Kafka to Kafka",
    description = "A pipeline that writes data to a kafka destination from another kafka source",
    optionsClass = KafkaToKafkaOptions.class,
    flexContainerName = "kafka-to-kafka",
    contactInformation = "https://cloud.google.com/support",
    hidden = true,
    streaming = true)
public class KafkaToKafka {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaToKafka.class);

  public static void main(String[] args) throws IOException {
    UncaughtExceptionLogger.register();
    KafkaToKafkaOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(KafkaToKafkaOptions.class);
    run(options);
  }

  public static PipelineResult run(KafkaToKafkaOptions options) throws IOException {
    // if (options.getMigrationType() != null) {
    //   if (options.getSourceAuthenticationMethod() != null) {
    //
    //     if (options.getMigrationType().equals("GMK-to-GMK")) {
    //       checkArgument(
    //           options.getSourceAuthenticationMethod().equals("SASL_PLAIN"),
    //           "GMK only supports SASL_PLAIN authentication");
    //       checkArgument(
    //           options.getDestinationAuthenticationMethod().equals("SASL_PLAIN"),
    //           "GMK only supports SASL_PLAIN authentication");
    //       checkArgument(
    //           options.getSourceUsernameSecretId().trim().length() > 0,
    //           "version id required to access username to GMK");
    //       checkArgument(
    //           options.getSourcePasswordSecretId().trim().length() > 0,
    //           "version id required to access password to authenticate to GMK");
    //     }
    //
    //     if (options.getMigrationType().split("to-")[1].equals("GMK")) {
    //       checkArgument(
    //           options.getDestinationUsernameSecretId().trim().length() > 0,
    //           "version id required to access username to GMK");
    //
    //       checkArgument(
    //           options.getDestinationPasswordSecretId().trim().length() > 0,
    //           "version id required to access password to authenticate to GMK");
    //     }
    //     if (options.getMigrationType().equals("nonGMK-to-nonGMK")) {
    //       if (options.getSourceAuthenticationMethod().equals("SASL_PLAIN")) {
    //         checkArgument(
    //             options.getSourceUsernameSecretId().trim().length() > 0,
    //             "version id required to access username for source Kafka");
    //         checkArgument(
    //             options.getSourcePasswordSecretId().trim().length() > 0,
    //             "version id required to access password for source kafka");
    //         checkArgument(
    //             options.getDestinationUsernameSecretId().trim().length() > 0,
    //             "version id required to access username for destination Kafka");
    //         checkArgument(
    //             options.getDestinationPasswordSecretId().trim().length() > 0,
    //             "version id required to access password for destination kafka");
    //       }
    //         else {
    //           checkArgument(options.getSourceTruststoreLocation().trim().length() > 0,
    //               "trust store certificate required for ssl authentication");
    //           checkArgument(
    //               options.getSourceTruststorePasswordSecretId().trim().length() > 0,
    //               "trust store password required for accessing truststore");
    //           checkArgument(options.getSourceKeystoreLocation().trim().length() > 0,
    //               "key store location required for ssl authentication");
    //           checkArgument(options.getSourceKeystorePasswordSecretId().trim().length() > 0,
    //               "key store password required to access key store");
    //           checkArgument(options.getSourceKeyPasswordSecretId().trim().length() > 0,
    //               "source key password secret id version required for SSL authentication");
    //           checkArgument(options.getDestinationTruststoreLocation().trim().length() > 0,
    //               "trust store certificate required for ssl authentication");
    //           checkArgument(
    //               options.getDestinationTruststorePasswordSecretId().trim().length() > 0,
    //               "trust store password required for accessing truststore");
    //           checkArgument(options.getDestinationKeystoreLocation().trim().length() > 0,
    //               "key store location required for ssl authentication");
    //           checkArgument(options.getDestinationKeystorePasswordSecretId().trim().length() > 0,
    //               "key store password required to access key store");
    //           checkArgument(options.getDestinationKeyPasswordSecretId().trim().length() > 0,
    //               "source key password secret id version required for SSL authentication");
    //
    //         }
    //       }
    //     }
    //   }
    String outputTopic;
    String inputTopic = options.getInputTopic();

    checkArgument(
        inputTopic.trim().length() > 0, "Kafka input topic to read from cannot be empty.");

    String sourceBootstrapServers = options.getSourceBootstrapServers();

    checkArgument(
        sourceBootstrapServers.trim().length() > 0, "source bootstrap servers cannot be empty");

    if (options.getOutputTopic() != null) {
      outputTopic = options.getOutputTopic();

    } else {
      outputTopic = inputTopic;
    }

    String destinationBootstrapServer = options.getDestinationBootstrapServer();

    checkArgument(
        destinationBootstrapServer.trim().length() > 0,
        "destination bootstrap server cannot be empty");

    Pipeline pipeline = Pipeline.create(options);
    // if (options.getSourceAuthenticationMethod() != null) {
    //   PCollection<KV<byte[], byte[]>> records;
    //   records =
    pipeline.apply(
        "Read from Kafka",
        KafkaIO.<byte[], byte[]>read()
            .withBootstrapServers(sourceBootstrapServers)
            .withTopic(inputTopic)
            .withKeyDeserializer(ByteArrayDeserializer.class)
            .withValueDeserializer(ByteArrayDeserializer.class)
            .withConsumerConfigUpdates(ConsumerProperties.get(options))
            .withoutMetadata());
    // KafkaIO.Write<byte[], byte[]> kafkaWrite =
    //     KafkaIO.<byte[], byte[]>write()
    //         .withBootstrapServers(destinationBootstrapServer)
    //         .withTopic(outputTopic)
    //         .withKeySerializer(ByteArraySerializer.class)
    //         .withValueSerializer(ByteArraySerializer.class);
    // records.apply(
    //     "write messages to kafka",
    //     kafkaWrite.withProducerConfigUpdates(ProducerProperties.get(options)));
    // } else {
    //   PCollection<KV<byte[], byte[]>> records;
    //   records =
    //       pipeline.apply(
    //           "Read from Kafka",
    //           KafkaIO.<byte[], byte[]>read()
    //               .withBootstrapServers(sourceBootstrapServers)
    //               .withTopic(inputTopic)
    //               .withKeyDeserializer(ByteArrayDeserializer.class)
    //               .withValueDeserializer(ByteArrayDeserializer.class)
    //               .withConsumerConfigUpdates(
    //                   ImmutableMap.of(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"))
    //               .withoutMetadata());
    //
    //   KafkaIO.Write<byte[], byte[]> kafkaWrite =
    //       KafkaIO.<byte[], byte[]>write()
    //           .withBootstrapServers(destinationBootstrapServer)
    //           .withTopic(outputTopic)
    //           .withKeySerializer(ByteArraySerializer.class)
    //           .withValueSerializer(ByteArraySerializer.class);
    //   records.apply(
    //       "write messages to kafka",
    //       kafkaWrite.withProducerConfigUpdates(
    //           ImmutableMap.of(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")));
    // }


    return pipeline.run();
  }

}