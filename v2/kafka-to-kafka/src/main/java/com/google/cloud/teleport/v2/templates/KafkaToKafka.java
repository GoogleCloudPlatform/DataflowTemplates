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

import static com.google.cloud.teleport.v2.kafka.utils.KafkaCommonUtils.getKafkaCredentialsFromVault;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.v2.common.UncaughtExceptionLogger;
import com.google.cloud.teleport.v2.kafka.utils.SslConsumerFactoryFn;
import com.google.cloud.teleport.v2.options.KafkaToKafkaOptions;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Template(
    name = "Kafka_to_Kafka",
    category = TemplateCategory.STREAMING,
    displayName = "Kafka to Kafka",
    description = "A pipeline that writes data to a google managed kafka from an on-prem Kafka",
    optionsClass = KafkaToKafkaOptions.class,
    flexContainerName = "kafka-to-gmk",
    contactInformation = "https://cloud.google.com/support",
    hidden = true,
    streaming = true)
public class KafkaToKafka {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaToKafka.class);

  public static void main(String[] args) {
    UncaughtExceptionLogger.register();
    KafkaToKafkaOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(KafkaToKafkaOptions.class);
    run(options);
  }

  public static PipelineResult run(KafkaToKafkaOptions options) {

    if (options.getMigrationType().split("to-")[1].equals("GMK")) {
      checkArgument(options.getAuthenticationMethod().equals("secret manager"),
          "GMK only supports authentication through secret manager");
      checkArgument(options.getProjectNumber().trim().length() > 0,
          "project number required for secret manager");
      checkArgument(options.getSecretIdSinkUsername().trim().length() > 0,
          "secret id required to access username for sink kafka");
      checkArgument(options.getVersionIdSinkUsername().trim().length() > 0,
          "versionId for the sink require to access username from secret manager");
      checkArgument(options.getSecretIdSinkPassword().trim().length() > 0,
          "secret id requited to access password to authenticate to sink kafka");
      checkArgument(options.getVersionIdSinkPassword().trim().length() > 0,
          "versionId for password required for kafka authentication");

    }

    if (options.getMigrationType().equals("nonGMK-to-nonGMK")) {
      if (options.getSecretStoreUrl().trim().length() >= 0) {
        checkArgument(options.getSecretIdSourceUsername().trim().length() == 0,
            "authentication to kafka with either secret manager or truststore.");
        checkArgument(options.getVaultToken().trim().length() > 0,
            "vault token needed for kafka authentication");
      }

    }

    String outputTopic;
    String inputTopic = options.getInputTopic();

    checkArgument(inputTopic.trim().length() > 0,
        "Kafka input topic to read from cannot be empty.");

    String sourceBootstrapServers = options.getSourceBootstrapServers();

    checkArgument(sourceBootstrapServers.trim().length() > 0,
        "source bootstrap servers cannot be empty");

    if (options.getOutputTopic() != null) {
      outputTopic = options.getOutputTopic();

    } else {
      outputTopic = inputTopic;
    }

    String sinkBootstrapServer = options.getSinkBootstrapServer();

    checkArgument(sinkBootstrapServer.trim().length() > 0, "sink bootstrap server cannot be empty");

    Map<String, String> sslConfigRead = null;
    if (options.getSecretStoreUrl() != null && options.getVaultToken() != null) {
      Map<String, Map<String, String>> credentials = getKafkaCredentialsFromVault(
          options.getSecretStoreUrl(), options.getVaultToken());
      sslConfigRead = credentials.get(KafkaConstants.SSL_CREDENTIALS);
    } else {
      LOG.warn(
          "No info to retrieve Kafka source credentials. Initiating unauthorized connection"
      );
    }

    Pipeline pipeline = Pipeline.create(options);
    PCollection<KV<Void, String>> records;
    if (sslConfigRead != null) {
      records =
          pipeline.apply(
              "Read from Kafka",
              KafkaIO.<Void, String>read()
                  .withBootstrapServers(sourceBootstrapServers)
                  .withTopic(inputTopic)
                  .withValueDeserializer(StringDeserializer.class)
                  .withConsumerFactoryFn(new SslConsumerFactoryFn(sslConfigRead))
                  .withConsumerConfigUpdates(
                      options.getAuthenticationMethod().equals("secret manager") ?
                          ConsumerProperties.get(options)
                          : ImmutableMap.of(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"))
                  .withoutMetadata());
    } else {

      records =
          pipeline.apply(
              "Read from Kafka",
              KafkaIO.<Void, String>read()
                  .withBootstrapServers(sourceBootstrapServers)
                  .withTopic(inputTopic)
                  .withValueDeserializer(StringDeserializer.class)
                  .withConsumerConfigUpdates(
                      options.getAuthenticationMethod().equals("secret manager") ?
                          ConsumerProperties.get(options)
                          : ImmutableMap.of(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"))
                  .withoutMetadata());
    }
    KafkaIO.Write<Void, String> kafkaWrite = KafkaIO.<Void, String>write()
          .withBootstrapServers(options.getSinkBootstrapServer())
          .withTopic(options.getOutputTopic())
          .withValueSerializer(StringSerializer.class);
      records.apply("write messages to kafka", kafkaWrite.withProducerConfigUpdates(
          options.getAuthenticationMethod().equals("secret manager") ?
              ProducerProperties.get(options)
              : ImmutableMap.of(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")));

    return pipeline.run();
  }
}
