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
import com.google.cloud.teleport.v2.kafka.utils.FileAwareConsumerFactoryFn;
import com.google.cloud.teleport.v2.kafka.utils.FileAwareProducerFactoryFn;
import com.google.cloud.teleport.v2.kafka.utils.KafkaTopicUtils;
import com.google.cloud.teleport.v2.kafka.values.KafkaAuthenticationMethod;
import com.google.cloud.teleport.v2.options.KafkaToKafkaOptions;
import java.io.IOException;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
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
    skipOptions = {"readBootstrapServers", "kafkaReadTopics", "kafkaReadAuthenticationMode","writeBootstrapServers","kafkaWriteTopics"},
    hidden = true,
    streaming = false)
public class KafkaToKafka {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaToKafka.class);

  public static void main(String[] args) throws IOException {
    UncaughtExceptionLogger.register();
    KafkaToKafkaOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(KafkaToKafkaOptions.class);
    run(options);
  }


  public static PipelineResult run(KafkaToKafkaOptions options) throws IOException {

    if (options.getSourceAuthenticationMethod().equals(KafkaAuthenticationMethod.SASL_PLAIN)) {

      checkArgument(
          options.getSourceUsernameSecretId().trim().length() > 0,
          "sourceUsernameSecretId required to access username for source Kafka");
      checkArgument(
          options.getSourcePasswordSecretId().trim().length() > 0,
          "sourcePasswordSecretId required to access password for source kafka");
    } else if  (options.getSourceAuthenticationMethod().equals(KafkaAuthenticationMethod.SSL)) {
        checkArgument(
            options.getSourceTruststoreLocation().trim().length() > 0,
            "sourceTruststoreLocation for trust store certificate required for ssl authentication");
        checkArgument(
            options.getSourceTruststorePasswordSecretId().trim().length() > 0,
            "sourceTruststorePassword for trust store password required for accessing truststore");
        checkArgument(
            options.getSourceKeystoreLocation().trim().length() > 0,
            "sourceKeystoreLocation for key store location required for ssl authentication");
        checkArgument(
            options.getSourceKeystorePasswordSecretId().trim().length() > 0,
            "sourceKeystorePassword for key store password required to access key store");
        checkArgument(
            options.getSourceKeyPasswordSecretId().trim().length() > 0,
            "sourceKeyPasswordSecretId version for key password required for SSL authentication");
    } else {
      throw new UnsupportedOperationException(
          "Authentication method not supported: " + options.getSourceAuthenticationMethod());
    }
      if (options.getDestinationAuthenticationMethod().equals(KafkaAuthenticationMethod.SASL_PLAIN)) {
        checkArgument(options.getDestinationUsernameSecretId().trim().length() > 0,
            "destinationUsernameSecretId required to access username for source Kafka");
        checkArgument(options.getDestinationPasswordSecretId().trim().length() > 0,
            "destinationPasswordSecretId required to access password for destination Kafka");
      } else if (options.getDestinationAuthenticationMethod().equals(KafkaAuthenticationMethod.SSL)) {
        checkArgument(
            options.getDestinationTruststoreLocation().trim().length() > 0,
            "destinationTruststoreLocation for trust store certificate required for ssl authentication");
        checkArgument(
            options.getDestinationTruststorePasswordSecretId().trim().length() > 0,
            "destinationTruststorePasswordSecretId for trust store password required for accessing truststore");
        checkArgument(
            options.getDestinationKeystoreLocation().trim().length() > 0,
            "destinationKeystoreLocation for key store location required for ssl authentication");
        checkArgument(
            options.getDestinationKeystorePasswordSecretId().trim().length() > 0,
            "destinationKeystorePasswordSecretId for key store password required to access key store");
        checkArgument(
            options.getDestinationKeyPasswordSecretId().trim().length() > 0,
            "destinationKeyPasswordSecretId for source key password secret id version required for SSL authentication");
    } else {
        throw new UnsupportedOperationException(
            "Authentication method not supported: " + options.getDestinationAuthenticationMethod());
      }


    String sourceTopic;
    String sourceBootstrapServers;
    List<String> sourceBootstrapServerAndTopicList =
        KafkaTopicUtils.getBootstrapServerAndTopic(options.getSourceTopic());
    sourceTopic = sourceBootstrapServerAndTopicList.get(1);
    sourceBootstrapServers = sourceBootstrapServerAndTopicList.get(0);
    String destinationTopic;
    String destinationBootstrapServers;
    List<String> destinationBootstrapServerAndTopicList =
        KafkaTopicUtils.getBootstrapServerAndTopic(options.getDestinationTopic());
    destinationBootstrapServers = destinationBootstrapServerAndTopicList.get(0);
    destinationTopic = destinationBootstrapServerAndTopicList.get(1);
    Pipeline pipeline = Pipeline.create(options);
    if (options.getEnableCommitOffsets()) {

    }
    pipeline
        .apply(
            "Read from Kafka",
            KafkaIO.<byte[], byte[]>read()
                .withBootstrapServers(sourceBootstrapServers)
                .withTopic(sourceTopic)
                .withKeyDeserializer(ByteArrayDeserializer.class)
                .withValueDeserializer(ByteArrayDeserializer.class)
                .withConsumerConfigUpdates(ConsumerProperties.from(options))
                .withConsumerFactoryFn(new FileAwareConsumerFactoryFn())
                .withoutMetadata())
        .apply(
            "Write to Kafka",
            KafkaIO.<byte[], byte[]>write()
                .withBootstrapServers(destinationBootstrapServers)
                .withTopic(destinationTopic)
                .withKeySerializer(ByteArraySerializer.class)
                .withValueSerializer(ByteArraySerializer.class)
                .withProducerConfigUpdates(ProducerProperties.from(options))
                .withProducerFactoryFn(new FileAwareProducerFactoryFn()));

    return pipeline.run();
  }
}

