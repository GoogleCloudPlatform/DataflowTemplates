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
import com.google.cloud.teleport.v2.kafka.options.KafkaReadOptions;
import com.google.cloud.teleport.v2.kafka.options.KafkaWriteOptions;
import com.google.cloud.teleport.v2.kafka.transforms.KafkaTransform;
import com.google.cloud.teleport.v2.kafka.utils.FileAwareProducerFactoryFn;
import com.google.cloud.teleport.v2.kafka.utils.KafkaConfig;
import com.google.cloud.teleport.v2.kafka.utils.KafkaTopicUtils;
import com.google.cloud.teleport.v2.kafka.values.KafkaAuthenticationMethod;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.kafka.common.serialization.ByteArraySerializer;

@Template(
    name = "Kafka_to_Kafka",
    category = TemplateCategory.STREAMING,
    displayName = "Kafka to Kafka",
    description = "A pipeline that writes data to a kafka destination from another kafka source",
    optionsClass = KafkaToKafka.KafkaToKafkaOptions.class,
    flexContainerName = "kafka-to-kafka",
    contactInformation = "https://cloud.google.com/support")
public class KafkaToKafka {
  public interface KafkaToKafkaOptions
      extends PipelineOptions, KafkaReadOptions, KafkaWriteOptions, DataflowPipelineOptions {}

  public static void main(String[] args) throws IOException {
    UncaughtExceptionLogger.register();
    KafkaToKafkaOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(KafkaToKafkaOptions.class);
    run(options);
  }

  public static PipelineResult run(KafkaToKafkaOptions options) {

    if (options.getKafkaReadAuthenticationMode().equals(KafkaAuthenticationMethod.SASL_PLAIN)) {
      checkArgument(
          options.getKafkaReadUsernameSecretId().trim().length() > 0,
          "KafkaReadUsernameSecretId required to access username for source Kafka");
      checkArgument(
          options.getKafkaReadPasswordSecretId().trim().length() > 0,
          "KafkaReadPasswordSecretId required to access password for source kafka");
    } else if (options.getKafkaReadAuthenticationMode().equals(KafkaAuthenticationMethod.TLS)) {
      checkArgument(
          options.getKafkaReadTruststoreLocation().trim().length() > 0,
          "KafkaReadTruststoreLocation for trust store certificate required for ssl authentication");
      checkArgument(
          options.getKafkaReadTruststorePasswordSecretId().trim().length() > 0,
          "KafkaReadTruststorePassword for trust store password required for accessing truststore");
      checkArgument(
          options.getKafkaReadKeystoreLocation().trim().length() > 0,
          "KafkaReadKeystoreLocation for key store location required for ssl authentication");
      checkArgument(
          options.getKafkaReadKeystorePasswordSecretId().trim().length() > 0,
          "KafkaReadKeystorePassword for key store password required to access key store");
      checkArgument(
          options.getKafkaReadKeyPasswordSecretId().trim().length() > 0,
          "KafkaReadKeyPasswordSecretId version for key password required for SSL authentication");
    } else if (options.getKafkaReadAuthenticationMode().equals(KafkaAuthenticationMethod.NONE)) {
    } else {
      throw new UnsupportedOperationException(
          "Authentication method not supported: " + options.getKafkaReadAuthenticationMode());
    }

    if (options.getKafkaWriteAuthenticationMethod().equals(KafkaAuthenticationMethod.SASL_PLAIN)) {
      checkArgument(
          options.getKafkaWriteUsernameSecretId().trim().length() > 0,
          "KafkaWriteUsernameSecretId required to access username for source Kafka");
      checkArgument(
          options.getKafkaWritePasswordSecretId().trim().length() > 0,
          "KafkaWritePasswordSecretId required to access password for destination Kafka");
    } else if (options.getKafkaWriteAuthenticationMethod().equals(KafkaAuthenticationMethod.TLS)) {
      checkArgument(
          options.getKafkaWriteTruststoreLocation().trim().length() > 0,
          "KafkaWriteTruststoreLocation for trust store certificate required for ssl authentication");
      checkArgument(
          options.getKafkaWriteTruststorePasswordSecretId().trim().length() > 0,
          "KafkaWriteTruststorePasswordSecretId for trust store password required for accessing truststore");
      checkArgument(
          options.getKafkaWriteKeystoreLocation().trim().length() > 0,
          "KafkaWriteKeystoreLocation for key store location required for ssl authentication");
      checkArgument(
          options.getKafkaWriteKeystorePasswordSecretId().trim().length() > 0,
          "KafkaWriteKeystorePasswordSecretId for key store password required to access key store");
      checkArgument(
          options.getKafkaWriteKeyPasswordSecretId().trim().length() > 0,
          "KafkaWriteKeyPasswordSecretId for source key password secret id version required for SSL authentication");
    } else if (options.getKafkaWriteAuthenticationMethod().equals(KafkaAuthenticationMethod.NONE)) {
    } else {
      throw new UnsupportedOperationException(
          "Authentication method not supported: " + options.getKafkaWriteAuthenticationMethod());
    }

    String sourceTopic;
    String sourceBootstrapServers;
    if (options.getReadBootstrapServerAndTopic() != null) {
      List<String> sourceBootstrapServerAndTopicList =
          KafkaTopicUtils.getBootstrapServerAndTopic(
              options.getReadBootstrapServerAndTopic(), options.getProject());
      sourceTopic = sourceBootstrapServerAndTopicList.get(1);
      sourceBootstrapServers = sourceBootstrapServerAndTopicList.get(0);
    } else {
      throw new IllegalArgumentException(
          "Please provide a valid bootstrap server which matches `[,:a-zA-Z0-9._-]+` and a topic which matches `[,a-zA-Z0-9._-]+`");
    }

    String destinationTopic;
    String destinationBootstrapServers;
    if (options.getWriteBootstrapServerAndTopic() != null) {
      List<String> destinationBootstrapServerAndTopicList =
          KafkaTopicUtils.getBootstrapServerAndTopic(
              options.getWriteBootstrapServerAndTopic(), options.getProject());
      destinationBootstrapServers = destinationBootstrapServerAndTopicList.get(0);
      destinationTopic = destinationBootstrapServerAndTopicList.get(1);
    } else {
      throw new IllegalArgumentException(
          "Please provide a valid bootstrap server which matches `[,:a-zA-Z0-9._-]+` and a topic which matches `[,a-zA-Z0-9._-]+`");
    }

    Pipeline pipeline = Pipeline.create(options);
    pipeline
        .apply(
            "Read from Kafka",
            KafkaTransform.readBytesFromKafka(
                    sourceBootstrapServers,
                    Collections.singletonList(sourceTopic),
                    KafkaConfig.fromReadOptions(options),
                    options.getEnableCommitOffsets())
                .withoutMetadata())
        .apply(
            "Write to Kafka",
            KafkaIO.<byte[], byte[]>write()
                .withBootstrapServers(destinationBootstrapServers)
                .withTopic(destinationTopic)
                .withKeySerializer(ByteArraySerializer.class)
                .withValueSerializer(ByteArraySerializer.class)
                .withProducerConfigUpdates(KafkaConfig.fromWriteOptions(options))
                .withProducerFactoryFn(new FileAwareProducerFactoryFn()));

    return pipeline.run();
  }
}
