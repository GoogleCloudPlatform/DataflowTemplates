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

import com.google.cloud.secretmanager.v1.SecretVersionName;
import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.v2.options.KafkaToGCSOptions;
import com.google.cloud.teleport.v2.transforms.WriteTransform;
import com.google.cloud.teleport.v2.utils.SecretManagerUtils;
import com.google.common.collect.ImmutableMap;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import java.io.IOException;
import java.util.*;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

@Template(
    name = "Kafka_to_GCS_2",
    category = TemplateCategory.STREAMING,
    displayName = "Kafka to Cloud Storage",
    description =
        "A streaming pipeline which ingests data from Kafka and writes to a pre-existing Cloud"
            + " Storage bucket with a variety of file types.",
    optionsClass = KafkaToGCSOptions.class,
    flexContainerName = "kafka-to-gcs-2",
    contactInformation = "https://cloud.google.com/support",
    hidden = true,
    streaming = true)
public class KafkaToGCSFlex {
  /* Logger for class */
  private static final String topicsSplitDelimiter = ",";

  public static class ClientAuthConfig {
    public static ImmutableMap<String, Object> get(String username, String password) {
      ImmutableMap.Builder<String, Object> properties = ImmutableMap.builder();
      properties.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
      properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
      properties.put(
          SaslConfigs.SASL_JAAS_CONFIG,
          "org.apache.kafka.common.security.plain.PlainLoginModule required"
              + " username=\'"
              + username
              + "\'"
              + " password=\'"
              + password
              + "\';");
      return properties.buildOrThrow();
    }
  }

  public static PipelineResult run(KafkaToGCSOptions options) throws UnsupportedOperationException {

    // Create the Pipeline
    Pipeline pipeline = Pipeline.create(options);

    PCollection<KafkaRecord<byte[], byte[]>> kafkaRecord;

    List<String> topics =
        new ArrayList<>(Arrays.asList(options.getInputTopics().split(topicsSplitDelimiter)));

    options.setStreaming(true);

    String kafkaSaslPlainUserName = SecretManagerUtils.getSecret(options.getUserNameSecretID());
    String kafkaSaslPlainPassword = SecretManagerUtils.getSecret(options.getPasswordSecretID());

    Map<String, Object> kafkaConfig = new HashMap<>();
    // Set offset to either earliest or latest.
    kafkaConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, options.getOffset());
    kafkaConfig.putAll(ClientAuthConfig.get(kafkaSaslPlainUserName, kafkaSaslPlainPassword));

    // Step 1: Read from Kafka as bytes.
    kafkaRecord =
        pipeline.apply(
            KafkaIO.<byte[], byte[]>read()
                .withBootstrapServers(options.getBootstrapServers())
                .withTopics(topics)
                .withKeyDeserializerAndCoder(
                    ByteArrayDeserializer.class, NullableCoder.of(ByteArrayCoder.of()))
                .withValueDeserializerAndCoder(
                    ByteArrayDeserializer.class, NullableCoder.of(ByteArrayCoder.of()))
                .withConsumerConfigUpdates(kafkaConfig));

    kafkaRecord.apply(WriteTransform.newBuilder().setOptions(options).build());
    return pipeline.run();
  }

  public static void validateOptions(KafkaToGCSOptions options) {
    if (options.getUserNameSecretID().isBlank() || options.getPasswordSecretID().isBlank()) {
      throw new IllegalArgumentException(
          "No Information to retrieve Kafka SASL_PLAIN username/password was provided.");
    }
    if (!SecretVersionName.isParsableFrom(options.getUserNameSecretID())) {
      throw new IllegalArgumentException(
          "Provided Secret Username ID must be in the form"
              + " projects/{project}/secrets/{secret}/versions/{secret_version}");
    }
    if (!SecretVersionName.isParsableFrom(options.getPasswordSecretID())) {
      throw new IllegalArgumentException(
          "Provided Secret Password ID must be in the form"
              + " projects/{project}/secrets/{secret}/versions/{secret_version}");
    }
  }

  public static void main(String[] args) throws RestClientException, IOException {
    KafkaToGCSOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(KafkaToGCSOptions.class);
    validateOptions(options);
    run(options);
  }
}
