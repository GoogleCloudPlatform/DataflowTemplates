/*
 * Copyright (C) 2020 Google LLC
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
package com.google.cloud.teleport.v2.kafka.options;

import com.google.cloud.teleport.metadata.TemplateParameter;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptions;

/**
 * The {@link KafkaReadOptions} interface provides the custom execution options passed by the
 * executor at the command-line.
 */
public interface KafkaReadOptions extends PipelineOptions {

  @TemplateParameter.Text(
      order = 1,
      groupName = "Source",
      optional = true,
      regexes = {"[,:a-zA-Z0-9._-]+"},
      description = "Kafka Bootstrap Server list",
      helpText = "Kafka Bootstrap Server list, separated by commas.",
      example = "localhost:9092,127.0.0.1:9093")
  String getReadBootstrapServers();

  void setReadBootstrapServers(String bootstrapServers);

  @TemplateParameter.Text(
      order = 2,
      groupName = "Source",
      optional = true,
      regexes = {"[,a-zA-Z0-9._-]+"},
      description = "Kafka Topic(s) to read input from",
      helpText = "Kafka topic(s) to read input from.",
      example = "topic1,topic2")
  String getKafkaReadTopics();

  void setKafkaReadTopics(String inputTopics);

  @TemplateParameter.Boolean(
      order = 3,
      groupName = "Source",
      name = "enableCommitOffsets",
      optional = true,
      description = "Commit Offsets to Kafka",
      helpText =
          "Commit offsets of processed messages to Kafka. "
              + "If enabled, this will minimize the gaps or duplicate processing"
              + " of messages when restarting the pipeline. Requires specifying the Consumer Group ID.")
  @Default.Boolean(false)
  Boolean getEnableCommitOffsets();

  void setEnableCommitOffsets(Boolean value);

  @TemplateParameter.Text(
      order = 4,
      groupName = "Source",
      optional = true,
      description = "Consumer Group ID",
      helpText = "Consumer group ID to commit offsets to Kafka.")
  @Default.String("")
  String getConsumerGroupId();

  void setConsumerGroupId(String value);

  @TemplateParameter.Enum(
      order = 5,
      groupName = "Source",
      enumOptions = {
        @TemplateParameter.TemplateEnumOption("earliest"),
        @TemplateParameter.TemplateEnumOption("latest"),
      },
      optional = true,
      description = "Default Kafka Start Offset",
      helpText =
          "The Kafka offset to read from. If there are no committed offsets on Kafka, default offset will be used.")
  @Default.String("latest")
  String getKafkaReadOffset();

  void setKafkaReadOffset(String value);

  @TemplateParameter.Enum(
      order = 6,
      name = "kafkaReadAuthenticationMode",
      groupName = "Source",
      enumOptions = {
        @TemplateParameter.TemplateEnumOption("NONE"),
        @TemplateParameter.TemplateEnumOption("SASL_PLAIN"),
        @TemplateParameter.TemplateEnumOption("SSL"),
      },
      optional = false,
      description = "Authentication Mode",
      helpText = "Kafka read authentication mode. Can be NONE or SASL_PLAIN")
  @Default.String("NONE")
  String getKafkaReadAuthenticationMode();

  void setKafkaReadAuthenticationMode(String value);

  @TemplateParameter.Text(
      order = 7,
      groupName = "Source",
      parentName = "kafkaReadAuthenticationMode",
      parentTriggerValues = {"SASL_PLAIN"},
      optional = true,
      description = "Username",
      helpText =
          "Secret Manager secret ID for the SASL_PLAIN username. Should be in the format projects/{project}/secrets/{secret}/versions/{secret_version}.",
      example = "projects/your-project-id/secrets/your-secret/versions/your-secret-version")
  @Default.String("")
  String getKafkaReadUsernameSecretId();

  void setKafkaReadUsernameSecretId(String value);

  @TemplateParameter.Text(
      order = 8,
      groupName = "Source",
      parentName = "kafkaReadAuthenticationMode",
      parentTriggerValues = {"SASL_PLAIN"},
      optional = true,
      description = "Password",
      helpText =
          "Secret Manager secret ID for the SASL_PLAIN password. Should be in the format projects/{project}/secrets/{secret}/versions/{secret_version}",
      example = "projects/your-project-id/secrets/your-secret/versions/your-secret-version")
  @Default.String("")
  String getKafkaReadPasswordSecretId();

  void setKafkaReadPasswordSecretId(String value);

  @TemplateParameter.GcsReadFile(
      order = 7,
      parentName = "kafkaReadAuthenticationMode",
      parentTriggerValues = {"SSL"},
      optional = true,
      description = "Truststore File Location",
      helpText =
          "Location of the SSL certificate where the trust store for authentication to Schema Registry are stored.",
      example = "/your-bucket/truststore.jks")
  String getKafkaReadTruststoreLocation();

  void setKafkaReadTruststoreLocation(String truststoreLocation);

  @TemplateParameter.Text(
      order = 8,
      parentName = "kafkaReadAuthenticationMode",
      parentTriggerValues = {"SSL"},
      optional = true,
      description = "Truststore Password",
      helpText =
          "SecretId in secret manager where the password to access secret in truststore is stored.",
      example =
          "projects/your-project-number/secrets/your-secret-name/versions/your-secret-version")
  String getKafkaReadTruststorePasswordSecretId();

  void setKafkaReadTruststorePasswordSecretId(String truststorePasswordSecretId);

  @TemplateParameter.GcsReadFile(
      order = 9,
      parentName = "kafkaReadAuthenticationMode",
      parentTriggerValues = {"SSL"},
      optional = true,
      description = "Keystore File Location",
      helpText = "Keystore location that contains the SSL certificate and private key.",
      example = "/your-bucket/keystore.jks")
  String getKafkaReadKeystoreLocation();

  void setKafkaReadKeystoreLocation(String keystoreLocation);

  @TemplateParameter.Text(
      order = 10,
      parentName = "kafkaReadAuthenticationMode",
      parentTriggerValues = {"SSL"},
      optional = true,
      description = "Keystore Password",
      helpText = "SecretId in secret manager where the password to access the keystore file",
      example =
          "projects/your-project-number/secrets/your-secret-name/versions/your-secret-version")
  String getKafkaReadKeystorePasswordSecretId();

  void setKafkaReadKeystorePasswordSecretId(String keystorePasswordSecretId);

  @TemplateParameter.Text(
      order = 11,
      parentName = "kafkaReadAuthenticationMode",
      parentTriggerValues = {"SSL"},
      optional = true,
      description = "Private Key Password",
      helpText =
          "SecretId of password required to access the client's private key stored within the keystore",
      example =
          "projects/your-project-number/secrets/your-secret-name/versions/your-secret-version")
  String getKafkaReadKeyPasswordSecretId();

  void setKafkaReadKeyPasswordSecretId(String keyPasswordSecretId);
}
