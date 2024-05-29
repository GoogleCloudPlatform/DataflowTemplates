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
import com.google.cloud.teleport.v2.kafka.values.KafkaAuthenticationMethod;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptions;

/**
 * The {@link KafkaReadOptions} interface provides the custom execution options passed by the
 * executor at the command-line.
 */
public interface KafkaReadOptions extends PipelineOptions {

  final class Offset {
    public static final String LATEST = "latest";
    public static final String EARLIEST = "earliest";
  }

  @TemplateParameter.KafkaTopic(
      order = 1,
      name = "readBootstrapServerAndTopic",
      groupName = "Source",
      description = "Source Kafka Topic",
      helpText = "Kafka Topic to read the input from.")
  String getReadBootstrapServerAndTopic();

  void setReadBootstrapServerAndTopic(String value);

  @TemplateParameter.Boolean(
      order = 2,
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
      order = 3,
      groupName = "Source",
      optional = true,
      description = "Consumer Group ID",
      helpText =
          "Unique identifier for the consumer group to which this pipeline belongs."
              + " Required if 'Commit Offsets to Kafka' is enabled.")
  @Default.String("")
  String getConsumerGroupId();

  void setConsumerGroupId(String value);

  @TemplateParameter.Enum(
      order = 4,
      groupName = "Source",
      enumOptions = {
        @TemplateParameter.TemplateEnumOption(Offset.EARLIEST),
        @TemplateParameter.TemplateEnumOption(Offset.LATEST),
      },
      optional = true,
      description = "Default Kafka Start Offset",
      helpText =
          "Starting point for reading messages if no committed offsets exist."
              + "earliest starts from the beginning, latest from the newest message.")
  @Default.String(Offset.LATEST)
  String getKafkaReadOffset();

  void setKafkaReadOffset(String value);

  @TemplateParameter.Enum(
      order = 5,
      name = "kafkaReadAuthenticationMode",
      groupName = "Source",
      enumOptions = {
        @TemplateParameter.TemplateEnumOption(KafkaAuthenticationMethod.SASL_PLAIN),
        @TemplateParameter.TemplateEnumOption(KafkaAuthenticationMethod.TLS),
        @TemplateParameter.TemplateEnumOption(KafkaAuthenticationMethod.NONE),
      },
      description = "Kafka Source Authentication Mode",
      helpText =
          "Mode of authentication with the Kafka cluster. "
              + "NONE for no authentication, SASL_PLAIN for SASL/PLAIN username/password, "
              + "TLS for certificate-based authentication. "
              + "Note: Apache Kafka for BigQuery supports only SASL_PLAIN Authentication Mode")
  @Default.String("NONE")
  String getKafkaReadAuthenticationMode();

  void setKafkaReadAuthenticationMode(String value);

  @TemplateParameter.Text(
      order = 6,
      groupName = "Source",
      parentName = "kafkaReadAuthenticationMode",
      parentTriggerValues = {KafkaAuthenticationMethod.SASL_PLAIN},
      optional = true,
      description = "Secret Version ID for Kafka SASL/PLAIN username",
      helpText =
          "Google Cloud Secret Manager secret ID containing the Kafka username "
              + "for SASL_PLAIN authentication.",
      example = "projects/{project}/secrets/{secret}/versions/{secret_version}")
  @Default.String("")
  String getKafkaReadUsernameSecretId();

  void setKafkaReadUsernameSecretId(String value);

  @TemplateParameter.Text(
      order = 7,
      groupName = "Source",
      parentName = "kafkaReadAuthenticationMode",
      parentTriggerValues = KafkaAuthenticationMethod.SASL_PLAIN,
      optional = true,
      description = "Secret Version ID for Kafka SASL/PLAIN password",
      helpText =
          "Google Cloud Secret Manager secret ID containing the Kafka password for SASL_PLAIN authentication.",
      example = "projects/{project}/secrets/{secret}/versions/{secret_version}")
  @Default.String("")
  String getKafkaReadPasswordSecretId();

  void setKafkaReadPasswordSecretId(String value);

  @TemplateParameter.GcsReadFile(
      order = 8,
      optional = true,
      groupName = "Source",
      parentName = "kafkaReadAuthenticationMode",
      parentTriggerValues = {KafkaAuthenticationMethod.TLS},
      helpText =
          "Google Cloud Storage path to the Java KeyStore (JKS) file containing the "
              + "TLS certificate and private key for authenticating with the Kafka cluster.",
      description = "Location of Keystore",
      example = "gs://your-bucket/keystore.jks")
  String getKafkaReadKeystoreLocation();

  void setKafkaReadKeystoreLocation(String sourceKeystoreLocation);

  @TemplateParameter.GcsReadFile(
      order = 9,
      optional = true,
      groupName = "Source",
      parentName = "kafkaReadAuthenticationMode",
      parentTriggerValues = {KafkaAuthenticationMethod.TLS},
      description = "Truststore File Location",
      helpText =
          "Google Cloud Storage path to the Java TrustStore (JKS) file containing"
              + " trusted certificates to verify the identity of the Kafka broker.")
  String getKafkaReadTruststoreLocation();

  void setKafkaReadTruststoreLocation(String sourceTruststoreLocation);

  @TemplateParameter.Text(
      order = 10,
      optional = true,
      groupName = "Source",
      parentName = "kafkaReadAuthenticationMode",
      parentTriggerValues = {KafkaAuthenticationMethod.TLS},
      description = "Secret Version ID for Truststore Password",
      helpText =
          "Google Cloud Secret Manager secret ID containing the password to "
              + "access the Java TrustStore (JKS) file for Kafka TLS authentication. Format: projects/{project}/secrets/{secret}/versions/{secret_version}",
      example = "projects/{project}/secrets/{secret}/versions/{secret_version}")
  String getKafkaReadTruststorePasswordSecretId();

  void setKafkaReadTruststorePasswordSecretId(String sourceTruststorePasswordSecretId);

  @TemplateParameter.Text(
      order = 11,
      optional = true,
      groupName = "Source",
      parentName = "kafkaReadAuthenticationMode",
      parentTriggerValues = {KafkaAuthenticationMethod.TLS},
      description = "Secret Version ID of Keystore Password",
      helpText =
          "Google Cloud Secret Manager secret ID containing the password to"
              + " access the Java KeyStore (JKS) file for Kafka TLS authentication. Format: projects/{project}/secrets/{secret}/versions/{secret_version}",
      example = "projects/{project}/secrets/{secret}/versions/{secret_version}")
  String getKafkaReadKeystorePasswordSecretId();

  void setKafkaReadKeystorePasswordSecretId(String sourceKeystorePasswordSecretId);

  @TemplateParameter.Text(
      order = 12,
      optional = true,
      parentName = "kafkaReadAuthenticationMode",
      groupName = "Source",
      parentTriggerValues = {KafkaAuthenticationMethod.TLS},
      helpText =
          "Google Cloud Secret Manager secret ID containing the password to access the private key within the Java KeyStore (JKS) file"
              + " for Kafka TLS authentication. Format: projects/{project}/secrets/{secret}/versions/{secret_version}",
      description = "Secret Version ID of Private Key Password",
      example = "projects/{project}/secrets/{secret}/versions/{secret_version}")
  String getKafkaReadKeyPasswordSecretId();

  void setKafkaReadKeyPasswordSecretId(String sourceKeyPasswordSecretId);
}
