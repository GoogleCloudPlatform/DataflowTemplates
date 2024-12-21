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

  @TemplateParameter.KafkaReadTopic(
      order = 1,
      name = "readBootstrapServerAndTopic",
      groupName = "Source",
      description = "Source Kafka Bootstrap server and topic",
      helpText = "Kafka Bootstrap server and topic to read the input from.",
      example = "localhost:9092;topic1,topic2")
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
      parentName = "enableCommitOffsets",
      parentTriggerValues = {"true"},
      optional = true,
      description = "Consumer Group ID",
      helpText =
          "The unique identifier for the consumer group that this pipeline belongs to."
              + " Required if Commit Offsets to Kafka is enabled.")
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
          "The starting point for reading messages when no committed offsets exist."
              + " The earliest starts from the beginning, the latest from the newest message.")
  @Default.String(Offset.LATEST)
  String getKafkaReadOffset();

  void setKafkaReadOffset(String value);

  @TemplateParameter.Enum(
      order = 5,
      name = "kafkaReadAuthenticationMode",
      groupName = "Source",
      enumOptions = {
        @TemplateParameter.TemplateEnumOption(
            KafkaAuthenticationMethod.APPLICATION_DEFAULT_CREDENTIALS),
        @TemplateParameter.TemplateEnumOption(KafkaAuthenticationMethod.SASL_PLAIN),
        @TemplateParameter.TemplateEnumOption(KafkaAuthenticationMethod.TLS),
        @TemplateParameter.TemplateEnumOption(KafkaAuthenticationMethod.NONE),
      },
      description = "Kafka Source Authentication Mode",
      helpText =
          "The mode of authentication to use with the Kafka cluster. "
              + "Use `KafkaAuthenticationMethod.NONE` for no authentication, `KafkaAuthenticationMethod.SASL_PLAIN` for SASL/PLAIN username and password, "
              + "and `KafkaAuthenticationMethod.TLS` for certificate-based authentication. `KafkaAuthenticationMethod.APPLICATION_DEFAULT_CREDENTIALS` "
              + "should be used only for Google Cloud Apache Kafka for BigQuery cluster, it allows to authenticate using application default credentials.")
  @Default.String(KafkaAuthenticationMethod.SASL_PLAIN)
  String getKafkaReadAuthenticationMode();

  void setKafkaReadAuthenticationMode(String value);

  @TemplateParameter.Text(
      order = 6,
      groupName = "Source",
      parentName = "kafkaReadAuthenticationMode",
      parentTriggerValues = {KafkaAuthenticationMethod.SASL_PLAIN},
      optional = true,
      description = "Secret Version ID For Kafka SASL/PLAIN Username",
      helpText =
          "The Google Cloud Secret Manager secret ID that contains the Kafka username "
              + "to use with `SASL_PLAIN` authentication.",
      example = "projects/<PROJECT_ID>/secrets/<SECRET_ID>/versions/<SECRET_VERSION>")
  @Default.String("")
  String getKafkaReadUsernameSecretId();

  void setKafkaReadUsernameSecretId(String value);

  @TemplateParameter.Text(
      order = 7,
      groupName = "Source",
      parentName = "kafkaReadAuthenticationMode",
      parentTriggerValues = KafkaAuthenticationMethod.SASL_PLAIN,
      optional = true,
      description = "Secret Version ID For Kafka SASL/PLAIN Password",
      helpText =
          "The Google Cloud Secret Manager secret ID that contains the Kafka password to use with `SASL_PLAIN` authentication.",
      example = "projects/<PROJECT_ID>/secrets/<SECRET_ID>/versions/<SECRET_VERSION>")
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
          "The Google Cloud Storage path to the Java KeyStore (JKS) file that contains the "
              + "TLS certificate and private key to use when authenticating with the Kafka cluster.",
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
          "The Google Cloud Storage path to the Java TrustStore (JKS) file that contains"
              + " the trusted certificates to use to verify the identity of the Kafka broker.")
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
          "The Google Cloud Secret Manager secret ID that contains the password to "
              + "use to access the Java TrustStore (JKS) file for Kafka TLS authentication",
      example = "projects/<PROJECT_ID>/secrets/<SECRET_ID>/versions/<SECRET_VERSION>")
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
          "The Google Cloud Secret Manager secret ID that contains the password to"
              + " use to access the Java KeyStore (JKS) file for Kafka TLS authentication.",
      example = "projects/<PROJECT_ID>/secrets/<SECRET_ID>/versions/<SECRET_VERSION>")
  String getKafkaReadKeystorePasswordSecretId();

  void setKafkaReadKeystorePasswordSecretId(String sourceKeystorePasswordSecretId);

  @TemplateParameter.Text(
      order = 12,
      optional = true,
      parentName = "kafkaReadAuthenticationMode",
      groupName = "Source",
      parentTriggerValues = {KafkaAuthenticationMethod.TLS},
      helpText =
          "The Google Cloud Secret Manager secret ID that contains the password to use to access the private key within the Java KeyStore (JKS) file"
              + " for Kafka TLS authentication.",
      description = "Secret Version ID of Private Key Password",
      example = "projects/<PROJECT_ID>/secrets/<SECRET_ID>/versions/<SECRET_VERSION>")
  String getKafkaReadKeyPasswordSecretId();

  void setKafkaReadKeyPasswordSecretId(String sourceKeyPasswordSecretId);
}
