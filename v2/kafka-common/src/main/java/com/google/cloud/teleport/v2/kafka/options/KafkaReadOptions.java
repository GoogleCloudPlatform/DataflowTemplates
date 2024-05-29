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
      helpText = "Consumer group ID to commit offsets to Kafka.")
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
          "The Kafka offset to read from. If there are no committed offsets on Kafka, default offset will be used.")
  @Default.String(Offset.LATEST)
  String getKafkaReadOffset();

  void setKafkaReadOffset(String value);

  @TemplateParameter.Enum(
      order = 5,
      name = "kafkaReadAuthenticationMode",
      groupName = "Source",
      enumOptions = {
        @TemplateParameter.TemplateEnumOption(KafkaAuthenticationMethod.PLAIN),
        @TemplateParameter.TemplateEnumOption(KafkaAuthenticationMethod.SSL),
        @TemplateParameter.TemplateEnumOption(KafkaAuthenticationMethod.NONE),
      },
      description = "Authentication Mode",
      helpText = "Kafka read authentication mode. Can be NONE, PLAIN or SSL")
  @Default.String("NONE")
  String getKafkaReadAuthenticationMode();

  void setKafkaReadAuthenticationMode(String value);

  @TemplateParameter.Text(
      order = 6,
      groupName = "Source",
      parentName = "kafkaReadAuthenticationMode",
      parentTriggerValues = {KafkaAuthenticationMethod.PLAIN},
      optional = true,
      description = "Username",
      helpText =
          "Secret Manager secret ID for the SASL_PLAIN username. Should be in the format projects/{project}/secrets/{secret}/versions/{secret_version}.",
      example = "projects/your-project-id/secrets/your-secret/versions/your-secret-version")
  @Default.String("")
  String getKafkaReadUsernameSecretId();

  void setKafkaReadUsernameSecretId(String value);

  @TemplateParameter.Text(
      order = 7,
      groupName = "Source",
      parentName = "kafkaReadAuthenticationMode",
      parentTriggerValues = KafkaAuthenticationMethod.PLAIN,
      optional = true,
      description = "Password",
      helpText =
          "Secret Manager secret ID for the SASL_PLAIN password. Should be in the format projects/{project}/secrets/{secret}/versions/{secret_version}",
      example = "projects/your-project-id/secrets/your-secret/versions/your-secret-version")
  @Default.String("")
  String getKafkaReadPasswordSecretId();

  void setKafkaReadPasswordSecretId(String value);

  @TemplateParameter.GcsReadFile(
      order = 8,
      optional = true,
      groupName = "Source",
      parentName = "kafkaReadAuthenticationMode",
      parentTriggerValues = {KafkaAuthenticationMethod.SSL},
      helpText =
          "Cloud storage path for the Keystore location that contains the SSL certificate and private key.",
      description = "Location of Keystore",
      example = "gs://your-bucket/keystore.jks")
  String getKafkaReadKeystoreLocation();

  void setKafkaReadKeystoreLocation(String sourceKeystoreLocation);

  @TemplateParameter.GcsReadFile(
      order = 9,
      optional = true,
      groupName = "Source",
      parentName = "kafkaReadAuthenticationMode",
      parentTriggerValues = {KafkaAuthenticationMethod.SSL},
      description = "Truststore File Location",
      helpText =
          "Location of the jks file in Cloud Storage with SSL certificate to verify identity.",
      example = "gs://your-bucket/truststore.jks")
  String getKafkaReadTruststoreLocation();

  void setKafkaReadTruststoreLocation(String sourceTruststoreLocation);

  @TemplateParameter.Text(
      order = 10,
      optional = true,
      groupName = "Source",
      parentName = "kafkaReadAuthenticationMode",
      parentTriggerValues = {KafkaAuthenticationMethod.SSL},
      helpText =
          "Secret Version ID to get password to access secret in truststore for source Kafka.",
      description = "Secret Version ID for Truststore Password",
      example =
          "projects/your-project-number/secrets/your-secret-name/versions/your-secret-version")
  String getKafkaReadTruststorePasswordSecretId();

  void setKafkaReadTruststorePasswordSecretId(String sourceTruststorePasswordSecretId);

  @TemplateParameter.Text(
      order = 11,
      optional = true,
      groupName = "Source",
      parentName = "kafkaReadAuthenticationMode",
      parentTriggerValues = {KafkaAuthenticationMethod.SSL},
      helpText = "Secret Version ID to get password to access secret keystore, for source kafka.",
      description = "Secret Version ID of Keystore Password",
      example =
          "projects/your-project-number/secrets/your-secret-name/versions/your-secret-version")
  String getKafkaReadKeystorePasswordSecretId();

  void setKafkaReadKeystorePasswordSecretId(String sourceKeystorePasswordSecretId);

  @TemplateParameter.Text(
      order = 12,
      optional = true,
      parentName = "kafkaReadAuthenticationMode",
      groupName = "Source",
      parentTriggerValues = {KafkaAuthenticationMethod.SSL},
      helpText =
          "Secret Version ID of password to access private key inside the keystore, for source Kafka.",
      description = "Secret Version ID of Private Key Password",
      example =
          "projects/your-project-number/secrets/your-secret-name/versions/your-secret-version")
  String getKafkaReadKeyPasswordSecretId();

  void setKafkaReadKeyPasswordSecretId(String sourceKeyPasswordSecretId);
}
