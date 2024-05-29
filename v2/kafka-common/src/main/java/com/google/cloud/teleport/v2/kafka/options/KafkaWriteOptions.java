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
 * The {@link KafkaWriteOptions} interface provides the custom execution options passed by the
 * executor at the command-line.
 */
public interface KafkaWriteOptions extends PipelineOptions {
  @TemplateParameter.KafkaTopic(
      groupName = "Destination",
      order = 3,
      description = "Destination Kafka Topic",
      helpText = "Kafka topic to write the output to.")
  String getWriteBootstrapServerAndTopic();

  void setWriteBootstrapServerAndTopic(String destinationTopic);

  @TemplateParameter.Enum(
      groupName = "Destination",
      order = 4,
      name = "kafkaWriteAuthenticationMethod",
      description = "Kafka Destination Authentication Method",
      enumOptions = {
        @TemplateParameter.TemplateEnumOption(KafkaAuthenticationMethod.PLAIN),
        @TemplateParameter.TemplateEnumOption(KafkaAuthenticationMethod.SSL),
        @TemplateParameter.TemplateEnumOption(KafkaAuthenticationMethod.NONE)
      },
      helpText = "Type of authentication mechanism to use with the destination Kafka.")
  @Default.String(KafkaAuthenticationMethod.NONE)
  String getKafkaWriteAuthenticationMethod();

  void setKafkaWriteAuthenticationMethod(String destinationAuthenticationMethod);

  @TemplateParameter.Text(
      optional = true,
      order = 5,
      groupName = "Destination",
      parentName = "kafkaWriteAuthenticationMethod",
      parentTriggerValues = {KafkaAuthenticationMethod.PLAIN},
      description = "Secret Version ID for Kafka username",
      helpText =
          "Secret Version ID from the Secret Manager to get Kafka"
              + KafkaAuthenticationMethod.PLAIN
              + " username for the destination Kafka.",
      example =
          "projects/your-project-number/secrets/your-secret-name/versions/your-secret-version")
  String getKafkaWriteUsernameSecretId();

  void setKafkaWriteUsernameSecretId(String destinationUsernameSecretId);

  @TemplateParameter.Text(
      groupName = "Destination",
      order = 6,
      optional = true,
      parentName = "kafkaWriteAuthenticationMethod",
      parentTriggerValues = {KafkaAuthenticationMethod.PLAIN},
      helpText =
          "Secret Version ID from the Secret Manager to get Kafka "
              + KafkaAuthenticationMethod.PLAIN
              + " password for the destination Kafka.",
      description = "Secret Version ID of for Kafka password",
      example =
          "projects/your-project-number/secrets/your-secret-name/versions/your-secret-version")
  String getKafkaWritePasswordSecretId();

  void setKafkaWritePasswordSecretId(String destinationPasswordSecretId);

  @TemplateParameter.GcsReadFile(
      order = 7,
      optional = true,
      groupName = "Destination",
      parentName = "kafkaWriteAuthenticationMethod",
      description = "Truststore File Location",
      parentTriggerValues = {KafkaAuthenticationMethod.SSL},
      helpText =
          "Location of the jks file in Cloud Storage with SSL certificate to verify identity.")
  String getKafkaWriteTruststoreLocation();

  void setKafkaWriteTruststoreLocation(String destinationTruststoreLocation);

  @TemplateParameter.Text(
      order = 8,
      optional = true,
      groupName = "Destination",
      parentName = "kafkaWriteAuthenticationMethod",
      parentTriggerValues = {KafkaAuthenticationMethod.SSL},
      helpText =
          "Secret Version ID to get password to access secret in truststore, for destination kafka.",
      description = "Secret Version ID of Truststore password",
      example =
          "projects/your-project-number/secrets/your-secret-name/versions/your-secret-version")
  String getKafkaWriteTruststorePasswordSecretId();

  void setKafkaWriteTruststorePasswordSecretId(String destinationTruststorePasswordSecretId);

  @TemplateParameter.GcsReadFile(
      order = 10,
      optional = true,
      helpText =
          "Cloud storage path for the Keystore location that contains the SSL certificate and private key.",
      groupName = "Destination",
      parentName = "kafkaWriteAuthenticationMethod",
      parentTriggerValues = {KafkaAuthenticationMethod.SSL},
      description = "Location of Keystore",
      example = "gs://your-bucket/keystore.jks")
  String getKafkaWriteKeystoreLocation();

  void setKafkaWriteKeystoreLocation(String destinationKeystoreLocation);

  @TemplateParameter.Text(
      order = 11,
      optional = true,
      groupName = "Destination",
      parentName = "kafkaWriteAuthenticationMethod",
      parentTriggerValues = {KafkaAuthenticationMethod.SSL},
      helpText =
          "Secret Version ID to get password to access secret keystore, for destination kafka.",
      description = "Secret Version Version ID of Keystore Password",
      example =
          "projects/your-project-number/secrets/your-secret-name/versions/your-secret-version")
  String getKafkaWriteKeystorePasswordSecretId();

  void setKafkaWriteKeystorePasswordSecretId(String destinationKeystorePasswordSecretId);

  @TemplateParameter.Text(
      order = 12,
      optional = true,
      parentName = "kafkaWriteAuthenticationMethod",
      groupName = "Destination",
      parentTriggerValues = {KafkaAuthenticationMethod.SSL},
      helpText =
          "Secret Version ID of password to access private key inside the keystore, for destination Kafka.",
      description = "Secret Version ID of key",
      example =
          "projects/your-project-number/secrets/your-secret-name/versions/your-secret-version")
  String getKafkaWriteKeyPasswordSecretId();

  void setKafkaWriteKeyPasswordSecretId(String destinationKeyPasswordSecretId);
}
