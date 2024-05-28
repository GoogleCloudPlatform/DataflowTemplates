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
import org.apache.beam.sdk.options.Validation;

/**
 * The {@link KafkaWriteOptions} interface provides the custom execution options passed by the
 * executor at the command-line.
 */
public interface KafkaWriteOptions extends PipelineOptions {

  @TemplateParameter.Text(
      order = 1,
      optional = true,
      regexes = {"[,:a-zA-Z0-9._-]+"},
      description = "Kafka Bootstrap Server list",
      helpText = "Kafka Bootstrap Server list, separated by commas.",
      example = "localhost:9092,127.0.0.1:9093")
  String getWriteBootstrapServers();

  void setWriteBootstrapServers(String bootstrapServers);

  @TemplateParameter.Text(
      order = 2,
      optional = true,
      regexes = {"[,a-zA-Z0-9._-]+"},
      description = "Kafka topic(s) to write to",
      helpText = "Kafka topic(s) to write to.",
      example = "topic1,topic2")
  String getKafkaWriteTopics();

  void setWriteTopics(String inputTopics);

  @TemplateParameter.KafkaTopic(
      groupName = "Destination",
      order = 3,
      name = "destinationTopic",
      description = "Destination Kafka Topic",
      helpText = "Kafka topic to write the output to.",
      optional = false)
  @Validation.Required
  String getDestinationTopic();

  void setDestinationTopic(String destinationTopic);

  @TemplateParameter.Text(
      groupName = "Destination",
      order = 4,
      description = "Project of Destination Kafka",
      helpText = "Project where Destination Kafka resides.",
      optional = false)
  @Validation.Required
  String getDestinationProject();

  void setDestinationProject(String destinationProject);

  @TemplateParameter.Enum(
      groupName = "Destination",
      order = 4,
      name = "destinationAuthenticationMethod",
      optional = false,
      description = "Destination Authentication Method",
      enumOptions = {
        @TemplateParameter.TemplateEnumOption(KafkaAuthenticationMethod.SASL_PLAIN),
        @TemplateParameter.TemplateEnumOption(KafkaAuthenticationMethod.SSL),
        @TemplateParameter.TemplateEnumOption(KafkaAuthenticationMethod.NONE)
      },
      helpText = "Type of authentication mechanism to use with the destination Kafka.")
  @Validation.Required
  @Default.String(KafkaAuthenticationMethod.NONE)
  String getDestinationAuthenticationMethod();

  void setDestinationAuthenticationMethod(String destinationAuthenticationMethod);

  @TemplateParameter.Text(
      optional = true,
      order = 5,
      name = "destinationUsernameSecretId",
      groupName = "Destination",
      parentName = "destinationAuthenticationMethod",
      parentTriggerValues = {KafkaAuthenticationMethod.SASL_PLAIN},
      description = "Secret Version ID for Kafka username",
      helpText =
          "Secret Version ID from the Secret Manager to get Kafka"
              + KafkaAuthenticationMethod.SASL_PLAIN
              + " username for the destination Kafka.",
      example =
          "projects/your-project-number/secrets/your-secret-name/versions/your-secret-version")
  String getDestinationUsernameSecretId();

  void setDestinationUsernameSecretId(String destinationUsernameSecretId);

  @TemplateParameter.Text(
      groupName = "Destination",
      order = 6,
      name = "destinationPasswordSecretId",
      parentName = "destinationAuthenticationMethod",
      parentTriggerValues = {KafkaAuthenticationMethod.SASL_PLAIN},
      helpText =
          "Secret Version ID from the Secret Manager to get Kafka "
              + KafkaAuthenticationMethod.SASL_PLAIN
              + " password for the destination Kafka.",
      description = "Secret Version ID of for Kafka password",
      example =
          "projects/your-project-number/secrets/your-secret-name/versions/your-secret-version")
  String getDestinationPasswordSecretId();

  void setDestinationPasswordSecretId(String destinationPasswordSecretId);

  @TemplateParameter.GcsReadFile(
      order = 7,
      optional = true,
      name = "truststoredestination",
      groupName = "Destination",
      parentName = "destinationAuthenticationMethod",
      description = "Truststore File Location",
      parentTriggerValues = {KafkaAuthenticationMethod.SSL},
      helpText =
          "Location of the jks file in Cloud Storage with SSL certificate to verify identity.")
  String getDestinationTruststoreLocation();

  void setDestinationTruststoreLocation(String destinationTruststoreLocation);

  @TemplateParameter.Text(
      order = 8,
      optional = true,
      name = "destinationTruststorePassword",
      groupName = "Destination",
      parentName = "destinationAuthenticationMethod",
      parentTriggerValues = {KafkaAuthenticationMethod.SSL},
      helpText =
          "Secret Version ID to get password to access secret in truststore, for destination kafka.",
      description = "Secret Version ID of Truststore password",
      example =
          "projects/your-project-number/secrets/your-secret-name/versions/your-secret-version")
  String getDestinationTruststorePasswordSecretId();

  void setDestinationTruststorePasswordSecretId(String destinationTruststorePasswordSecretId);

  @TemplateParameter.GcsReadFile(
      order = 10,
      optional = true,
      helpText =
          "Cloud storage path for the Keystore location that contains the SSL certificate and private key.",
      name = "destinationKeystoreLocation",
      groupName = "Destination",
      parentName = "destinationAuthenticationMethod",
      parentTriggerValues = {KafkaAuthenticationMethod.SSL},
      description = "Location of Keystore",
      example = "gs://your-bucket/keystore.jks")
  String getDestinationKeystoreLocation();

  void setDestinationKeystoreLocation(String destinationKeystoreLocation);

  @TemplateParameter.Text(
      order = 11,
      optional = true,
      name = "destinationKeystorePassword",
      groupName = "Destination",
      parentName = "destinationAuthenticationMethod",
      parentTriggerValues = {KafkaAuthenticationMethod.SSL},
      helpText =
          "Secret Version ID to get password to access secret keystore, for destination kafka.",
      description = "Secret Version Version ID of Keystore Password",
      example =
          "projects/your-project-number/secrets/your-secret-name/versions/your-secret-version")
  String getDestinationKeystorePasswordSecretId();

  void setDestinationKeystorePasswordSecretId(String destinationKeystorePasswordSecretId);

  @TemplateParameter.Text(
      order = 12,
      optional = true,
      name = "destinationKey",
      parentName = "destinationAuthenticationMethod",
      groupName = "Destination",
      parentTriggerValues = {KafkaAuthenticationMethod.SSL},
      helpText =
          "Secret Version ID of password to access private key inside the keystore, for destination Kafka.",
      description = "Secret Version ID of key",
      example =
          "projects/your-project-number/secrets/your-secret-name/versions/your-secret-version")
  String getDestinationKeyPasswordSecretId();

  void setDestinationKeyPasswordSecretId(String destinationKeyPasswordSecretId);
}
