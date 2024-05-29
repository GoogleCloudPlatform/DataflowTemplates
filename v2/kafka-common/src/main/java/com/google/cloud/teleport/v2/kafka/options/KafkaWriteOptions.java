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
        @TemplateParameter.TemplateEnumOption(KafkaAuthenticationMethod.SASL_PLAIN),
        @TemplateParameter.TemplateEnumOption(KafkaAuthenticationMethod.TLS),
        @TemplateParameter.TemplateEnumOption(KafkaAuthenticationMethod.NONE)
      },
      helpText =
          "Mode of authentication with the Kafka cluster. "
              + "NONE for no authentication, SASL_PLAIN for SASL/PLAIN username/password, "
              + "TLS for certificate-based authentication.")
  @Default.String(KafkaAuthenticationMethod.NONE)
  String getKafkaWriteAuthenticationMethod();

  void setKafkaWriteAuthenticationMethod(String destinationAuthenticationMethod);

  @TemplateParameter.Text(
      optional = true,
      order = 5,
      groupName = "Destination",
      parentName = "kafkaWriteAuthenticationMethod",
      parentTriggerValues = {KafkaAuthenticationMethod.SASL_PLAIN},
      description = "Secret Version ID for Kafka SASL/PLAIN username",
      helpText =
          "Google Cloud Secret Manager secret ID containing the Kafka username"
              + " for SASL_PLAIN authentication with the destination Kafka cluster.",
      example = "projects/{project}/secrets/{secret}/versions/{secret_version}")
  @Default.String("")
  String getKafkaWriteUsernameSecretId();

  void setKafkaWriteUsernameSecretId(String destinationUsernameSecretId);

  @TemplateParameter.Text(
      groupName = "Destination",
      order = 6,
      optional = true,
      parentName = "kafkaWriteAuthenticationMethod",
      parentTriggerValues = {KafkaAuthenticationMethod.SASL_PLAIN},
      description = "Secret Version ID for Kafka SASL/PLAIN password",
      helpText =
          "Google Cloud Secret Manager secret ID containing the Kafka password for SASL/PLAIN authentication"
              + " with the destination Kafka cluster.",
      example = "projects/{project}/secrets/{secret}/versions/{secret_version}")
  @Default.String("")
  String getKafkaWritePasswordSecretId();

  @TemplateParameter.GcsReadFile(
      order = 7,
      optional = true,
      groupName = "Destination",
      parentName = "kafkaWriteAuthenticationMethod",
      parentTriggerValues = {KafkaAuthenticationMethod.TLS},
      description = "Location of Keystore",
      helpText =
          "Google Cloud Storage path to the Java KeyStore (JKS) file containing the TLS certificate "
              + "and private key for authenticating with the destination Kafka cluster.",
      example = "gs://your-bucket/keystore.jks")
  String getKafkaWriteKeystoreLocation();

  void setKafkaWriteKeystoreLocation(String destinationKeystoreLocation);

  void setKafkaWritePasswordSecretId(String destinationPasswordSecretId);

  @TemplateParameter.GcsReadFile(
      order = 8,
      optional = true,
      groupName = "Destination",
      parentName = "kafkaWriteAuthenticationMethod",
      description = "Truststore File Location",
      parentTriggerValues = {KafkaAuthenticationMethod.TLS},
      helpText =
          "Google Cloud Storage path to the Java TrustStore (JKS) file containing trusted certificates"
              + " to verify the identity of the destination Kafka broker.")
  String getKafkaWriteTruststoreLocation();

  void setKafkaWriteTruststoreLocation(String destinationTruststoreLocation);

  @TemplateParameter.Text(
      order = 9,
      optional = true,
      groupName = "Destination",
      parentName = "kafkaWriteAuthenticationMethod",
      parentTriggerValues = {KafkaAuthenticationMethod.TLS},
      description = "Secret Version ID of Truststore password",
      helpText =
          "Google Cloud Secret Manager secret ID containing the password to access the Java TrustStore (JKS) file "
              + "for TLS authentication with the destination Kafka cluster.",
      example = "projects/{project}/secrets/{secret}/versions/{secret_version}")
  String getKafkaWriteTruststorePasswordSecretId();

  void setKafkaWriteTruststorePasswordSecretId(String destinationTruststorePasswordSecretId);

  @TemplateParameter.Text(
      order = 11,
      optional = true,
      groupName = "Destination",
      parentName = "kafkaWriteAuthenticationMethod",
      parentTriggerValues = {KafkaAuthenticationMethod.TLS},
      description = "Secret Version ID of Keystore Password",
      helpText =
          "Google Cloud Secret Manager secret ID containing the password to access the Java KeyStore (JKS) "
              + "file for TLS authentication with the destination Kafka cluster.",
      example = "projects/{project}/secrets/{secret}/versions/{secret_version}")
  String getKafkaWriteKeystorePasswordSecretId();

  void setKafkaWriteKeystorePasswordSecretId(String destinationKeystorePasswordSecretId);

  @TemplateParameter.Text(
      order = 12,
      optional = true,
      parentName = "kafkaWriteAuthenticationMethod",
      groupName = "Destination",
      parentTriggerValues = {KafkaAuthenticationMethod.TLS},
      description = "Secret Version ID of Private Key Password",
      helpText =
          "Google Cloud Secret Manager secret ID containing the password to access the private key within the "
              + "Java KeyStore (JKS) file for TLS authentication with the destination Kafka cluster.",
      example = "projects/{project}/secrets/{secret}/versions/{secret_version}")
  String getKafkaWriteKeyPasswordSecretId();

  void setKafkaWriteKeyPasswordSecretId(String destinationKeyPasswordSecretId);
}
