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
  @TemplateParameter.KafkaWriteTopic(
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
        @TemplateParameter.TemplateEnumOption(
            KafkaAuthenticationMethod.APPLICATION_DEFAULT_CREDENTIALS),
        @TemplateParameter.TemplateEnumOption(KafkaAuthenticationMethod.SASL_PLAIN),
        @TemplateParameter.TemplateEnumOption(KafkaAuthenticationMethod.SASL_SCRAM_512),
        @TemplateParameter.TemplateEnumOption(KafkaAuthenticationMethod.TLS),
        @TemplateParameter.TemplateEnumOption(KafkaAuthenticationMethod.NONE),
      },
      helpText =
          "The mode of authentication to use with the Kafka cluster. "
              + "Use NONE for no authentication, SASL_PLAIN for SASL/PLAIN username and password, "
              + " SASL_SCRAM_512 for SASL_SCRAM_512 based authentication and"
              + " TLS for certificate-based authentication.")
  @Default.String(KafkaAuthenticationMethod.APPLICATION_DEFAULT_CREDENTIALS)
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
          "The Google Cloud Secret Manager secret ID that contains the Kafka username "
              + " for SASL_PLAIN authentication with the destination Kafka cluster.",
      example = "projects/<PROJECT_ID>/secrets/<SECRET_ID>/versions/<SECRET_VERSION>")
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
          "The Google Cloud Secret Manager secret ID that contains the Kafka password to use for SASL_PLAIN authentication"
              + " with the destination Kafka cluster.",
      example = "projects/<PROJECT_ID>/secrets/<SECRET_ID>/versions/<SECRET_VERSION>")
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
          "The Google Cloud Storage path to the Java KeyStore (JKS) file that contains the TLS certificate "
              + "and private key for authenticating with the destination Kafka cluster.",
      example = "gs://<BUCKET>/<KEYSTORE>.jks")
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
          "The Google Cloud Storage path to the Java TrustStore (JKS) file that contains the trusted certificates"
              + " to use to verify the identity of the destination Kafka broker.")
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
          "The Google Cloud Secret Manager secret ID that contains the password to use to access the Java TrustStore (JKS) file "
              + "for TLS authentication with the destination Kafka cluster.",
      example = "projects/<PROJECT_ID>/secrets/<SECRET_ID>/versions/<SECRET_VERSION>")
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
          "The Google Cloud Secret Manager secret ID that contains the password to access the Java KeyStore (JKS) "
              + "file to use for TLS authentication with the destination Kafka cluster.",
      example = "projects/<PROJECT_ID>/secrets/<SECRET_ID>/versions/<SECRET_VERSION>")
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
          "The Google Cloud Secret Manager secret ID that contains the password to use to access the private key within the "
              + "Java KeyStore (JKS) file for TLS authentication with the destination Kafka cluster.",
      example = "projects/<PROJECT_ID>/secrets/<SECRET_ID>/versions/<SECRET_VERSION>")
  String getKafkaWriteKeyPasswordSecretId();

  void setKafkaWriteKeyPasswordSecretId(String destinationKeyPasswordSecretId);

  @TemplateParameter.Text(
      order = 13,
      groupName = "Destination",
      parentName = "kafkaReadAuthenticationMode",
      parentTriggerValues = {KafkaAuthenticationMethod.SASL_SCRAM_512},
      optional = true,
      description = "Secret Version ID For Kafka SASL_SCRAM Username",
      helpText =
          "The Google Cloud Secret Manager secret ID that contains the Kafka username "
              + "to use with `SASL_SCRAM` authentication.",
      example = "projects/<PROJECT_ID>/secrets/<SECRET_ID>/versions/<SECRET_VERSION>")
  String getKafkaReadSaslScramUsernameSecretId();

  void setKafkaReadSaslScramUsernameSecretId(String value);

  @TemplateParameter.Text(
      order = 14,
      groupName = "Destination",
      parentName = "kafkaReadAuthenticationMode",
      parentTriggerValues = KafkaAuthenticationMethod.SASL_SCRAM_512,
      optional = true,
      description = "Secret Version ID For Kafka SASL_SCRAM Password",
      helpText =
          "The Google Cloud Secret Manager secret ID that contains the Kafka password to use with `SASL_SCRAM` authentication.",
      example = "projects/<PROJECT_ID>/secrets/<SECRET_ID>/versions/<SECRET_VERSION>")
  String getKafkaReadSaslScramPasswordSecretId();

  void setKafkaReadSaslScramPasswordSecretId(String value);

  @TemplateParameter.GcsReadFile(
      order = 15,
      optional = true,
      groupName = "Destination",
      parentName = "kafkaReadAuthenticationMode",
      parentTriggerValues = {KafkaAuthenticationMethod.SASL_SCRAM_512},
      description = "Truststore File Location",
      helpText =
          "The Google Cloud Storage path to the Java TrustStore (JKS) file that contains"
              + " the trusted certificates to use to verify the identity of the Kafka broker.")
  String getKafkaReadSaslScramTruststoreLocation();

  void setKafkaReadSaslScramTruststoreLocation(String sourceSaslScramTruststoreLocation);

  @TemplateParameter.Text(
      order = 16,
      optional = true,
      groupName = "Destination",
      parentName = "kafkaReadAuthenticationMode",
      parentTriggerValues = {KafkaAuthenticationMethod.SASL_SCRAM_512},
      description = "Secret Version ID for Truststore Password",
      helpText =
          "The Google Cloud Secret Manager secret ID that contains the password to "
              + "use to access the Java TrustStore (JKS) file for Kafka SASL_SCRAM authentication",
      example = "projects/<PROJECT_ID>/secrets/<SECRET_ID>/versions/<SECRET_VERSION>")
  String getKafkaReadSaslScramTruststorePasswordSecretId();

  void setKafkaReadSaslScramTruststorePasswordSecretId(
      String sourceSaslScramTruststorePasswordSecretId);
}
