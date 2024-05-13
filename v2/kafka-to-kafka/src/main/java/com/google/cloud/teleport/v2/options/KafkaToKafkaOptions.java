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
package com.google.cloud.teleport.v2.options;

import com.google.cloud.teleport.metadata.TemplateParameter;
import com.google.cloud.teleport.v2.kafka.options.KafkaCommonOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;

public interface KafkaToKafkaOptions extends PipelineOptions, KafkaCommonOptions {
  @TemplateParameter.Text(
      order = 1,
      optional = false,
      regexes = {"[,:a-zA-Z0-9._-]+"},
      description = "Kafka Bootstrap Server List to read from",
      helpText =
          "Kafka Bootstrap Server List, separated by commas to read messages from the given input topic.",
      example = "localhost:9092, 127.0.0.1:9093")
  @Validation.Required
  String getSourceBootstrapServers();

  void setSourceBootstrapServers(String sourceBootstrapServers);

  @TemplateParameter.Text(
      groupName = "Source",
      optional = false,
      regexes = {"[,a-zA-Z0-9._-]+"},
      description = "Kafka topic(s) to read the input from",
      helpText = "Kafka topic(s) to read the input from the given source bootstrap server.",
      example = "topic1,topic2")
  @Validation.Required
  String getInputTopic();

  void setInputTopic(String inputTopic);

  @TemplateParameter.Text(
      order = 3,
      optional = true,
      regexes = {"[,:a-zA-Z0-9._-]+"},
      description = "Output topics to write to",
      helpText =
          "Topics to write to in the destination Kafka for the data read from the source Kafka.",
      example = "topic1,topic2")
  String getOutputTopic();

  void setOutputTopic(String outputTopic);

  @TemplateParameter.Text(
      order = 4,
      optional = false,
      regexes = {"[,:a-zA-Z0-9._-]+"},
      description = "Destination kafka Bootstrap Server",
      helpText = "Destination kafka Bootstrap Server to write data to.",
      example = "localhost:9092")
  @Validation.Required
  String getDestinationBootstrapServer();

  void setDestinationBootstrapServer(String destinationBootstrapServer);

  @TemplateParameter.Enum(
      order = 5,
      enumOptions = {
          @TemplateParameter.TemplateEnumOption("nonGMK-to-nonGMK"),
          @TemplateParameter.TemplateEnumOption("GMK-to-GMK"),
          @TemplateParameter.TemplateEnumOption("nonGMK-to-GMK")
      },
      optional = true,
      description = "The type of kafka-to-kafka migration",
      helpText = "Migration type for the data movement from a source to a destination kafka.")
  @Validation.Required
  String getMigrationType();

  void setMigrationType(String migrationType);

  @TemplateParameter.Enum(
      order = 6,
      optional = true,
      description = "Method for source kafka authentication",
      enumOptions = {
          @TemplateParameter.TemplateEnumOption("SASL_PLAIN"),
          @TemplateParameter.TemplateEnumOption("SSL"),
      },
      helpText = "Type of authentication mechanism to authenticate to source Kafka."
  )
  @Validation.Required
  String getSourceAuthenticationMethod();

  void setSourceAuthenticationMethod(String sourceAuthenticationMethod);

  @TemplateParameter.Enum(
      order = 7,
      optional = true,
      description = "Method for sink kafka authentication",
      enumOptions = {
          @TemplateParameter.TemplateEnumOption("SASL_PLAIN"),
          @TemplateParameter.TemplateEnumOption("SSL"),
      },
      helpText = "Type of authentication mechanism to authenticate to sink Kafka."
  )
  @Validation.Required
  String getDestinationAuthenticationMethod();

  void setDestinationAuthenticationMethod(String destinationAuthenticationMethod);

  @TemplateParameter.Text(
      order = 8,
      optional = true,
      description = "Secret version id of Kafka source username",
      helpText =
          "Secret version id from the secret manager to get Kafka SASL_PLAIN username for source Kafka.",
      example =
          "projects/your-project-number/secrets/your-secret-name/versions/your-secret-version")
  @Validation.Required
  String getSourceUsernameSecretId();

  void setSourceUsernameSecretId(String sourceUsernameSecretId);

  @TemplateParameter.Text(
      order = 9,
      optional = true,
      description = "Secret version of Kafka source password",
      helpText =
          "Secret version id from the secret manager to get Kafka SASL_PLAIN password for the source Kafka.",
      example =
          "projects/your-project-number/secrets/your-secret-name/versions/your-secret-version")
  @Validation.Required
  String getSourcePasswordSecretId();

  void setSourcePasswordSecretId(String sourcePasswordSecretId);

  @TemplateParameter.Text(
      order = 10,
      optional = true,
      description = "Secret version id for destination Kafka username",
      helpText =
          "Secret version id from the secret manager to get Kafka SASL_PLAIN username for the destination Kafka.",
      example =
          "projects/your-project-number/secrets/your-secret-name/versions/your-secret-version")
  @Validation.Required
  String getDestinationUsernameSecretId();

  void setDestinationUsernameSecretId(String destinationUsernameSecretId);

  @TemplateParameter.Text(
      order = 11,
      optional = true,
      description = "Secret version Id for destination Kafka password",
      helpText =
          " Secret version id from the secret manager to get Kafka SASL_PLAIN password for the destination Kafka.",
      example =
          "projects/your-project-number/secrets/your-secret-name/versions/your-secret-version")
  @Validation.Required
  String getDestinationPasswordSecretId();

  void setDestinationPasswordSecretId(String destinationPasswordSecretId);

  @TemplateParameter.Text(
      order = 12,
      optional = true,
      description = "Location of the jks file in GCS with SSL certificate to verify identity",
      helpText =
          "Location of the SSL certificate where the trust store for authentication to Kafka are stored.",
      example =
          "/your-bucket/truststore.jks"
  )
  String getSourceTruststoreLocation();

  void setSourceTruststoreLocation(String sourceTruststoreLocation);

  @TemplateParameter.Text(
      order = 13,
      optional = true,
      description = "SecretId to get password to access secret in truststore for source kafka",
      helpText =
          "SecretId in secret manager where the password to access secret in truststore is stored.",
      example =
          "projects/your-project-number/secrets/your-secret-name/versions/your-secret-version"
  )

  String getSourceTruststorePasswordSecretId();
  void setSourceTruststorePasswordSecretId(String sourceTruststorePasswordSecretId);

  @TemplateParameter.Text(
      order = 14,
      optional = true,
      description = "Path to keystore",
      helpText =
          "Keystore location that contains the SSL certificate and private key.",
      example =
          "/your-bucket/keystore.jks"
  )

  String getSourceKeystoreLocation();
  void setSourceKeystoreLocation(String sourceKeystoreLocation);

  @TemplateParameter.Text(
      order = 15,
      optional = true,
      description = "SecretId to get password to access secret in truststore for source kafka",
      helpText =
          "SecretId in secret manager where the password to access the keystore file",
      example =
          "projects/your-project-number/secrets/your-secret-name/versions/your-secret-version"
  )

  String getSourceKeystorePasswordSecretId();
  void setSourceKeystorePasswordSecretId(String sourceKeystorePasswordSecretId);

  @TemplateParameter.Text(
      order = 16,
      optional = true,
      description = "SecretId of password to access private key inside the keystore",
      helpText =
          "SecretId of password required to access the client's private key stored within the keystore",
      example =
          "projects/your-project-number/secrets/your-secret-name/versions/your-secret-version"

  )

  String getSourceKeyPasswordSecretId();
  void setSourceKeyPasswordSecretId(String sourceKeyPasswordSecretId);

  @TemplateParameter.Text(
      order = 17,
      optional = true,
      description = "Location of the jks file in GCS with SSL certificate to verify identity",
      helpText =
          "Location of the SSL certificate where the trust store for authentication to Kafka are stored.",
      example =
          "/your-bucket/truststore.jks"
  )

  String getDestinationTruststoreLocation();

  void setDestinationTruststoreLocation(String destinationTruststoreLocation);

  @TemplateParameter.Text(
      order = 18,
      optional = true,
      description = "SecretId to get password to access secret in truststore for source kafka",
      helpText =
          "SecretId in secret manager where the password to access secret in truststore is stored.",
      example =
          "projects/your-project-number/secrets/your-secret-name/versions/your-secret-version"
  )

  String getDestinationTruststorePasswordSecretId();
  void setDestinationTruststorePasswordSecretId(String destinationTruststorePasswordSecretId);

  @TemplateParameter.Text(
      order = 19,
      optional = true,
      description = "Path to keystore",
      helpText =
          "Keystore location that contains the SSL certificate and private key.",
      example =
          "/your-bucket/keystore.jks"
  )

  String getDestinationKeystoreLocation();
  void setDestinationKeystoreLocation(String destinationKeystoreLocation);

  @TemplateParameter.Text(
      order = 20,
      optional = true,
      description = "SecretId to get password to access secret in truststore for source kafka",
      helpText =
          "SecretId in secret manager where the password to access the keystore file",
      example =
          "projects/your-project-number/secrets/your-secret-name/versions/your-secret-version"
  )

  String getDestinationKeystorePasswordSecretId();
  void setDestinationKeystorePasswordSecretId(String destinationKeystorePasswordSecretId);

  @TemplateParameter.Text(
      order = 21,
      optional = true,
      description = "SecretId of password to access private key inside the keystore",
      helpText =
          "SecretId of password required to access the client's private key stored within the keystore",
      example =
          "projects/your-project-number/secrets/your-secret-name/versions/your-secret-version"

  )

  String getDestinationKeyPasswordSecretId();
  void setDestinationKeyPasswordSecretId(String destinationKeyPasswordSecretId);
}
