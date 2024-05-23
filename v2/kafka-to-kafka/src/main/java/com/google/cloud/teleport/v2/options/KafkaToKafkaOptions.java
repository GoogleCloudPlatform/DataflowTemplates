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
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;

public interface KafkaToKafkaOptions extends PipelineOptions {

  @TemplateParameter.KafkaTopic(
      order = 1,
      groupName = "Source",
      optional = false,
      helpText = "Source Kafka Topic",
      description = "Kafka topic to read input from.")
  @Validation.Required
  String getSourceTopic();

  void setSourceTopic(String sourceTopic);

  @TemplateParameter.Enum(
      groupName = "Source",
      order = 2,
      name = "startOffset",
      optional = false,
      helpText = "Kafka offset",
      description = "Kafka offset to read data",
      enumOptions = {
        @TemplateParameter.TemplateEnumOption("latest"),
        @TemplateParameter.TemplateEnumOption("earliest")
      })
  @Validation.Required
  String getKafkaOffset();

  void setKafkaOffset(String kafkaOffset);

  @TemplateParameter.Enum(
      groupName = "Source",
      order = 3,
      name = "sourceAuthenticationMethod",
      optional = false,
      helpText = "Source Authentication Mode",
      enumOptions = {
        @TemplateParameter.TemplateEnumOption("SASL_PLAIN"),
        @TemplateParameter.TemplateEnumOption("SSL"),
        @TemplateParameter.TemplateEnumOption("No_Authentication")
      },
      description = "Type of authentication mechanism to authenticate to source Kafka.")
  @Validation.Required
  String getSourceAuthenticationMethod();

  void setSourceAuthenticationMethod(String sourceAuthenticationMethod);

  @TemplateParameter.Text(
      groupName = "Source",
      order = 4,
      optional = true,
      parentName = "sourceAuthenticationMethod",
      parentTriggerValues = {"SASL_PLAIN"},
      helpText = "Secret version id for Username",
      description =
          "Secret version id from the secret manager to get Kafka SASL_PLAIN username for source Kafka.",
      example =
          "projects/your-project-number/secrets/your-secret-name/versions/your-secret-version")
  @Validation.Required
  String getSourceUsernameSecretId();

  void setSourceUsernameSecretId(String sourceUsernameSecretId);

  @TemplateParameter.Text(
      order = 5,
      groupName = "Source",
      parentName = "sourceAuthenticationMethod",
      parentTriggerValues = {"SASL_PLAIN"},
      optional = true,
      helpText = "Secret version id of password",
      description =
          "Secret version id from the secret manager to get Kafka SASL_PLAIN password for the source Kafka.",
      example =
          "projects/your-project-number/secrets/your-secret-name/versions/your-secret-version")
  @Validation.Required
  String getSourcePasswordSecretId();

  void setSourcePasswordSecretId(String sourcePasswordSecretId);

  @TemplateParameter.Text(
      order = 6,
      optional = true,
      name = "sourceSSL",
      groupName = "Source",
      parentName = "sourceAuthenticationMethod",
      parentTriggerValues = {"SSL"},
      description = "Location of the jks file in GCS with SSL certificate to verify identity.",
      helpText = "Truststore File Location",
      example = "/your-bucket/truststore.jks")
  String getSourceTruststoreLocation();

  void setSourceTruststoreLocation(String sourceTruststoreLocation);

  @TemplateParameter.Text(
      order = 7,
      optional = true,
      name = "sourceTruststorePassword",
      groupName = "Source",
      parentName = "sourceAuthenticationMethod",
      parentTriggerValues = {"SSL"},
      description = "SecretId to get password to access secret in truststore for source kafka.",
      helpText = "Secret Version Id for truststore password",
      example =
          "projects/your-project-number/secrets/your-secret-name/versions/your-secret-version")
  String getSourceTruststorePasswordSecretId();

  void setSourceTruststorePasswordSecretId(String sourceTruststorePasswordSecretId);

  @TemplateParameter.Text(
      order = 8,
      optional = true,
      description = "Keystore location that contains the SSL certificate and private key.",
      name = "keystoreLocation",
      groupName = "Source",
      parentName = "sourceAuthenticationMethod",
      parentTriggerValues = {"SSL"},
      helpText = "Location of keystore",
      example = "/your-bucket/keystore.jks")
  String getSourceKeystoreLocation();

  void setSourceKeystoreLocation(String sourceKeystoreLocation);

  @TemplateParameter.Text(
      order = 9,
      optional = true,
      name = "sourceKeystorePassword",
      groupName = "Source",
      parentName = "sourceAuthenticationMethod",
      parentTriggerValues = {"SSL"},
      description = "SecretId to get password to access secret keystore for source kafka.",
      helpText = "Secret Version ID of Keystore Password",
      example =
          "projects/your-project-number/secrets/your-secret-name/versions/your-secret-version")
  String getSourceKeystorePasswordSecretId();

  void setSourceKeystorePasswordSecretId(String sourceKeystorePasswordSecretId);

  @TemplateParameter.Text(
      order = 10,
      optional = true,
      name = "sourceKey",
      parentName = "sourceAuthenticationMethod",
      groupName = "Source",
      parentTriggerValues = {"SSL"},
      description = "SecretId of password to access private key inside the keystore.",
      helpText = "Secret Version Id of key",
      example =
          "projects/your-project-number/secrets/your-secret-name/versions/your-secret-version")
  String getSourceKeyPasswordSecretId();

  void setSourceKeyPasswordSecretId(String sourceKeyPasswordSecretId);

  @TemplateParameter.KafkaTopic(
      order = 11,
      groupName = "Destination",
      name = "destinationTopic",
      helpText = "Destination Kafka Topic",
      description = "Kafka topic to write the output to.",
      optional = false)
  @Validation.Required
  String getDestinationTopic();

  void setDestinationTopic(String destinationTopic);

  @TemplateParameter.Enum(
      groupName = "Destination",
      order = 12,
      name = "destinationAuthenticationMethod",
      optional = false,
      helpText = "Destination Authentication Method",
      enumOptions = {
        @TemplateParameter.TemplateEnumOption("SASL_PLAIN"),
        @TemplateParameter.TemplateEnumOption("SSL"),
        @TemplateParameter.TemplateEnumOption("No Authentication")
      },
      description = "Type of authentication mechanism to authenticate to destination Kafka.")
  @Validation.Required
  String getDestinationAuthenticationMethod();

  void setDestinationAuthenticationMethod(String destinationAuthenticationMethod);

  @TemplateParameter.Text(
      optional = true,
      order = 13,
      name = "destinationUsernameSecretId",
      groupName = "Destination",
      parentName = "destinationAuthenticationMethod",
      parentTriggerValues = {"SASL_PLAIN"},
      description = "Secret version id for Kafka username.",
      helpText = "Secret version id of Kafka SASL_PLAIN username.",
      example =
          "projects/your-project-number/secrets/your-secret-name/versions/your-secret-version")
  @Validation.Required
  String getDestinationUsernameSecretId();

  void setDestinationUsernameSecretId(String destinationUsernameSecretId);

  @TemplateParameter.Text(
      groupName = "Destination",
      order = 14,
      name = "destinationPasswordSecretId",
      parentName = "destinationAuthenticationMethod",
      parentTriggerValues = {"SASL_PLAIN"},
      description = "Secret version Id for destination Kafka password.",
      helpText = " Secret version id of SASL_PLAIN password",
      example =
          "projects/your-project-number/secrets/your-secret-name/versions/your-secret-version")
  @Validation.Required
  String getDestinationPasswordSecretId();

  void setDestinationPasswordSecretId(String destinationPasswordSecretId);

  @TemplateParameter.Text(
      order = 15,
      optional = true,
      name = "truststoredestination",
      groupName = "Destination",
      parentName = "destinationAuthenticationMethod",
      parentTriggerValues = {"SSL"},
      description = "Location of the jks file in GCS with SSL certificate to verify identity.",
      helpText = "Truststore File Location",
      example = "/your-bucket/truststore.jks")
  String getDestinationTruststoreLocation();

  void setDestinationTruststoreLocation(String destinationTruststoreLocation);

  @TemplateParameter.Text(
      order = 16,
      optional = true,
      name = "destinationTruststorePassword",
      groupName = "Destination",
      parentName = "destinationAuthenticationMethod",
      parentTriggerValues = {"SSL"},
      description =
          "SecretId to get password to access secret in truststore for destination kafka.",
      helpText = "Secret Version Id of Truststore password",
      example =
          "projects/your-project-number/secrets/your-secret-name/versions/your-secret-version")
  String getDestinationTruststorePasswordSecretId();

  void setDestinationTruststorePasswordSecretId(String destinationTruststorePasswordSecretId);

  @TemplateParameter.Text(
      order = 17,
      optional = true,
      description = "SecretId of Keystore password.",
      name = "keystoreLocation",
      groupName = "Destination",
      parentName = "destinationAuthenticationMethod",
      parentTriggerValues = {"SSL"},
      helpText = "Location of Keystore",
      example = "/your-bucket/keystore.jks")
  String getDestinationKeystoreLocation();

  void setDestinationKeystoreLocation(String destinationKeystoreLocation);

  @TemplateParameter.Text(
      order = 18,
      optional = true,
      name = "destinationKeystorePassword",
      groupName = "Destination",
      parentName = "destinationAuthenticationMethod",
      parentTriggerValues = {"SSL"},
      description = "SecretId to get password to access secret keystore for destination kafka.",
      helpText = "Secret Version ID of Keystore Password",
      example =
          "projects/your-project-number/secrets/your-secret-name/versions/your-secret-version")
  String getDestinationKeystorePasswordSecretId();

  void setDestinationKeystorePasswordSecretId(String destinationKeystorePasswordSecretId);

  @TemplateParameter.Text(
      order = 19,
      optional = true,
      name = "destinationKey",
      parentName = "destinationAuthenticationMethod",
      groupName = "Destination",
      parentTriggerValues = {"SSL"},
      description = "SecretId of password to access private key inside the keystore.",
      helpText = "Secret Version Id of key",
      example =
          "projects/your-project-number/secrets/your-secret-name/versions/your-secret-version")
  String getDestinationKeyPasswordSecretId();

  void setDestinationKeyPasswordSecretId(String destinationKeyPasswordSecretId);
}
//   //TO-DO:remove the code below
//
//   @TemplateParameter.Text(
//       order = 14,
//       optional = false,
//       groupName ="removeLater",
//       regexes = {"[,:a-zA-Z0-9._-]+"},
//       description = "Kafka Bootstrap Server List to read from",
//       helpText =
//           "Kafka Bootstrap Server List, separated by commas to read messages from the given input
// topic.",
//       example = "localhost:9092, 127.0.0.1:9093")
//   @Validation.Required
//   String getSourceBootstrapServers();
//
//   void setSourceBootstrapServers(String sourceBootstrapServers);
//
//   @TemplateParameter.Text(
//       order = 15,
//       optional = false,
//       groupName ="removeLater",
//       regexes = {"[,a-zA-Z0-9._-]+"},
//       description = "Kafka topic(s) to read the input from",
//       helpText = "Kafka topic(s) to read the input from the given source bootstrap server.",
//       example = "topic1,topic2")
//   @Validation.Required
//   String getInputTopic();
//
//   void setInputTopic(String inputTopic);
//
//   @TemplateParameter.Text(
//       order = 16,
//       optional = false,
//       groupName ="removeLater",
//       regexes = {"[,:a-zA-Z0-9._-]+"},
//       description = "Output topics to write to",
//       helpText =
//           "Topics to write to in the destination Kafka for the data read from the source Kafka.",
//       example = "topic1,topic2")
//   @Validation.Required
//   String getOutputTopic();
//
//   void setOutputTopic(String outputTopic);
//
//   @TemplateParameter.Text(
//       order = 17,
//       optional = false,
//       groupName = "removeLater",
//       regexes = {"[,:a-zA-Z0-9._-]+"},
//       description = "Destination kafka Bootstrap Server",
//       helpText = "Destination kafka Bootstrap Server to write data to.",
//       example = "localhost:9092")
//   @Validation.Required
//   String getDestinationBootstrapServer();
//
//   void setDestinationBootstrapServer(String destinationBootstrapServer);
// }
