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

public interface KafkaToKafkaOptions extends PipelineOptions, KafkaCommonOptions, CommonTemplateOptions {

  @TemplateParameter.KafkaTopic(
      order = 1,
      groupName = "Source",
      optional = false,
      description = "Source Kafka Topic",
      helpText =
          "Kafka topic to read input from."
  )
  @Validation.Required
  String getSourceTopic();

  void setSourceTopic(String sourceTopic);

  @TemplateParameter.Enum(
      groupName = "Source authentication",
      order = 2,
      name = "sourceAuthenticationMethod",
      optional = false,
      description = "Method for source Kafka authentication",
      enumOptions = {
          @TemplateParameter.TemplateEnumOption("SASL_PLAIN"),
          @TemplateParameter.TemplateEnumOption("SSL"),
          @TemplateParameter.TemplateEnumOption("No_Authentication")
      },
      helpText = "Type of authentication mechanism to authenticate to source Kafka."
  )
  @Validation.Required
  String getSourceAuthenticationMethod();

  void setSourceAuthenticationMethod(String sourceAuthenticationMethod);

  @TemplateParameter.Text(
      groupName = "source Authentication",
      order = 3,

      optional = true,
      parentName = "sourceAuthenticationMethod",
      parentTriggerValues = {"SASL_PLAIN"},

      description = "Secret version id of Kafka source username",
      helpText =
          "Secret version id from the secret manager to get Kafka SASL_PLAIN username for source Kafka.",
      example =
          "projects/your-project-number/secrets/your-secret-name/versions/your-secret-version")
  @Validation.Required
  String getSourceUsernameSecretId();

  void setSourceUsernameSecretId(String sourceUsernameSecretId);

  @TemplateParameter.Text(
      order = 4,
      groupName = "source Authentication",
      parentName = "sourceAuthenticationMethod",
      parentTriggerValues = {"SASL_PLAIN"},
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
      order = 5,
      optional = true,
      name = "sourceSSL",
      groupName = "source Authentication",
      parentName = "sourceAuthenticationMethod",
      parentTriggerValues = {"SSL"},
      description = "Location of the jks file in GCS with SSL certificate to verify identity",
      helpText =
          "Truststore File Location",
      example =
          "/your-bucket/truststore.jks"
  )
  String getSourceTruststoreLocation();

  void setSourceTruststoreLocation(String sourceTruststoreLocation);

  @TemplateParameter.Text(
      order = 6,
      optional = true,
      name = "sourceTruststorePassword",
      groupName = "source Authentication",
      parentName = "sourceAuthenticationMethod",
      parentTriggerValues = {"SSL"},
      description = "SecretId to get password to access secret in truststore for source kafka",
      helpText =
          "Secret Version Id",
      example =
          "projects/your-project-number/secrets/your-secret-name/versions/your-secret-version"
  )
  String getSourceTruststorePasswordSecretId();

  void setSourceTruststorePasswordSecretId(String sourceTruststorePasswordSecretId);

  @TemplateParameter.Text(
      order = 7,
      optional = true,
      description = "Location of Keystore",
      name = "keystoreLocation",
      groupName = "source Authentication",
      parentName = "sourceAuthenticationMethod",
      parentTriggerValues = {"SSL"},
      helpText =
          "Keystore location that contains the SSL certificate and private key.",
      example =
          "/your-bucket/keystore.jks"
  )
  String getSourceKeystoreLocation();

  void setSourceKeystoreLocation(String sourceKeystoreLocation);

  @TemplateParameter.Text(
      order = 8,
      optional = true,
      name = "sourceKeystorePassword",
      groupName = "source Authentication",
      parentName = "sourceAuthenticationMethod",
      parentTriggerValues = {"SSL"},
      description = "SecretId to get password to access secret keystore for source kafka",
      helpText =
          "Secret Version ID of Keystore Password",
      example =
          "projects/your-project-number/secrets/your-secret-name/versions/your-secret-version"
  )
  String getSourceKeystorePasswordSecretId();

  void setSourceKeystorePasswordSecretId(String sourceKeystorePasswordSecretId);

  @TemplateParameter.Text(
      order = 9,
      optional = true,
      name = "sourceKey",
      parentName = "sourceAuthenticationMethod",
      groupName = "source Authentication",
      parentTriggerValues = {"SSL"},
      description = "SecretId of password to access private key inside the keystore",
      helpText =
          "Secret Version Id of key",
      example =
          "projects/your-project-number/secrets/your-secret-name/versions/your-secret-version"

  )
  String getSourceKeyPasswordSecretId();

  void setSourceKeyPasswordSecretId(String sourceKeyPasswordSecretId);

  @TemplateParameter.KafkaTopic(
      order = 5,
      groupName = "destination",
      name = "destinationTopic",
      description = "Destination Kafka Topic",
      helpText = "Kafka topic to write the output to",
      optional = false

  )
  @Validation.Required
  String getDestinationTopic();

  void setDestinationTopic(String destinationTopic);

  @TemplateParameter.Enum(
      groupName = "destination Authentication",
      order = 6,
      name = "destinationAuthenticationMethod",
      optional = false,
      description = "Method for source Kafka authentication",
      enumOptions = {
          @TemplateParameter.TemplateEnumOption("SASL_PLAIN"),
          @TemplateParameter.TemplateEnumOption("SSL"),
          @TemplateParameter.TemplateEnumOption("No Authentication")
      },
      helpText = "Type of authentication mechanism to authenticate to source Kafka."
  )
  @Validation.Required
  String getDestinationAuthenticationMethod();

  void setDestinationAuthenticationMethod(String destinationAuthenticationMethod);

  @TemplateParameter.Text(
      optional = true,
      order = 7,
      name = "destinationUsernameSecretId",
      groupName = "destination Authentication",
      parentName = "destinationAuthenticationMethod",
      parentTriggerValues = {"SASL_PLAIN"},
      description = "Secret version id for destination Kafka username",
      helpText =
          "Secret version id from the secret manager to get Kafka SASL_PLAIN username for the destination Kafka.",
      example =
          "projects/your-project-number/secrets/your-secret-name/versions/your-secret-version")
  @Validation.Required
  String getDestinationUsernameSecretId();

  void setDestinationUsernameSecretId(String destinationUsernameSecretId);

  @TemplateParameter.Text(
      groupName = "destination Authentication",
      order = 8,
      name = "destinationPasswordSecretId",
      parentName = "destinationAuthenticationMethod",
      parentTriggerValues = {"SASL_PLAIN"},
      description = "Secret version Id for destination Kafka password",
      helpText =
          " Secret version id from the secret manager to get Kafka SASL_PLAIN password for the destination Kafka.",
      example =
          "projects/your-project-number/secrets/your-secret-name/versions/your-secret-version")
  @Validation.Required
  String getDestinationPasswordSecretId();

  void setDestinationPasswordSecretId(String destinationPasswordSecretId);

  @TemplateParameter.Text(
      order = 9,
      optional = true,
      name = "destinationSSL",
      groupName = "destination Authentication",
      parentName = "destinationAuthenticationMethod",
      parentTriggerValues = {"SSL"},
      description = "Location of the jks file in GCS with SSL certificate to verify identity",
      helpText =
          "Truststore File Location",
      example =
          "/your-bucket/truststore.jks"
  )
  String getDestinationTruststoreLocation();

  void setDestinationTruststoreLocation(String destinationTruststoreLocation);

  @TemplateParameter.Text(
      order = 10,
      optional = true,
      name = "destinationTruststorePassword",
      groupName = "destination Authentication",
      parentName = "destinationAuthenticationMethod",
      parentTriggerValues = {"SSL"},
      description = "SecretId to get password to access secret in truststore for destination kafka",
      helpText =
          "Secret Version Id",
      example =
          "projects/your-project-number/secrets/your-secret-name/versions/your-secret-version"
  )
  String getDestinationTruststorePasswordSecretId();

  void setDestinationTruststorePasswordSecretId(String destinationTruststorePasswordSecretId);

  @TemplateParameter.Text(
      order = 11,
      optional = true,
      description = "Location of Keystore",
      name = "keystoreLocation",
      groupName = "destination Authentication",
      parentName = "destinationAuthenticationMethod",
      parentTriggerValues = {"SSL"},
      helpText =
          "Keystore location that contains the SSL certificate and private key.",
      example =
          "/your-bucket/keystore.jks"
  )
  String getDestinationKeystoreLocation();

  void setDestinationKeystoreLocation(String destinationKeystoreLocation);

  @TemplateParameter.Text(
      order = 12,
      optional = true,
      name = "destinationKeystorePassword",
      groupName = "destination Authentication",
      parentName = "destinationAuthenticationMethod",
      parentTriggerValues = {"SSL"},
      description = "SecretId to get password to access secret keystore for destination kafka",
      helpText =
          "Secret Version ID of Keystore Password",
      example =
          "projects/your-project-number/secrets/your-secret-name/versions/your-secret-version"
  )
  String getDestinationKeystorePasswordSecretId();

  void setDestinationKeystorePasswordSecretId(String sourceKeystorePasswordSecretId);

  @TemplateParameter.Text(
      order = 13,
      optional = true,
      name = "destinationKey",
      parentName = "destinationAuthenticationMethod",
      groupName = "destination Authentication",
      parentTriggerValues = {"SSL"},
      description = "SecretId of password to access private key inside the keystore",
      helpText =
          "Secret Version Id of key",
      example =
          "projects/your-project-number/secrets/your-secret-name/versions/your-secret-version"

  )
  String getDestinationKeyPasswordSecretId();

  void setDestinationKeyPasswordSecretId(String destinationKeyPasswordSecretId);
  //TO-DO:remove the code below

  @TemplateParameter.Text(
      order = 14,
      optional = false,
      groupName ="removelater",
      regexes = {"[,:a-zA-Z0-9._-]+"},
      description = "Kafka Bootstrap Server List to read from",
      helpText =
          "Kafka Bootstrap Server List, separated by commas to read messages from the given input topic.",
      example = "localhost:9092, 127.0.0.1:9093")
  @Validation.Required
  String getSourceBootstrapServers();

  void setSourceBootstrapServers(String sourceBootstrapServers);

  @TemplateParameter.Text(
      order = 15,
      optional = false,
      groupName ="removeLater",
      regexes = {"[,a-zA-Z0-9._-]+"},
      description = "Kafka topic(s) to read the input from",
      helpText = "Kafka topic(s) to read the input from the given source bootstrap server.",
      example = "topic1,topic2")
  @Validation.Required
  String getInputTopic();

  void setInputTopic(String inputTopic);

  @TemplateParameter.Text(
      order = 16,
      optional = false,
      groupName ="removeLater",
      regexes = {"[,:a-zA-Z0-9._-]+"},
      description = "Output topics to write to",
      helpText =
          "Topics to write to in the destination Kafka for the data read from the source Kafka.",
      example = "topic1,topic2")
  @Validation.Required
  String getOutputTopic();

  void setOutputTopic(String outputTopic);

  @TemplateParameter.Text(
      order = 17,
      optional = false,
      groupName = "removeLater",
      regexes = {"[,:a-zA-Z0-9._-]+"},
      description = "Destination kafka Bootstrap Server",
      helpText = "Destination kafka Bootstrap Server to write data to.",
      example = "localhost:9092")
  @Validation.Required
  String getDestinationBootstrapServer();

  void setDestinationBootstrapServer(String destinationBootstrapServer);
}
