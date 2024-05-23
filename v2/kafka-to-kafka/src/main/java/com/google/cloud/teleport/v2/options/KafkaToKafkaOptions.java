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
      description = "Source Kafka Topic",
      helpText = "Kafka topic to read input from.")

  @Validation.Required
  String getSourceTopic();

  void setSourceTopic(String sourceTopic);


  @TemplateParameter.Enum(
      groupName = "Source authentication",
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
      description = "Source Authentication Mode",
      enumOptions = {
        @TemplateParameter.TemplateEnumOption("SASL_PLAIN"),
        @TemplateParameter.TemplateEnumOption("SSL"),
        @TemplateParameter.TemplateEnumOption("No_AUTHENTICATION")
      },
      helpText = "Type of authentication mechanism to authenticate to source Kafka.")

  @Validation.Required
  String getSourceAuthenticationMethod();

  void setSourceAuthenticationMethod(String sourceAuthenticationMethod);

  @TemplateParameter.Text(


      order = 4,

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
      order = 5,
      groupName = "Source",
      parentName = "sourceAuthenticationMethod",
      parentTriggerValues = {"SASL_PLAIN"},
      optional = true,
      description = "Secret version id of password",
      helpText =
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
      groupName = "source Authentication",
      parentName = "sourceAuthenticationMethod",
      parentTriggerValues = {"SSL"},
      helpText = "Location of the jks file in GCS with SSL certificate to verify identity.",
      description = "Truststore File Location",
      example = "gs://your-bucket/truststore.jks")

  String getSourceTruststoreLocation();

  void setSourceTruststoreLocation(String sourceTruststoreLocation);

  @TemplateParameter.Text(
      order = 7,
      optional = true,
      name = "sourceTruststorePassword",
      groupName = "source Authentication",
      parentName = "sourceAuthenticationMethod",
      parentTriggerValues = {"SSL"},

      helpText = "SecretId to get password to access secret in truststore for source kafka.",
      description = "Secret Version Id for truststore password",

      example =
          "projects/your-project-number/secrets/your-secret-name/versions/your-secret-version"
  )
  String getSourceTruststorePasswordSecretId();

  void setSourceTruststorePasswordSecretId(String sourceTruststorePasswordSecretId);

  @TemplateParameter.Text(
      order = 8,
      optional = true,

      name = "keystoreLocation",
      groupName = "source Authentication",
      parentName = "sourceAuthenticationMethod",
      parentTriggerValues = {"SSL"},

      helpText =
          "Keystore location that contains the SSL certificate and private key.",



      description = "Location of keystore",
      example = "gs://your-bucket/keystore.jks")

  String getSourceKeystoreLocation();

  void setSourceKeystoreLocation(String sourceKeystoreLocation);

  @TemplateParameter.Text(
      order = 9,
      optional = true,
      name = "sourceKeystorePassword",
      groupName = "source Authentication",
      parentName = "sourceAuthenticationMethod",
      parentTriggerValues = {"SSL"},
      helpText = "SecretId to get password to access secret keystore for source kafka.",
      description = "Secret Version ID of Keystore Password",
      example =
          "projects/your-project-number/secrets/your-secret-name/versions/your-secret-version"
  )
  String getSourceKeystorePasswordSecretId();

  void setSourceKeystorePasswordSecretId(String sourceKeystorePasswordSecretId);

  @TemplateParameter.Text(
      order = 10,
      optional = true,
      name = "sourceKey",
      parentName = "sourceAuthenticationMethod",
      groupName = "source Authentication",
      parentTriggerValues = {"SSL"},

      helpText = "SecretId of password to access private key inside the keystore.",
      description = "Secret Version Id of key",
      example =
          "projects/your-project-number/secrets/your-secret-name/versions/your-secret-version"

  )
  String getSourceKeyPasswordSecretId();

  void setSourceKeyPasswordSecretId(String sourceKeyPasswordSecretId);



  @TemplateParameter.KafkaTopic(
      order = 11,
      groupName = "destination",
      name = "destinationTopic",
      description = "Destination Kafka Topic",
      helpText = "Kafka topic to write the output to",

      optional = false)

  @Validation.Required
  String getDestinationTopic();

  void setDestinationTopic(String destinationTopic);

  @TemplateParameter.Enum(
      groupName = "destination Authentication",
      order = 12,
      name = "destinationAuthenticationMethod",
      optional = false,

      description = "Destination Authentication Method",

      enumOptions = {
        @TemplateParameter.TemplateEnumOption("SASL_PLAIN"),
        @TemplateParameter.TemplateEnumOption("SSL"),
        @TemplateParameter.TemplateEnumOption("No_AUTHENTICATION")
      },
      helpText = "Type of authentication mechanism to authenticate to destination Kafka.")

  @Validation.Required
  String getDestinationAuthenticationMethod();

  void setDestinationAuthenticationMethod(String destinationAuthenticationMethod);

  @TemplateParameter.Text(
      optional = true,
      order = 13,
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
      order = 14,
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
      order = 15,
      optional = true,
      name = "destinationSSL",
      groupName = "destination Authentication",
      parentName = "destinationAuthenticationMethod",
      parentTriggerValues = {"SSL"},

      helpText = "Location of the jks file in GCS with SSL certificate to verify identity.",
      description = "Truststore File Location",
      example = "gs://your-bucket/truststore.jks")
  String getDestinationTruststoreLocation();

  void setDestinationTruststoreLocation(String destinationTruststoreLocation);

  @TemplateParameter.Text(
      order = 16,
      optional = true,
      name = "destinationTruststorePassword",
      groupName = "destination Authentication",
      parentName = "destinationAuthenticationMethod",
      parentTriggerValues = {"SSL"},
      helpText = "SecretId to get password to access secret in truststore for destination kafka.",
      description = "Secret Version Id of Truststore password",
      example =
          "projects/your-project-number/secrets/your-secret-name/versions/your-secret-version"
  )
  String getDestinationTruststorePasswordSecretId();

  void setDestinationTruststorePasswordSecretId(String destinationTruststorePasswordSecretId);

  @TemplateParameter.Text(
      order = 17,
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
      order = 18,
      optional = true,
      name = "destinationKeystorePassword",
      groupName = "destination Authentication",
      parentName = "destinationAuthenticationMethod",
      parentTriggerValues = {"SSL"},


      helpText = "SecretId to get password to access secret keystore for destination kafka.",
      description = "Secret Version ID of Keystore Password",

      example =
          "projects/your-project-number/secrets/your-secret-name/versions/your-secret-version"
  )
  String getDestinationKeystorePasswordSecretId();

  void setDestinationKeystorePasswordSecretId(String sourceKeystorePasswordSecretId);

  @TemplateParameter.Text(
      order = 19,
      optional = true,
      name = "destinationKey",
      parentName = "destinationAuthenticationMethod",
      groupName = "destination Authentication",
      parentTriggerValues = {"SSL"},


      helpText = "SecretId of password to access private key inside the keystore.",
      description = "Secret Version Id of key",

      example =
          "projects/your-project-number/secrets/your-secret-name/versions/your-secret-version"

  )
  String getDestinationKeyPasswordSecretId();

  void setDestinationKeyPasswordSecretId(String destinationKeyPasswordSecretId);
}
