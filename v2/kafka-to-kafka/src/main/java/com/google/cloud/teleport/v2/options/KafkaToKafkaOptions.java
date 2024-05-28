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
import com.google.cloud.teleport.v2.kafka.options.KafkaReadOptions;
import com.google.cloud.teleport.v2.kafka.options.KafkaWriteOptions;
import com.google.cloud.teleport.v2.kafka.values.KafkaAuthenticationMethod;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;


public interface KafkaToKafkaOptions extends PipelineOptions, KafkaReadOptions, KafkaWriteOptions {

  @TemplateParameter.KafkaTopic(

      order = 1,
      groupName = "Source",
      optional = false,
      description = "Source Kafka Topic",
      helpText = "Kafka topic to read input from.")
  @Validation.Required
  String getSourceTopic();

  void setSourceTopic(String sourceTopic);

  @TemplateParameter.Text(

      order = 2,
      groupName = "Source",
      optional = false,
      description = "Project of Source Kafka",
      helpText = "Project where source kafka resides.")
  @Validation.Required
  String getSourceProject();

  void setSourceProject(String sourceProject);

  @TemplateParameter.Enum(
      groupName = "Source",
      order = 3,
      name = "sourceAuthenticationMethod",

      optional = false,
      description = "Source Authentication Mode",
      enumOptions = {
          @TemplateParameter.TemplateEnumOption(KafkaAuthenticationMethod.SASL_PLAIN),
          @TemplateParameter.TemplateEnumOption(KafkaAuthenticationMethod.SSL),
          @TemplateParameter.TemplateEnumOption(KafkaAuthenticationMethod.NONE)
      },

      helpText = "Type of authentication mechanism to use with the source Kafka.")
  @Validation.Required
  @Default.String(KafkaAuthenticationMethod.NONE)
  String getSourceAuthenticationMethod();
  void setSourceAuthenticationMethod(String sourceAuthenticationMethod);

  @TemplateParameter.Text(
      groupName = "Source",
      order = 4,
      optional = true,

      parentName="sourceAuthenticationMethod",
      parentTriggerValues = {KafkaAuthenticationMethod.SASL_PLAIN},
      description = "Secret Version ID for Username",
      helpText =
          "Secret Version ID from the secret manager to get Kafka SASL_PLAIN username for source Kafka.",
      example =
          "projects/your-project-number/secrets/your-secret-name/versions/your-secret-version")

  String getSourceUsernameSecretId();

  void setSourceUsernameSecretId(String sourceUsernameSecretId);

  @TemplateParameter.Text(

      order=5,
      groupName = "Source",
      parentName = "sourceAuthenticationMethod",
      parentTriggerValues = {KafkaAuthenticationMethod.SASL_PLAIN},

          optional=true,
      description = "Secret version id of password",
      helpText =
          "Secret version id from the secret manager to get Kafka SASL_PLAIN password for the source Kafka.",
      example =
          "projects/your-project-number/secrets/your-secret-name/versions/your-secret-version")

  String getSourcePasswordSecretId();

  void setSourcePasswordSecretId(String sourcePasswordSecretId);

  @TemplateParameter.GcsReadFile(
      order = 6,
      optional = true,
      name = "sourceSSL",
      groupName = "Source",
      parentName = "sourceAuthenticationMethod",
      parentTriggerValues = {KafkaAuthenticationMethod.SSL},
      description = "Truststore File Location",
      helpText =

          "Location of the jks file in Cloud Storage with SSL certificate to verify identity.",
      example = "gs://your-bucket/truststore.jks")
  String getSourceTruststoreLocation();

  void setSourceTruststoreLocation(String sourceTruststoreLocation);

  @TemplateParameter.Text(
      order = 7,
      optional = true,
      name = "sourceTruststorePassword",
      groupName = "Source",
      parentName = "sourceAuthenticationMethod",
      parentTriggerValues = {KafkaAuthenticationMethod.SSL},
      helpText =
          "Secret Version ID to get password to access secret in truststore for source Kafka.",
      description = "Secret Version ID for Truststore Password",
      example =
          "projects/your-project-number/secrets/your-secret-name/versions/your-secret-version")
  String getSourceTruststorePasswordSecretId();

  void setSourceTruststorePasswordSecretId(String sourceTruststorePasswordSecretId);

  @TemplateParameter.GcsReadFile(
      order = 8,
      optional = true,
      name = "keystoreLocation",
      groupName = "Source",
      parentName = "sourceAuthenticationMethod",
      parentTriggerValues = {KafkaAuthenticationMethod.SSL},
      helpText =
          "Cloud storage path for the Keystore location that contains the SSL certificate and private key.",
      description = "Location of Keystore",
      example = "gs://your-bucket/keystore.jks")
  String getSourceKeystoreLocation();

  void setSourceKeystoreLocation(String sourceKeystoreLocation);

  @TemplateParameter.Text(
      order = 9,
      optional = true,
      name = "sourceKeystorePassword",
      groupName = "Source",
      parentName = "sourceAuthenticationMethod",
      parentTriggerValues = {KafkaAuthenticationMethod.SSL},
      helpText = "Secret Version ID to get password to access secret keystore, for source kafka.",
      description = "Secret Version ID of Keystore Password",
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
      parentTriggerValues = {KafkaAuthenticationMethod.SSL},
      helpText =
          "Secret Version ID of password to access private key inside the keystore, for source Kafka.",
      description = "Secret Version ID of Private Key Password",
      example =
          "projects/your-project-number/secrets/your-secret-name/versions/your-secret-version")
  String getSourceKeyPasswordSecretId();

  void setSourceKeyPasswordSecretId(String sourceKeyPasswordSecretId);

}
