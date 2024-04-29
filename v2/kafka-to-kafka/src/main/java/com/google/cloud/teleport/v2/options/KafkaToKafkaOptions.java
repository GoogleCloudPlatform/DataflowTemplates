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
      optional = true,
      regexes = {"[,:a-zA-Z0-9._-]+"},
      description = "Kafka Bootstrap Server List to read from",
      helpText = "Kafka Bootstrap Server List, separated by commas.",
      example = "localhost:9092, 127.0.0.1:9093")
  @Validation.Required
  String getSourceBootstrapServers();

  void setSourceBootstrapServers(String sourceBootstrapServers);

  @TemplateParameter.Text(
      order = 2,
      optional = false,
      regexes = {"[,a-zA-Z0-9._-]+"},
      description = "Kafka topic(s) to read the input from",
      helpText = "Kafka topic(s) to read the input from.",
      example = "topic1,topic2")
  @Validation.Required
  String getInputTopic();

  void setInputTopic(String inputTopic);

  @TemplateParameter.Text(
      order = 3,
      optional = false,
      regexes = {"[,:a-zA-Z0-9._-]+"},
      description = "output topics to write to",
      helpText = "topics to write to for the data read from Kafka",
      example = "topic1,topic2")
  @Validation.Required
  String getOutputTopic();

  void setOutputTopic(String outputTopic);

  @TemplateParameter.Text(
      order = 4,
      optional = false,
      regexes = {"[,:a-zA-Z0-9._-]+"},
      description = "sink kafka Bootstrap Server",
      helpText = "sink kafka Bootstrap Server to write data to",
      example = "localhost:9092")
  @Validation.Required
  String getSinkBootstrapServer();

  void setSinkBootstrapServer(String sinkBootstrapServer);

  @TemplateParameter.Enum(
      order = 5,
      enumOptions = {
          @TemplateParameter.TemplateEnumOption("nonGMK-to-nonGMK"),
          @TemplateParameter.TemplateEnumOption("GMK-to-GMK"),
          @TemplateParameter.TemplateEnumOption("nonGMK-to-GMK")
      },
      optional = false,
      description = "the type of kafka-to-kafka migration",
      helpText = "Migration Type can be one of the three options `nonGMK-to-nonGMK`, `GMK-to-GMK`, nonGMK-to-GMK`"
  )
  String getMigrationType();
  void setMigrationType(String migrationType);

  @TemplateParameter.Text(
      order = 6,
      optional = true,
      description = "secretid to get SASL username for kafka source",
      helpText = "secretid for username to authenticate to kafka source"
  )
  String getSecretIdSourceUsername();
  void setSecretIdSourceUsername(String secretIdSourceUsername);

  @TemplateParameter.Text(
      order = 7,
      optional = true,
      description = "secretid to get SASL password for the kafka source",
      helpText = "secretid in secret manager to authenticate to kafka source"
  )

  String getSecretIdSourcePassword();
  void setSecretIdSourcePassword(String secretIdSourcePassword);

  @TemplateParameter.Text(
      order = 8,
      optional = true,
      description = "secretid to get SASL username for kafka sink",
      helpText = "secretid to get password to authenticate to kafka sink"
  )
  String getSecretIdSinkUsername();
  void setSecretIdSinkUsername(String secretIdSinkUsername);
  @TemplateParameter.Text(
      order = 9,
      optional = true,
      description = "secretid to get password for the kafka sink",
      helpText = "secretid in secret manager to get access to the secret"
  )
  String getSecretIdSinkPassword();
  void setSecretIdSinkPassword(String secretIdSinkPassword);

  @TemplateParameter.Text(
      order = 10,
      optional = true,
      description = "versionid from the secret manager to get username for source",
      helpText = "version of the secret"
  )

  String getVersionIdSourceUsername();
  void setVersionIdSourceUsername(String versionIdSourceUsername);

  @TemplateParameter.Text(
      order =11,
      optional = true,
      description = "versionId to get password for source",
      helpText = "version of the secret"
  )
  String getVersionIdSourcePassword();
  void setVersionIdSourcePassword(String versionIdSourcePassword);

  @TemplateParameter.Text(
      order = 12,
      optional = true,
      description = "versionid for username secret for sink",
      helpText = "version for the sink secret"
  )
  String getVersionIdSinkUsername();
  void setVersionIdSinkUsername(String versionIdSinkUsername);

  @TemplateParameter.Text(
      order =13,
      optional = true,
      description = "versionId four password for sink",
      helpText = "version for sink password secret"
  )
  String getVersionIdSinkPassword();
  void setVersionIdSinkPassword(String versionIdSinkPassword);
  @TemplateParameter.Text(
      order = 14,
      optional = true,
      description = "project number",
      helpText = "project number for the project"
  )
  String getProjectNumber();
  void setProjectNumber(String projectNumber);

  @TemplateParameter.Enum(
      order = 15,
      optional = true,
      description = "method for kafka authentication",
      enumOptions = {
          @TemplateParameter.TemplateEnumOption("secret manager"),
          @TemplateParameter.TemplateEnumOption("secretstore url"),
      },
      helpText = "type of authentication protocol for kafka."
  )
  String getAuthenticationMethod();
  void setAuthenticationMethod(String authenticationMethod);

}
