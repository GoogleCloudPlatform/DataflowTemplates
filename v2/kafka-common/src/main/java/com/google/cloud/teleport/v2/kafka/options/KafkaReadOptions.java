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
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptions;

/**
 * The {@link KafkaReadOptions} interface provides the custom execution options passed by the
 * executor at the command-line.
 */
public interface KafkaReadOptions extends PipelineOptions {

  @TemplateParameter.Text(
      order = 1,
      optional = true,
      regexes = {"[,:a-zA-Z0-9._-]+"},
      description = "Kafka Bootstrap Server list",
      helpText = "Kafka Bootstrap Server list, separated by commas.",
      example = "localhost:9092,127.0.0.1:9093")
  String getReadBootstrapServers();

  void setReadBootstrapServers(String bootstrapServers);

  @TemplateParameter.Text(
      order = 2,
      optional = true,
      regexes = {"[,a-zA-Z0-9._-]+"},
      description = "Kafka topic(s) to read input from.",
      helpText = "Kafka topic(s) to read input from.",
      example = "topic1,topic2")
  String getKafkaReadTopics();

  void setKafkaReadTopics(String inputTopics);

  @TemplateParameter.Text(
      order = 3,
      optional = true,
      description = "The Kafka Offset to read from.",
      helpText = "The Kafka Offset to read from.")
  @Default.String("latest")
  String getKafkaReadOffset();

  void setKafkaReadOffset(String value);

  @TemplateParameter.Text(
      order = 4,
      optional = true,
      description =
          "Username to be used with SASL_PLAIN mechanism for reading from Kafka, stored in Google Cloud Secret Manager.",
      helpText =
          "Secret Manager secret ID for the SASL_PLAIN username. Should be in the format projects/{project}/secrets/{secret}/versions/{secret_version}.",
      example = "projects/your-project-id/secrets/your-secret/versions/your-secret-version")
  String getKafkaReadUsernameSecretId();

  void setKafkaReadUsernameSecretId(String value);

  @TemplateParameter.Text(
      order = 5,
      optional = true,
      description =
          "Password to be used with SASL_PLAIN mechanism for reading from Kafka, stored in Google Cloud Secret Manager.",
      helpText =
          "Secret Manager secret ID for the SASL_PLAIN password. Should be in the format projects/{project}/secrets/{secret}/versions/{secret_version}",
      example = "projects/your-project-id/secrets/your-secret/versions/your-secret-version")
  String getKafkaReadPasswordSecretId();

  void setKafkaReadPasswordSecretId(String value);
}
