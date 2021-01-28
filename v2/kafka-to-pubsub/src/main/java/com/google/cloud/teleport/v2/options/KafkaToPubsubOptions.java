/*
 * Copyright (C) 2020 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.v2.options;

import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;

/**
 * The {@link KafkaToPubsubOptions} interface provides the custom execution options passed by the
 * executor at the command-line.
 */
public interface KafkaToPubsubOptions extends PipelineOptions {
  @Description(
      "Comma Separated list of Kafka Bootstrap Servers (e.g: server1:[port],server2:[port]).")
  @Validation.Required
  String getBootstrapServers();

  void setBootstrapServers(String bootstrapServers);

  @Description(
      "Comma Separated list of Kafka topic(s) to read the input from (e.g: topic1,topic2).")
  @Validation.Required
  String getInputTopics();

  void setInputTopics(String inputTopics);

  @Description(
      "The Cloud Pub/Sub topic to publish to. "
          + "The name should be in the format of "
          + "projects/<project-id>/topics/<topic-name>.")
  @Validation.Required
  String getOutputTopic();

  void setOutputTopic(String outputTopic);

  @Description("Gcs path to javascript udf source")
  String getJavascriptTextTransformGcsPath();

  void setJavascriptTextTransformGcsPath(String javascriptTextTransformGcsPath);

  @Description("UDF Javascript Function Name")
  String getJavascriptTextTransformFunctionName();

  void setJavascriptTextTransformFunctionName(String javascriptTextTransformFunctionName);

  @Description(
      "The dead-letter topic to receive failed messages. "
          + "The name should be in the format of projects/<project-id>/topics/<topic-name>.")
  String getOutputDeadLetterTopic();

  void setOutputDeadLetterTopic(String outputDeadLetterTable);

  @Description("URL to credentials in Vault")
  String getSecretStoreUrl();

  void setSecretStoreUrl(String secretStoreUrl);

  @Description("Vault token")
  String getVaultToken();

  void setVaultToken(String vaultToken);

  @Description("Gcs path to JSON file with Kafka options")
  String getKafkaOptionsGcsPath();

  void setKafkaOptionsGcsPath(String kafkaOptionsGcsPath);
}
