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
package com.google.cloud.teleport.v2.options;

import com.google.cloud.teleport.metadata.TemplateParameter;
import com.google.cloud.teleport.v2.kafka.options.KafkaCommonOptions;
import com.google.cloud.teleport.v2.transforms.JavascriptTextTransformer.JavascriptTextTransformerOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;

/**
 * The {@link KafkaToPubsubOptions} interface provides the custom execution options passed by the
 * executor at the command-line.
 */
public interface KafkaToPubsubOptions
    extends PipelineOptions, KafkaCommonOptions, JavascriptTextTransformerOptions {
  @TemplateParameter.Text(
      order = 1,
      groupName = "Source",
      optional = true,
      regexes = {"[,:a-zA-Z0-9._-]+"},
      description = "Kafka Bootstrap Server list",
      helpText = "Kafka Bootstrap Server list, separated by commas.",
      example = "localhost:9092,127.0.0.1:9093")
  @Validation.Required
  String getBootstrapServers();

  void setBootstrapServers(String bootstrapServers);

  @TemplateParameter.Text(
      order = 2,
      groupName = "Source",
      optional = false,
      regexes = {"[a-zA-Z0-9._-]+"},
      description = "Kafka topic(s) to read the input from",
      helpText = "Kafka topic(s) to read the input from.",
      example = "topic1,topic2")
  @Validation.Required
  String getInputTopics();

  void setInputTopics(String inputTopics);

  @TemplateParameter.PubsubTopic(
      order = 3,
      groupName = "Target",
      description = "Output Pub/Sub topic",
      helpText =
          "The name of the topic to which data should published, in the format of 'projects/your-project-id/topics/your-topic-name'",
      example = "projects/your-project-id/topics/your-topic-name")
  @Validation.Required
  String getOutputTopic();

  void setOutputTopic(String outputTopic);

  @TemplateParameter.PubsubTopic(
      order = 4,
      groupName = "Target",
      description = "Output deadletter Pub/Sub topic",
      helpText =
          "The Pub/Sub topic to publish deadletter records to. The name should be in the "
              + "format of projects/your-project-id/topics/your-topic-name.")
  String getOutputDeadLetterTopic();

  void setOutputDeadLetterTopic(String outputDeadLetterTable);
}
