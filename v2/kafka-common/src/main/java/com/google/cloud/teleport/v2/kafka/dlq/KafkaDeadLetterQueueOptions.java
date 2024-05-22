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
package com.google.cloud.teleport.v2.kafka.dlq;

import com.google.cloud.teleport.metadata.TemplateParameter;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptions;

/**
 * {@link KafkaDeadLetterQueueOptions} is used for any Dead letter queue sinks for the failed
 * records.
 */
public interface KafkaDeadLetterQueueOptions extends PipelineOptions {
  String KAFKA_DLQ_GROUP_NAME = "Dead Letter Queue";

  @TemplateParameter.Boolean(
      description = "Enable Kafka Dead Letter Queue.",
      groupName = KAFKA_DLQ_GROUP_NAME,
      helpText = "Enable Kafka Dead Letter Queue. The pipeline must have Kafka as Source or Sink.")
  @Default.Boolean(false)
  Boolean getEnableKafkaDlq();

  void setEnableKafkaDlq(Boolean value);

  @TemplateParameter.Text(
      groupName = KAFKA_DLQ_GROUP_NAME,
      description = "Kafka dead letter queue topic",
      parentName = "enableKafkaDlq",
      parentTriggerValues = {"true"},
      helpText =
          "Kafka topic for Dataflow to write dead letter queue "
              + "output. Requires Kafka as source or sink. Uses the same "
              + "bootstrap servers and authentication as Kafka Source/Sink.")
  @Default.String("")
  String getDeadLetterQueueKafkaTopic();

  void setDeadLetterQueueKafkaTopic(String value);
}
