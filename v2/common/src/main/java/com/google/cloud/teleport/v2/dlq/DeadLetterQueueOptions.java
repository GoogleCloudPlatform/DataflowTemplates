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
package com.google.cloud.teleport.v2.dlq;

import com.google.cloud.teleport.metadata.TemplateParameter;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptions;

public interface DeadLetterQueueOptions extends PipelineOptions {
  // TODO: These options are visible on top of the Required Parameters.
  // Figure out ordering of the parameters on the UI.

  public static final String GCS_DLQ_GROUP_NAME = "Google Cloud Storage Dead Letter Queue";
  public static final String KAFKA_DLQ_GROUP_NAME = "Kafka Dead Letter Queue";

  @TemplateParameter.Boolean(
      order = 1,
      optional = true,
      description = "Enable Google Cloud Storage Dead Letter Queue.",
      groupName = GCS_DLQ_GROUP_NAME,
      helpText =
          "Enable Google Cloud Storage Dead Letter Queue. This will write the failed records to ")
  @Default.Boolean(false)
  Boolean getEnableGcsDlq();

  void setEnableGcsDlq(boolean value);

  @TemplateParameter.Text(
      order = 2,
      groupName = GCS_DLQ_GROUP_NAME,
      description = "Dead letter queue directory.",
      parentName = "enableGcsDlq",
      parentTriggerValues = {"true"},
      helpText = "This is the file path for Dataflow to write the dead letter queue output.")
  @Default.String("")
  String getDeadLetterQueueDirectory();

  void setDeadLetterQueueDirectory(String value);

  @TemplateParameter.Boolean(
      order = 3,
      description = "Enable Kafka Dead Letter Queue.",
      groupName = KAFKA_DLQ_GROUP_NAME,
      optional = true,
      helpText =
          "Enable Kafka Dead Letter Queue. The pipeline must have Kafka as Source or Sink.")
  @Default.Boolean(false)
  Boolean getEnableKafkaDlq();

  void setEnableKafkaDlq(boolean value);
  @TemplateParameter.Text(
          order = 4,
          groupName = KAFKA_DLQ_GROUP_NAME,
          description = "Kafka dead letter queue topic",
          parentName = "enableKafkaDlq",
          parentTriggerValues = {"true"},
          helpText = "Kafka topic for Dataflow to write dead letter queue "
                  + "output. Requires Kafka as source or sink. Uses the same "
                  + "bootstrap servers and authentication as Kafka Source/Sink."
  )
  @Default.String("")
  String getDeadLetterQueueKafkaTopic();
  void setDeadLetterQueueKafkaTopic(String value);
}
