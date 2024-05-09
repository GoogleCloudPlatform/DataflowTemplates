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
  @TemplateParameter.Text(
      groupName = "Google Cloud Storage Dead Letter Queue.",
      description = "Dead letter queue directory.",
      optional = true,
      helpText = "This is the file path for Dataflow to write the dead letter queue output.")
  @Default.String("")
  String getDeadLetterQueueDirectory();

  void setDeadLetterQueueDirectory(String value);

  @TemplateParameter.Text(
      groupName = "Kafka Dead Letter Queue",
      description = "Kafka topic for Dead letter queue",
      optional = true,
      helpText =
          "This is the Kafka topic for Dataflow to write the dead letter queue output."
              + "Kafka DLQ can only be enabled if your pipeline has kafka as source or sink. Kafka DLQ"
              + " uses the same bootstrap servers and authentication config as in Kafka Source/Sink.")
  @Default.String("")
  String getDeadLetterQueueKafkaTopic();

  void setDeadLetterQueueKafkaTopic(String value);
}
