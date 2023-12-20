/*
 * Copyright (C) 2023 Google LLC
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
package com.google.cloud.teleport.templates.python;

import com.google.cloud.teleport.metadata.TemplateParameter;

/** Placeholder template class for WordCount in Python. */
// TODO(pranavbhandari): Re-enable template annotations for python templates after permission issues
// are fixed.
/*@Template(
name = "Streaming_LLM",
category = TemplateCategory.STREAMING,
type = TemplateType.PYTHON,
displayName = "Streaming LLM",
description = "Execute Large Language Models in Streaming mode using Pub/Sub.",
flexContainerName = "streaming-llm",
contactInformation = "https://cloud.google.com/support",
hidden = true,
streaming = true)*/
public interface StreamingLLM {

  @TemplateParameter.PubsubSubscription(
      order = 1,
      name = "input_subscription",
      description = "Pub/Sub input subscription",
      helpText =
          "Pub/Sub subscription to read the input from, in the format of 'projects/your-project-id/subscriptions/your-subscription-name'",
      example = "projects/your-project-id/subscriptions/your-subscription-name")
  String getInputSubscription();

  @TemplateParameter.PubsubTopic(
      order = 2,
      name = "output_topic",
      description = "Output Pub/Sub topic",
      helpText =
          "The name of the topic to which data should published, in the format of 'projects/your-project-id/topics/your-topic-name'",
      example = "projects/your-project-id/topics/your-topic-name")
  String getOutputTopic();

  @TemplateParameter.Text(
      order = 3,
      name = "model_name",
      description = "Model Name",
      helpText = "The name of the model to use",
      example = "google/flan-t5-large")
  String getModelName();

  @TemplateParameter.Text(
      order = 4,
      name = "device",
      description = "Device",
      helpText = "Device to use (CPU / GPU)",
      example = "CPU")
  String getDevice();
}
