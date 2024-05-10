/*
 * Copyright (C) 2021 Google LLC
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
package com.google.cloud.teleport.v2.elasticsearch.options;

import com.google.cloud.teleport.metadata.TemplateParameter;
import com.google.cloud.teleport.v2.elasticsearch.utils.Dataset;
import com.google.cloud.teleport.v2.transforms.JavascriptTextTransformer;
import com.google.cloud.teleport.v2.transforms.PythonExternalTextTransformer;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Validation;

/**
 * The {@link PubSubToElasticsearchOptions} class provides the custom execution options passed by
 * the executor at the command-line.
 *
 * <p>Inherits standard configuration options, options from {@link
 * JavascriptTextTransformer.JavascriptTextTransformerOptions}, and options from {@link
 * ElasticsearchWriteOptions}.
 */
public interface PubSubToElasticsearchOptions
    extends PythonExternalTextTransformer.PythonExternalTextTransformerOptions,
        ElasticsearchWriteOptions {

  @TemplateParameter.PubsubSubscription(
      order = 1,
      groupName = "Source",
      description = "Pub/Sub input subscription",
      helpText =
          "Pub/Sub subscription to consume the input from. Name should be in the format of 'projects/your-project-id/subscriptions/your-subscription-name'",
      example = "projects/your-project-id/subscriptions/your-subscription-name")
  @Validation.Required
  String getInputSubscription();

  void setInputSubscription(String inputSubscription);

  @TemplateParameter.Text(
      order = 2,
      groupName = "Source",
      optional = true,
      description = "Dataset, the type of logs that are sent to Pub/Sub",
      helpText =
          "The type of logs sent using Pub/Sub, for which we have an out-of-the-box dashboard. Known "
              + "log types values are audit, vpcflow and firewall. Default 'pubsub'")
  @Default.Enum("PUBSUB")
  Dataset getDataset();

  void setDataset(Dataset dataset);

  @TemplateParameter.Text(
      order = 3,
      optional = true,
      groupName = "Source",
      description = "The namespace for dataset.",
      helpText =
          "An arbitrary grouping, such as an environment (dev, prod, or qa), a team, or a strategic business unit. Default: 'default'")
  @Default.String("default")
  String getNamespace();

  void setNamespace(String namespace);

  @TemplateParameter.PubsubTopic(
      order = 4,
      groupName = "Target",
      description = "Output deadletter Pub/Sub topic",
      helpText =
          "Pub/Sub output topic for publishing failed records in the format of 'projects/your-project-id/topics/your-topic-name'.")
  @Validation.Required
  String getErrorOutputTopic();

  void setErrorOutputTopic(String errorOutputTopic);

  @TemplateParameter.Text(
      order = 5,
      groupName = "Target",
      optional = true,
      description = "Template Version.",
      helpText = "Dataflow Template Version Identifier, usually defined by Google Cloud.")
  @Default.String("1.0.0")
  String getElasticsearchTemplateVersion();

  void setElasticsearchTemplateVersion(String elasticsearchTemplateVersion);
}
