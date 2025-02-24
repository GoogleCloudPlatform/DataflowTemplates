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
package com.google.cloud.teleport.v2.templates.options;

import com.google.cloud.teleport.metadata.TemplateParameter;
import com.google.cloud.teleport.v2.options.CommonTemplateOptions;
import org.apache.beam.sdk.options.Validation;

/**
 * The {@link RabbitMqToPubsubOptions} interface provides the custom execution options passed by the
 * executor at the command-line.
 */
public interface RabbitMqToPubsubOptions extends CommonTemplateOptions {

  @TemplateParameter.Text(
      order = 1,
      groupName = "plain text uri or secret resource ID.",
      optional = false,
      description =
          "RabbitMQ connection URL string (amqp://username:password@ampq_server:5671) or secret resource ID (projects/{project}/secrets/{secret}/versions/{secret_version})",
      helpText = "The Uri connection string for RabbitMQ or the secret resource ID.",
      example =
          "amqp://username:password@ampq_server:5671 or projects/{project}/secrets/{secret}/versions/{secret_version}")
  String getConnectionUrl();

  void setConnectionUrl(String connectionUrl);

  @TemplateParameter.Text(
      order = 2,
      optional = false,
      regexes = {"^.+$"},
      description = "Queue",
      helpText = "RabbitMQ Queue to read from and publish to pubsub",
      example = "queueName")
  String getQueue();

  void setQueue(String queue);

  @TemplateParameter.PubsubTopic(
      order = 3,
      optional = false,
      groupName = "Target",
      description = "Output Pub/Sub topic",
      helpText =
          "The Pub/Sub topic to publish to, in the format projects/<PROJECT_ID>/topics/<TOPIC_NAME>.",
      example = "projects/your-project-id/topics/your-topic-name")
  @Validation.Required
  String getOutputTopic();

  void setOutputTopic(String outputTopic);
}
