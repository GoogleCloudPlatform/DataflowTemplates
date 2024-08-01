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
package com.google.cloud.teleport.v2.templates;

import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.v2.templates.options.RabbitMqToPubsubOptions;
import com.google.cloud.teleport.v2.utils.SecretManagerUtils;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.io.rabbitmq.RabbitMqIO;
import org.apache.beam.sdk.io.rabbitmq.RabbitMqMessage;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;

/**
 * Dataflow template which reads data from RabbitMQ queue and writes it to Cloud PubSub.
 *
 * <p>Check out <a
 * href="https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/v2/rabbitmq-to-pubsub/README_RabbitMQ_to_PubSub.md">README</a>
 * for instructions on how to use or modify this template.
 */
@Template(
    name = "RabbitMq_to_PubSub",
    category = TemplateCategory.STREAMING,
    displayName = "RabbitMQ to PubSub",
    description =
        "The RabbitMQ to Pub/Sub template is a streaming pipeline that reads messages from an RabbitMQ queue and writes them to Pub/Sub.",
    optionsClass = RabbitMqToPubsubOptions.class,
    flexContainerName = "rabbitmq-to-pubsub",
    contactInformation = "https://cloud.google.com/support",
    documentation =
        "https://cloud.google.com/dataflow/docs/guides/templates/provided/rabbitmq-to-pubsub",
    preview = true,
    requirements = {
      "The Pub/Sub output topic name must exist.",
      "The RabbitMQ connection URL in plain text or the secret Manager resource path.",
      "The RabbitMQ queue that data is extracted from must have a name."
    },
    streaming = true,
    supportsAtLeastOnce = true)
public class RabbitMqToPubsub {

  /**
   * Runs a pipeline which reads data from RabbitMQ queue and writes it to Cloud PubSub.
   *
   * @param args arguments to the pipeline
   */
  public static void main(String[] args) {
    RabbitMqToPubsubOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(RabbitMqToPubsubOptions.class);
    run(options);
  }

  public static void validate(RabbitMqToPubsubOptions options) {
    if (options != null) {
      if ((options.getConnectionUrl() == null || options.getConnectionUrl().isEmpty())
          || (options.getOutputTopic() == null || options.getOutputTopic().isBlank())
          || (options.getQueue() == null || options.getQueue().isBlank())) {
        throw new IllegalArgumentException(
            "connectionUrl, outputTopic and Queue name is mandatory");
      }
    }
  }

  public static PipelineResult run(RabbitMqToPubsubOptions options) {

    Pipeline pipeline = Pipeline.create(options);
    String connectionUrl = options.getConnectionUrl();
    if (!connectionUrl.startsWith("amqp")) {
      connectionUrl = SecretManagerUtils.getSecret(connectionUrl);
    }
    return pipeline
        .apply(
            "ReadFromRabbitMQ",
            RabbitMqIO.read().withUri(connectionUrl).withQueue(options.getQueue()))
        .apply(ParDo.of(new RabbitMqMessageToPubsubMessage()))
        .apply("WriteToPubSubTopic", PubsubIO.writeMessages().to(options.getOutputTopic()))
        .getPipeline()
        .run();
  }

  static class RabbitMqMessageToPubsubMessage extends DoFn<RabbitMqMessage, PubsubMessage> {
    @ProcessElement
    public void processElement(
        @Element RabbitMqMessage message, OutputReceiver<PubsubMessage> out) {
      String messageString = new String(message.getBody());
      byte[] payload = messageString.getBytes(StandardCharsets.UTF_8);
      // Create attributes for each message if you want/need from Rabbit
      HashMap<String, String> attributes = new HashMap<String, String>();
      PubsubMessage outputMessage = new PubsubMessage(payload, attributes);
      out.output(outputMessage);
    }
  }
}
