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

import static com.google.cloud.teleport.v2.templates.PubsubToJms.PubsubToJmsOptions;

import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.metadata.TemplateParameter;
import javax.jms.ConnectionFactory;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.jms.JmsIO;
import org.apache.beam.sdk.io.jms.TextMessageMapper;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Dataflow template which reads data from a Pubsub Subscription and writes to JMS Broker
 * Server(Topic/Queue).
 *
 * <p>Check out <a
 * href="https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/v2/pubsub-to-jms/README_PubSub_to_Jms.md">README</a>
 * for instructions on how to use or modify this template.
 */
@Template(
    name = "Pubsub_to_Jms",
    category = TemplateCategory.STREAMING,
    displayName = "Pubsub to JMS",
    description =
        "A streaming pipeline which inserts data "
            + "from a Pubsub Subscription and writes to JMS Broker Server Topic or Queue.",
    optionsClass = PubsubToJmsOptions.class,
    flexContainerName = "pubsub-to-jms",
    contactInformation = "https://cloud.google.com/support",
    hidden = true)
public class PubsubToJms {

  /**
   * Runs a pipeline which reads data from a Pubsub and writes to JMS Broker Server(Topic/Queue).
   *
   * @param args arguments to the pipeline
   */
  private static final Logger LOG = LoggerFactory.getLogger(PubsubToJms.class);

  public static void main(String[] args) {
    PubsubToJmsOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(PubsubToJmsOptions.class);
    run(options);
  }

  public static void validate(PubsubToJmsOptions options) {
    if (options != null) {
      if ((options.getUsername() != null
              && (!options.getUsername().isEmpty() || !options.getUsername().isBlank()))
          && (options.getPassword() == null
              || options.getPassword().isBlank()
              || options.getPassword().isEmpty())) {
        throw new IllegalArgumentException(
            "While username is provided, password is required for authentication");
      }
    }
  }

  public static PipelineResult run(PubsubToJmsOptions options) {
    validate(options);
    Pipeline pipeline = Pipeline.create(options);

    String connectionURI = options.getJmsServer();
    ConnectionFactory myConnectionFactory;

    if (!options.getUsername().isEmpty() || !options.getUsername().isBlank()) {
      myConnectionFactory =
          new ActiveMQConnectionFactory(
              options.getUsername(), options.getPassword(), connectionURI);
    } else {
      myConnectionFactory = new ActiveMQConnectionFactory(connectionURI);
    }

    LOG.info("Given Output Type " + options.getOutputType());
    PCollection<String> input =
        pipeline.apply(
            "Read PubSub Messages",
            PubsubIO.readStrings().fromSubscription(options.getInputSubscription()));
    if (options.getOutputType().equalsIgnoreCase("queue")) {
      input.apply(
          "Write to JMS Queue",
          JmsIO.<String>write()
              .withConnectionFactory(myConnectionFactory)
              .withQueue(options.getOutputName())
              .withValueMapper(new TextMessageMapper()));
    } else {
      input.apply(
          "Write to JMS Topic",
          JmsIO.<String>write()
              .withConnectionFactory(myConnectionFactory)
              .withTopic(options.getOutputName())
              .withValueMapper(new TextMessageMapper()));
    }

    return pipeline.run();
  }

  /**
   * The {@link PubsubToJmsOptions} interface provides the custom execution options passed by the
   * executor at the command-line.
   */
  public interface PubsubToJmsOptions extends PipelineOptions {
    @TemplateParameter.PubsubSubscription(
        order = 1,
        description = "Pub/Sub input subscription",
        helpText =
            "Pub/Sub subscription to read the input from, in the format of"
                + " 'projects/your-project-id/subscriptions/your-subscription-name'",
        example = "projects/your-project-id/subscriptions/your-subscription-name")
    @Validation.Required
    String getInputSubscription();

    void setInputSubscription(String inputSubscription);

    @TemplateParameter.Text(
        order = 2,
        optional = true,
        regexes = {"[,\\/:a-zA-Z0-9._-]+"},
        description = "JMS Host IP",
        helpText = "Server IP for JMS Host",
        example = "host:5672")
    @Validation.Required
    String getJmsServer();

    void setJmsServer(String jmsServer);

    @TemplateParameter.Text(
        order = 3,
        optional = false,
        regexes = {"[a-zA-Z0-9._-]+"},
        description = "JMS Queue/Topic Name to write the input to",
        helpText = "JMS Queue/Topic Name to write the input to.",
        example = "queue")
    @Validation.Required
    String getOutputName();

    void setOutputName(String outputName);

    @TemplateParameter.Text(
        order = 4,
        optional = false,
        regexes = {"[a-zA-Z0-9._-]+"},
        description = "JMS Destination Type to Write the input to",
        helpText = "JMS Destination Type to Write the input to.",
        example = "queue")
    @Validation.Required
    String getOutputType();

    void setOutputType(String outputType);

    @TemplateParameter.Text(
        order = 5,
        description = "JMS Username",
        helpText = "JMS username for authentication with JMS server",
        example = "sampleusername")
    String getUsername();

    void setUsername(String username);

    @TemplateParameter.Text(
        order = 6,
        description = "JMS Password",
        helpText = "Password for username provided for authentication with JMS server",
        example = "samplepassword")
    String getPassword();

    void setPassword(String password);
  }
}
