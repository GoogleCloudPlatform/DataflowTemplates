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
import com.google.cloud.teleport.metadata.TemplateParameter;
import javax.jms.ConnectionFactory;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.jms.JmsIO;
import org.apache.beam.sdk.io.jms.JmsRecord;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Dataflow template which reads data from Jms Queue/Topic and writes it to Cloud PubSub.
 *
 * <p>Check out <a
 * href="https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/v2/jms-to-pubsub/README_Jms_to_PubSub.md">README</a>
 * for instructions on how to use or modify this template.
 */
@Template(
    name = "Jms_to_PubSub",
    category = TemplateCategory.STREAMING,
    displayName = "JMS to Pubsub",
    description =
        "The JMS to Pub/Sub template is a streaming pipeline that reads messages from ActiveMQ JMS Server (Queue/Topic) and writes them to Pub/Sub.",
    optionsClass = com.google.cloud.teleport.v2.templates.JmsToPubsub.JmsToPubsubOptions.class,
    flexContainerName = "jms-to-pubsub",
    contactInformation = "https://cloud.google.com/support",
    documentation =
        "https://cloud.google.com/dataflow/docs/guides/templates/provided/jms-to-pubsub",
    preview = true,
    requirements = {
      "The Pub/Sub output topic name must exist.",
      "The JMS host IP must exist and have the proper network configuration for Dataflow worker VMs to reach the JMS host.",
      "The JMS topic/queue that data is extracted from must have a name."
    },
    streaming = true,
    supportsAtLeastOnce = true)
public class JmsToPubsub {

  /**
   * Runs a pipeline which reads data from JMS queue/topic and writes it to Cloud PubSub.
   *
   * @param args arguments to the pipeline
   */
  private static final Logger LOG = LoggerFactory.getLogger(JmsToPubsub.class);

  public static void main(String[] args) {
    JmsToPubsubOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(JmsToPubsubOptions.class);
    run(options);
  }

  public static void validate(JmsToPubsubOptions options) {
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

  public static PipelineResult run(JmsToPubsubOptions options) {
    validate(options);
    Pipeline pipeline = Pipeline.create(options);
    String connectionURI = options.getJmsServer();
    ConnectionFactory myConnectionFactory;
    PCollection<JmsRecord> input;

    if (!options.getUsername().isEmpty() || !options.getUsername().isBlank()) {
      myConnectionFactory =
          new ActiveMQConnectionFactory(
              options.getUsername(), options.getPassword(), connectionURI);
    } else {
      myConnectionFactory = new ActiveMQConnectionFactory(connectionURI);
    }
    LOG.info("Given Input Type " + options.getInputType());
    if (options.getInputType().equalsIgnoreCase("queue")) {
      input =
          pipeline.apply(
              "Read From JMS Queue",
              JmsIO.read()
                  .withConnectionFactory(myConnectionFactory)
                  .withQueue(options.getInputName()));
    } else {
      input =
          pipeline.apply(
              "Read From JMS Topic",
              JmsIO.read()
                  .withConnectionFactory(myConnectionFactory)
                  .withTopic(options.getInputName()));
    }

    input
        .apply(
            MapElements.via(
                new SimpleFunction<JmsRecord, String>() {
                  public String apply(JmsRecord input) {
                    return input.getPayload();
                  }
                }))
        .apply("WriteToPubSubTopic", PubsubIO.writeStrings().to(options.getOutputTopic()));
    return pipeline.run();
  }

  /**
   * The {@link JmsToPubsubOptions} interface provides the custom execution options passed by the
   * executor at the command-line.
   */
  public interface JmsToPubsubOptions extends PipelineOptions {
    @TemplateParameter.Text(
        order = 1,
        groupName = "Source",
        optional = true,
        regexes = {"[,\\/:a-zA-Z0-9._-]+"},
        description = "JMS Host IP",
        helpText = "Server IP for JMS Host",
        example = "host:5672")
    @Validation.Required
    String getJmsServer();

    void setJmsServer(String jmsServer);

    @TemplateParameter.Text(
        order = 2,
        groupName = "Source",
        optional = false,
        regexes = {"[a-zA-Z0-9._-]+"},
        description = "JMS Queue/Topic Name to read the input from",
        helpText = "JMS Queue/Topic Name to read the input from.",
        example = "queue")
    @Validation.Required
    String getInputName();

    void setInputName(String inputName);

    @TemplateParameter.Text(
        order = 3,
        groupName = "Source",
        optional = false,
        regexes = {"[a-zA-Z0-9._-]+"},
        description = "JMS Destination Type to read the input from",
        helpText = "JMS Destination Type to read the input from.",
        example = "queue")
    @Validation.Required
    String getInputType();

    void setInputType(String inputType);

    @TemplateParameter.Text(
        order = 4,
        groupName = "Target",
        description = "Output Pub/Sub topic",
        helpText =
            "The name of the topic to which data should published, in the format of"
                + " 'projects/your-project-id/topics/your-topic-name'",
        example = "projects/your-project-id/topics/your-topic-name")
    @Validation.Required
    String getOutputTopic();

    void setOutputTopic(String outputTopic);

    @TemplateParameter.Text(
        order = 5,
        groupName = "Source",
        description = "JMS Username",
        helpText = "JMS username for authentication with JMS server",
        example = "sampleusername")
    String getUsername();

    void setUsername(String username);

    @TemplateParameter.Text(
        order = 6,
        groupName = "Source",
        description = "JMS Password",
        helpText = "Password for username provided for authentication with JMS server",
        example = "samplepassword")
    String getPassword();

    void setPassword(String password);
  }
}
