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
import java.nio.charset.StandardCharsets;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.mqtt.MqttIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;

/**
 * Dataflow template which reads data from Mqtt Topic and writes it to Cloud PubSub.
 *
 * <p>Check out <a
 * href="https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/v2/mqtt-to-pubsub/README_Mqtt_to_PubSub.md">README</a>
 * for instructions on how to use or modify this template.
 */
@Template(
    name = "Mqtt_to_PubSub",
    category = TemplateCategory.STREAMING,
    displayName = "Mqtt to Pubsub",
    description = "A pipeline to extract from Mqtt Broker Server to Pubsub Topic.",
    optionsClass = MqttToPubsub.MqttToPubsubOptions.class,
    flexContainerName = "mqtt-to-pubsub",
    contactInformation = "https://cloud.google.com/support")
public class MqttToPubsub {

  /**
   * Runs a pipeline which reads data from Mqtt topic and writes it to Cloud PubSub.
   *
   * @param args arguments to the pipeline
   */
  public static void main(String[] args) {
    MqttToPubsubOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(MqttToPubsubOptions.class);
    run(options);
  }

  public static void validate(MqttToPubsubOptions options) {
    if (options != null) {
      if ((options.getUsername() != null
              && (!options.getUsername().isEmpty() || !options.getUsername().isBlank()))
          && (options.getPassword() != null
              || options.getPassword().isBlank()
              || options.getPassword().isEmpty())) {
        throw new IllegalArgumentException(
            "While username is provided, password is required for authentication");
      }
    }
  }

  public static PipelineResult run(MqttToPubsubOptions options) {
    validate(options);
    Pipeline pipeline = Pipeline.create(options);
    MqttIO.Read mqttIo;
    if (!options.getUsername().isEmpty() || !options.getPassword().isBlank()) {
      mqttIo =
          MqttIO.read()
              .withConnectionConfiguration(
                  MqttIO.ConnectionConfiguration.create(
                          options.getBrokerServer(), options.getInputTopic())
                      .withUsername(options.getUsername())
                      .withPassword(options.getPassword()));
    } else {
      mqttIo =
          MqttIO.read()
              .withConnectionConfiguration(
                  MqttIO.ConnectionConfiguration.create(
                      options.getBrokerServer(), options.getInputTopic()));
    }

    return pipeline
        .apply("ReadFromMqttTopic", mqttIo)
        .apply(ParDo.of(new ByteToStringTransform()))
        .apply("WriteToPubSubTopic", PubsubIO.writeStrings().to(options.getOutputTopic()))
        .getPipeline()
        .run();
  }

  static class ByteToStringTransform extends DoFn<byte[], String> {
    @ProcessElement
    public void processElement(@Element byte[] word, OutputReceiver<String> out) {
      out.output(new String(word, StandardCharsets.UTF_8));
    }
  }

  /**
   * The {@link MqttToPubsubOptions} interface provides the custom execution options passed by the
   * executor at the command-line.
   */
  public interface MqttToPubsubOptions extends PipelineOptions {
    @TemplateParameter.Text(
        order = 1,
        optional = true,
        regexes = {"[,\\/:a-zA-Z0-9._-]+"},
        description = "MQTT Broker IP",
        helpText = "Server IP for MQTT broker",
        example = "tcp://host:1883")
    @Validation.Required
    String getBrokerServer();

    void setBrokerServer(String brokerServer);

    @TemplateParameter.Text(
        order = 2,
        optional = false,
        regexes = {"[a-zA-Z0-9._-]+"},
        description = "MQTT topic(s) to read the input from",
        helpText = "MQTT topic(s) to read the input from.",
        example = "topic")
    @Validation.Required
    String getInputTopic();

    void setInputTopic(String inputTopics);

    @TemplateParameter.PubsubTopic(
        order = 3,
        description = "Output Pub/Sub topic",
        helpText =
            "The name of the topic to which data should published, in the format of 'projects/your-project-id/topics/your-topic-name'",
        example = "projects/your-project-id/topics/your-topic-name")
    @Validation.Required
    String getOutputTopic();

    void setOutputTopic(String outputTopic);

    @TemplateParameter.Text(
        order = 4,
        description = "MQTT Username",
        helpText = "MQTT username for authentication with MQTT server",
        example = "sampleusername")
    String getUsername();

    void setUsername(String username);

    @TemplateParameter.Password(
        order = 5,
        description = "MQTT Password",
        helpText = "Password for username provided for authentication with MQTT server",
        example = "samplepassword")
    String getPassword();

    void setPassword(String password);
  }
}
