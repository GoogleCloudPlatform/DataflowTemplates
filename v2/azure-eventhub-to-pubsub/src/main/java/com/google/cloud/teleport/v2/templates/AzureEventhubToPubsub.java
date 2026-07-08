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
import com.google.cloud.teleport.v2.utils.SecretManagerUtils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Dataflow template which reads data from Azure Event-hub Topic and writes it to Cloud PubSub.
 *
 * <p>Check out <a
 * href="https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/v2/azure-eventhub-to-pubsub/README_Azure_Eventhub_to_PubSub.md">README</a>
 * for instructions on how to use or modify this template.
 */
@Template(
    name = "Azure_Eventhub_to_PubSub",
    category = TemplateCategory.STREAMING,
    displayName = "Azure Eventhub to Pubsub",
    description = "A pipeline to extract from Azure Event hub Server to Cloud Pub/Sub Topic.",
    optionsClass = AzureEventhubToPubsub.AzureEventhubToPubsubOptions.class,
    flexContainerName = "azure-eventhub-to-pubsub",
    contactInformation = "https://cloud.google.com/support",
    documentation =
        "https://cloud.google.com/dataflow/docs/guides/templates/provided/azure-eventhub-to-pubsub",
    streaming = true,
    preview = true,
    hidden = true)
public class AzureEventhubToPubsub {

  private static final Logger LOG = LoggerFactory.getLogger(AzureEventhubToPubsub.class);
  static Map<String, Object> config = new HashMap<>();
  static String saslMechanism = "PLAIN";
  static String securityProtocol = "SASL_SSL";

  public static void main(String[] args) throws IOException {
    AzureEventhubToPubsubOptions options =
        PipelineOptionsFactory.fromArgs(args)
            .withValidation()
            .as(AzureEventhubToPubsubOptions.class);

    String password = SecretManagerUtils.getSecret(options.getSecret());
    String eahl =
        String.format(
            "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$ConnectionString\" password=\"%s\";",
            password);
    config.put(SaslConfigs.SASL_MECHANISM, saslMechanism);
    config.put(SaslConfigs.SASL_JAAS_CONFIG, eahl);
    config.put("security.protocol", securityProtocol);
    run(options);
  }

  public static PipelineResult run(AzureEventhubToPubsubOptions options) {
    Pipeline pipeline = Pipeline.create(options);
    ArrayList<String> topicsList =
        new ArrayList<>(Arrays.asList(options.getInputTopic().split(",")));
    KafkaIO.Read<String, String> kafkaRecords =
        KafkaIO.<String, String>read()
            .withBootstrapServers(options.getBrokerServer())
            .withTopics(topicsList)
            .withConsumerConfigUpdates(config)
            .withKeyDeserializerAndCoder(
                StringDeserializer.class, NullableCoder.of(StringUtf8Coder.of()))
            .withValueDeserializerAndCoder(
                StringDeserializer.class, NullableCoder.of(StringUtf8Coder.of()));
    PCollection<KV<String, String>> pCollection =
        pipeline.apply("Read Azure Eventhub Topic", kafkaRecords.withoutMetadata());
    return pCollection
        .apply("Extract Value", ParDo.of(new KafkaRecordToString()))
        .apply("Write To Pub/Sub", PubsubIO.writeStrings().to(options.getOutputTopic()))
        .getPipeline()
        .run();
  }

  static class KafkaRecordToString extends DoFn<KV<String, String>, String> {
    @ProcessElement
    public void processElement(@Element KV<String, String> record, OutputReceiver<String> out) {
      out.output(record.getValue());
    }
  }

  public interface AzureEventhubToPubsubOptions extends PipelineOptions {
    @TemplateParameter.Text(
        order = 1,
        optional = false,
        regexes = {"[,\\/:a-zA-Z0-9._-]+"},
        groupName = "Source",
        description = "Azure Event Hub endpoint",
        helpText = "Server IP or DNS for Azure Eventhub Endpoint",
        example = "mynamespace.servicebus.windows.net:9093")
    @Validation.Required
    String getBrokerServer();

    void setBrokerServer(String brokerServer);

    @TemplateParameter.Text(
        order = 2,
        optional = false,
        regexes = {"[a-zA-Z0-9._-]+"},
        groupName = "Source",
        description = "Azure Eventhub topic(s) to read the input from",
        helpText = "Azure Eventhub topic(s) to read the input from",
        example = "topic")
    @Validation.Required
    String getInputTopic();

    void setInputTopic(String inputTopics);

    @TemplateParameter.PubsubTopic(
        order = 3,
        description = "Output Pub/Sub topic",
        groupName = "Target",
        helpText =
            "The name of the topic to which data should published, in the format of 'projects/your-project-id/topics/your-topic-name'",
        example = "projects/your-project-id/topics/your-topic-name")
    @Validation.Required
    String getOutputTopic();

    void setOutputTopic(String outputTopic);

    @TemplateParameter.Text(
        order = 6,
        description = "Secret Version",
        helpText = "Secret Version, it can be a number like 1,2 or 3 or can be 'latest'",
        example = "projects/{project}/secrets/{secret}/versions/{secret_version}")
    @Validation.Required
    String getSecret();

    void setSecret(String version);
  }
}
