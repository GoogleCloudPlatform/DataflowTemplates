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
package com.google.cloud.teleport.v2.options;

import com.google.cloud.teleport.v2.elasticsearch.options.ElasticsearchWriteOptions;
import com.google.cloud.teleport.v2.elasticsearch.utils.Dataset;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;

/**
 * The {@link KafkaToElasticsearchOptions} interface provides the custom execution options passed by the
 * executor at the command-line.
 */
public interface KafkaToElasticsearchOptions extends ElasticsearchWriteOptions, PipelineOptions {
  @Description(
          "Kafka topic to read from.")
  @Validation.Required
  String getInputTopic();

  void setInputTopic(String inputTopic);

  @Description(
      "Comma Separated list of Kafka Bootstrap Servers (e.g: server1:[port],server2:[port]).")
  @Validation.Required
  String getBootstrapServers();

  void setBootstrapServers(String bootstrapServers);

  @Description(
          "API key for access without requiring basic authentication. "
                  + "Refer  https://www.elastic.co/guide/en/elasticsearch/reference/current/security-api-create-api-key.html#security-api-create-api-key-request")
  @Validation.Required
  String getApiKey();

  void setApiKey(String apiKey);

  @Description(
          "Elasticsearch URL in format http://hostname:[port] or Base64 encoded CloudId")
  @Validation.Required
  String getConnectionUrl();

  void setConnectionUrl(String connectionUrl);

  @Description("The topic to output errors within Pub/Sub.")
  @Validation.Required
  String getErrorOutputTopic();

  void setErrorOutputTopic(String errorOutputTopic);

  @Description("URL to credentials in Vault")
  String getSecretStoreUrl();

  void setSecretStoreUrl(String secretStoreUrl);

  @Description("Kafka SSL endpoint identification algorithm, default is HTTPS")
  @Default.String("https")
  String getKafkaSslEndpointIdentificationAlgorithm();

  void setKafkaSslEndpointIdentificationAlgorithm(String kafkaSslEndpointIdentificationAlgorithm);

  @Description("Kafka SASL mechanism, default is PLAIN")
  @Default.String("PLAIN")
  String getKafkaSaslMechanism();

  void setKafkaSaslMechanism(String kafkaSaslMechanism);

  @Description("Kafka request timeout, default is 20000")
  @Default.Integer(20000)
  Integer getKafkaRequestTimeout();

  void setKafkaRequestTimeout(Integer kafkaRequestTimeout);

  @Description("Kafka retry backoff delay, default is 500")
  @Default.Integer(500)
  Integer getKafkaRetryBackoffDelay();

  void setKafkaRetryBackoffDelay(Integer kafkaRetryBackoffDelay);

  @Description("Kafka security protocol, default is SASL_SSL")
  @Default.String("SASL_SSL")
  String getKafkaSecurityProtocol();

  void setKafkaSecurityProtocol(String kafkaSecurityProtocol);

  @Description("Kafka username")
  String getKafkaUsername();

  void setKafkaUsername(String kafkaUsername);

  @Description("Kafka password")
  String getKafkaPassword();

  void setKafkaPassword(String kafkaPassword);

  @Description("Vault token")
  String getVaultToken();

  void setVaultToken(String vaultToken);

  @Description(
          "The type of logs sent via Pub/Sub for which we have out of the box dashboard. "
                  + "Known log types values are audit, vpcflow, and firewall. "
                  + "If no known log type is detected, we default to pubsub")
  @Default.Enum("PUBSUB")
  Dataset getDataset();

  void setDataset(Dataset dataset);

  @Description(
          "The namespace for dataset. Default is default")
  @Default.String("default")
  String getNamespace();

  void setNamespace(String namespace);

  @Description(
          "Username for Elasticsearch endpoint. Overrides ApiKey option if specified.")
  String getElasticsearchUsername();

  void setElasticsearchUsername(String elasticsearchUsername);

  @Description(
          "Password for Elasticsearch endpoint. Overrides ApiKey option if specified.")
  String getElasticsearchPassword();

  void setElasticsearchPassword(String elasticsearchPassword);

  @Description("Dataflow template version, should be set by GCP.")
  @Default.String("1.0.0")
  String getElasticsearchTemplateVersion();

  void setElasticsearchTemplateVersion(String elasticsearchTemplateVersion);
}
