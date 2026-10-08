/*
 * Copyright (C) 2026 Google LLC
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
package com.google.cloud.teleport.templates.yaml;

import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.metadata.TemplateParameter;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Validation;

@Template(
    name = "Kafka_To_Bigtable_Yaml",
    category = TemplateCategory.STREAMING,
    type = Template.TemplateType.YAML,
    displayName = "Kafka to Bigtable (YAML)",
    description =
        "The Kafka to Bigtable template is a streaming pipeline which ingests data from an Apache Kafka topic, executes a user-defined mapping, and writes the resulting records to Bigtable.",
    flexContainerName = "pipeline-yaml",
    yamlTemplateFile = "KafkaToBigtable.yaml",
    filesToCopy = {"main.py", "requirements.txt"},
    documentation =
        "https://cloud.google.com/dataflow/docs/guides/templates/provided-yaml/kafka-to-bigtable",
    contactInformation = "https://cloud.google.com/support",
    requirements = {
      "The input Apache Kafka topic must exist.",
      "The Apache Kafka broker server must be running and be reachable from the Dataflow worker machines.",
      "The output Bigtable table must exist."
    },
    streaming = true,
    hidden = false)
public interface KafkaToBigtableYaml {

  @TemplateParameter.Text(
      order = 1,
      name = "bootstrapServers",
      optional = false,
      description =
          "A list of host/port pairs to use for establishing the initial connection to the Kafka cluster.",
      helpText =
          "A list of host/port pairs to use for establishing the initial connection to the Kafka cluster. For example: host1:port1,host2:port2",
      example = "host1:port1,host2:port2,localhost:9092,127.0.0.1:9093")
  @Validation.Required
  String getBootstrapServers();

  @TemplateParameter.Text(
      order = 2,
      name = "topic",
      optional = false,
      description = "Kafka topic to read from.",
      helpText = "Kafka topic to read from. For example: my_topic",
      example = "my_topic")
  @Validation.Required
  String getTopic();

  @TemplateParameter.Text(
      order = 3,
      name = "confluentSchemaRegistrySubject",
      optional = true,
      description = "The subject name for the Confluent Schema Registry.",
      helpText = "The subject name for the Confluent Schema Registry. For example: my_subject",
      example = "my_subject")
  String getConfluentSchemaRegistrySubject();

  @TemplateParameter.Text(
      order = 4,
      name = "confluentSchemaRegistryUrl",
      optional = true,
      description = "The URL for the Confluent Schema Registry.",
      helpText =
          "The URL for the Confluent Schema Registry. For example: http://schema-registry:8081",
      example = "http://schema-registry:8081")
  String getConfluentSchemaRegistryUrl();

  @TemplateParameter.Text(
      order = 5,
      name = "consumerConfigUpdates",
      optional = true,
      description =
          "A list of key-value pairs that act as configuration parameters for Kafka consumers.",
      helpText =
          "A list of key-value pairs that act as configuration parameters for Kafka consumers. For example: {'group.id': 'my_group'}",
      example = "{\"group.id\": \"my_group\"}")
  String getConsumerConfigUpdates();

  @TemplateParameter.Text(
      order = 6,
      name = "fileDescriptorPath",
      optional = true,
      description = "The path to the Protocol Buffer File Descriptor Set file.",
      helpText =
          "The path to the Protocol Buffer File Descriptor Set file. For example: gs://bucket/path/to/descriptor.pb",
      example = "gs://bucket/path/to/descriptor.pb")
  String getFileDescriptorPath();

  @TemplateParameter.Text(
      order = 7,
      name = "format",
      optional = true,
      description = "The encoding format for the data stored in Kafka.",
      helpText =
          "The encoding format for the data stored in Kafka. Valid options are: RAW,STRING,AVRO,JSON,PROTO. For example: JSON",
      example = "JSON")
  @Default.String("JSON")
  String getFormat();

  @TemplateParameter.Text(
      order = 8,
      name = "messageName",
      optional = true,
      description =
          "The name of the Protocol Buffer message to be used for schema extraction and data conversion.",
      helpText =
          "The name of the Protocol Buffer message to be used for schema extraction and data conversion. For example: MyMessage",
      example = "MyMessage")
  String getMessageName();

  @TemplateParameter.Text(
      order = 9,
      name = "schema",
      optional = true,
      description = "The schema in which the data is encoded in the Kafka topic.",
      helpText =
          "The schema in which the data is encoded in the Kafka topic.  For example: {'type': 'record', 'name': 'User', 'fields': [{'name': 'name', 'type': 'string'}]}. A schema is required if data format is JSON, AVRO or PROTO.",
      example =
          "{\"type\": \"record\", \"name\": \"User\", \"fields\": [{\"name\": \"name\", \"type\": \"string\"}]}")
  String getSchema();

  @TemplateParameter.Text(
      order = 10,
      name = "language",
      optional = true,
      description = "Language used to define the expressions.",
      helpText =
          "The language used to define (and execute) the expressions and/or  callables in fields. Defaults to generic.",
      example = "python")
  @Default.String("generic")
  String getLanguage();

  @TemplateParameter.Text(
      order = 11,
      name = "fields",
      optional = false,
      description = "Field mapping configuration",
      helpText =
          "The output fields to compute, each mapping to the expression or callable that creates them.",
      example = "{\"key\": {\"expression\": \"key.encode('utf-8')\", \"output_type\": \"bytes\"}}")
  @Validation.Required
  String getFields();

  @TemplateParameter.Text(
      order = 12,
      name = "projectId",
      optional = false,
      description = "Bigtable project ID",
      helpText = "The Google Cloud project ID of the Bigtable instance.",
      example = "my-gcp-project")
  @Validation.Required
  String getProjectId();

  @TemplateParameter.Text(
      order = 13,
      name = "instanceId",
      optional = false,
      description = "Bigtable instance ID",
      helpText = "The Bigtable instance ID.",
      example = "my-bigtable-instance")
  @Validation.Required
  String getInstanceId();

  @TemplateParameter.Text(
      order = 14,
      name = "tableId",
      optional = false,
      description = "Bigtable output table",
      helpText = "Bigtable table ID to write the output to.",
      example = "my-bigtable-table")
  @Validation.Required
  String getTableId();

  @TemplateParameter.Text(
      order = 15,
      name = "windowing",
      optional = false,
      description = "Windowing options",
      helpText =
          "Windowing options - see https://beam.apache.org/documentation/sdks/yaml/#windowing",
      example = "{\"type\": \"fixed\", \"size\": \"10s\"}")
  @Validation.Required
  String getWindowing();
}
