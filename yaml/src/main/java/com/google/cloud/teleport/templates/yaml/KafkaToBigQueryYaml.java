/*
 * Copyright (C) 2025 Google LLC
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
    name = "Kafka_to_BigQuery_Yaml",
    category = TemplateCategory.STREAMING,
    type = Template.TemplateType.YAML,
    displayName = "Kafka to BigQuery (YAML)",
    description =
        "The Apache Kafka to BigQuery template is a streaming pipeline which ingests  text data from Apache Kafka, executes a user-defined function (UDF), and  outputs the resulting records to BigQuery. Any errors which occur in the  transformation of the data, execution of the UDF, or inserting into the  output table are inserted into a separate errors table in BigQuery.  If the errors table does not exist prior to execution, then it is created.",
    flexContainerName = "pipeline-yaml",
    yamlTemplateFile = "KafkaToBigQuery.yaml",
    filesToCopy = {"main.py", "requirements.txt"},
    documentation =
        "https://cloud.google.com/dataflow/docs/guides/templates/provided/kafka-to-bigquery",
    contactInformation = "https://cloud.google.com/support",
    requirements = {
      "The output BigQuery table must exist.",
      "The Apache Kafka broker server must be running and be reachable from the Dataflow worker machines.",
      "The Apache Kafka topics must exist and the messages must be encoded in a valid JSON format."
    },
    streaming = true,
    hidden = false)
public interface KafkaToBigQueryYaml {

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

  @TemplateParameter.Boolean(
      order = 3,
      name = "allowDuplicates",
      optional = true,
      description = "If the Kafka read allows duplicates.",
      helpText = "If the Kafka read allows duplicates. For example: true",
      example = "true")
  Boolean getAllowDuplicates();

  @TemplateParameter.Text(
      order = 4,
      name = "confluentSchemaRegistrySubject",
      optional = true,
      description = "The subject name for the Confluent Schema Registry.",
      helpText = "The subject name for the Confluent Schema Registry. For example: my_subject",
      example = "my_subject")
  String getConfluentSchemaRegistrySubject();

  @TemplateParameter.Text(
      order = 5,
      name = "confluentSchemaRegistryUrl",
      optional = true,
      description = "The URL for the Confluent Schema Registry.",
      helpText =
          "The URL for the Confluent Schema Registry. For example: http://schema-registry:8081",
      example = "http://schema-registry:8081")
  String getConfluentSchemaRegistryUrl();

  @TemplateParameter.Text(
      order = 6,
      name = "consumerConfigUpdates",
      optional = true,
      description =
          "A list of key-value pairs that act as configuration parameters for Kafka consumers.",
      helpText =
          "A list of key-value pairs that act as configuration parameters for Kafka consumers. For example: {'group.id': 'my_group'}",
      example = "{\"group.id\": \"my_group\"}")
  String getConsumerConfigUpdates();

  @TemplateParameter.Text(
      order = 7,
      name = "fileDescriptorPath",
      optional = true,
      description = "The path to the Protocol Buffer File Descriptor Set file.",
      helpText =
          "The path to the Protocol Buffer File Descriptor Set file. For example: gs://bucket/path/to/descriptor.pb",
      example = "gs://bucket/path/to/descriptor.pb")
  String getFileDescriptorPath();

  @TemplateParameter.Text(
      order = 8,
      name = "format",
      optional = true,
      description = "The encoding format for the data stored in Kafka.",
      helpText =
          "The encoding format for the data stored in Kafka. Valid options are: RAW,STRING,AVRO,JSON,PROTO. For example: JSON",
      example = "JSON")
  @Default.String("JSON")
  String getFormat();

  @TemplateParameter.Text(
      order = 9,
      name = "messageName",
      optional = true,
      description =
          "The name of the Protocol Buffer message to be used for schema extraction and data conversion.",
      helpText =
          "The name of the Protocol Buffer message to be used for schema extraction and data conversion. For example: MyMessage",
      example = "MyMessage")
  String getMessageName();

  @TemplateParameter.Boolean(
      order = 10,
      name = "offsetDeduplication",
      optional = true,
      description = "If the redistribute is using offset deduplication mode.",
      helpText = "If the redistribute is using offset deduplication mode. For example: true",
      example = "true")
  Boolean getOffsetDeduplication();

  @TemplateParameter.Boolean(
      order = 11,
      name = "redistributeByRecordKey",
      optional = true,
      description = "If the redistribute keys by the Kafka record key.",
      helpText = "If the redistribute keys by the Kafka record key. For example: true",
      example = "true")
  Boolean getRedistributeByRecordKey();

  @TemplateParameter.Integer(
      order = 12,
      name = "redistributeNumKeys",
      optional = true,
      description = "The number of keys for redistributing Kafka inputs.",
      helpText = "The number of keys for redistributing Kafka inputs. For example: 10",
      example = "10")
  Integer getRedistributeNumKeys();

  @TemplateParameter.Boolean(
      order = 13,
      name = "redistributed",
      optional = true,
      description = "If the Kafka read should be redistributed.",
      helpText = "If the Kafka read should be redistributed. For example: true",
      example = "true")
  Boolean getRedistributed();

  @TemplateParameter.Text(
      order = 14,
      name = "schema",
      optional = true,
      description = "The schema in which the data is encoded in the Kafka topic.",
      helpText =
          "The schema in which the data is encoded in the Kafka topic.  For example: {'type': 'record', 'name': 'User', 'fields': [{'name': 'name', 'type': 'string'}]}. A schema is required if data format is JSON, AVRO or PROTO.",
      example =
          "{\"type\": \"record\", \"name\": \"User\", \"fields\": [{\"name\": \"name\", \"type\": \"string\"}]}")
  String getSchema();

  @TemplateParameter.Text(
      order = 15,
      name = "table",
      optional = false,
      description = "BigQuery table",
      helpText =
          "BigQuery table location to write the output to or read from. The name  should be in the format <project>:<dataset>.<table_name>`. For write,  the table's schema must match input objects.",
      example = "")
  @Validation.Required
  String getTable();

  @TemplateParameter.Text(
      order = 16,
      name = "createDisposition",
      optional = true,
      description = "How to create",
      helpText =
          "Specifies whether a table should be created if it does not exist.  Valid inputs are 'Never' and 'IfNeeded'.",
      example = "")
  @Default.String("CREATE_IF_NEEDED")
  String getCreateDisposition();

  @TemplateParameter.Text(
      order = 17,
      name = "writeDisposition",
      optional = true,
      description = "How to write",
      helpText =
          "How to specify if a write should append to an existing table, replace the table, or verify that the table is empty. Note that the my_dataset being written to must already exist. Unbounded collections can only be written using 'WRITE_EMPTY' or 'WRITE_APPEND'.",
      example = "")
  @Default.String("WRITE_APPEND")
  String getWriteDisposition();

  @TemplateParameter.Integer(
      order = 18,
      name = "numStreams",
      optional = true,
      description = "Number of streams for BigQuery Storage Write API",
      helpText =
          "Number of streams defines the parallelism of the BigQueryIO’s Write  transform and roughly corresponds to the number of Storage Write API’s  streams which will be used by the pipeline. See https://cloud.google.com/blog/products/data-analytics/streaming-data-into-bigquery-using-storage-write-api for the recommended values. The default value is 1.",
      example = "")
  @Default.Integer(1)
  Integer getNumStreams();

  @TemplateParameter.Text(
      order = 19,
      name = "outputDeadletterTable",
      optional = false,
      description = "The dead-letter table name to output failed messages to BigQuery",
      helpText =
          "BigQuery table for failed messages. Messages failed to reach the output  table for different reasons (e.g., mismatched schema, malformed json)  are written to this table. If it doesn't exist, it will be created  during pipeline execution. If not specified,  'outputTableSpec_error_records' is used instead. The dead-letter table name to output failed messages to BigQuery.",
      example = "your-project-id:your-dataset.your-table-name")
  @Validation.Required
  String getOutputDeadletterTable();
}
