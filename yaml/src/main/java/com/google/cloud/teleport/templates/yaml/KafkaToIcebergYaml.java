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
    name = "Kafka_To_Iceberg_Yaml",
    category = TemplateCategory.STREAMING,
    type = Template.TemplateType.YAML,
    displayName = "Kafka to Iceberg (YAML)",
    description =
        "The Kafka to Iceberg template is a streaming pipeline that reads data from Kafka and writes to an Iceberg table.",
    flexContainerName = "kafka-to-iceberg-yaml",
    yamlTemplateFile = "KafkaToIceberg.yaml",
    filesToCopy = {
      "template.yaml",
      "main.py",
      "requirements.txt",
      "options/kafka_options.yaml",
      "options/iceberg_options.yaml"
    },
    documentation = "",
    contactInformation = "https://cloud.google.com/support",
    requirements = {
      "The Input Kafka topic must exist.",
      "The Output Iceberg table need not exist, but the storage must exist and passed through catalog_properties."
    },
    streaming = true,
    hidden = false)
public interface KafkaToIcebergYaml {

  @TemplateParameter.Text(
      order = 1,
      name = "bootstrap_servers",
      optional = false,
      description =
          "A list of host/port pairs to use for establishing the initial connection to the Kafka cluster.",
      helpText =
          "A list of host/port pairs to use for establishing the initial connection to the Kafka cluster. For example: host1:port1,host2:port2",
      example = "host1:port1,host2:port2")
  @Validation.Required
  String getBootstrap_servers();

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
      name = "allow_duplicates",
      optional = true,
      description = "If the Kafka read allows duplicates.",
      helpText = "If the Kafka read allows duplicates. For example: true",
      example = "true")
  Boolean getAllow_duplicates();

  @TemplateParameter.Text(
      order = 4,
      name = "confluent_schema_registry_subject",
      optional = true,
      description = "The subject name for the Confluent Schema Registry.",
      helpText = "The subject name for the Confluent Schema Registry. For example: my_subject",
      example = "my_subject")
  String getConfluent_schema_registry_subject();

  @TemplateParameter.Text(
      order = 5,
      name = "confluent_schema_registry_url",
      optional = true,
      description = "The URL for the Confluent Schema Registry.",
      helpText =
          "The URL for the Confluent Schema Registry. For example: http://schema-registry:8081",
      example = "http://schema-registry:8081")
  String getConfluent_schema_registry_url();

  @TemplateParameter.Text(
      order = 6,
      name = "consumer_config_updates",
      optional = true,
      description =
          "A list of key-value pairs that act as configuration parameters for Kafka consumers.",
      helpText =
          "A list of key-value pairs that act as configuration parameters for Kafka consumers. For example: {'group.id': 'my_group'}",
      example = "{\"group.id\": \"my_group\"}")
  String getConsumer_config_updates();

  @TemplateParameter.Text(
      order = 7,
      name = "file_descriptor_path",
      optional = true,
      description = "The path to the Protocol Buffer File Descriptor Set file.",
      helpText =
          "The path to the Protocol Buffer File Descriptor Set file. For example: gs://bucket/path/to/descriptor.pb",
      example = "gs://bucket/path/to/descriptor.pb")
  String getFile_descriptor_path();

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
      name = "message_name",
      optional = true,
      description =
          "The name of the Protocol Buffer message to be used for schema extraction and data conversion.",
      helpText =
          "The name of the Protocol Buffer message to be used for schema extraction and data conversion. For example: MyMessage",
      example = "MyMessage")
  String getMessage_name();

  @TemplateParameter.Boolean(
      order = 10,
      name = "offset_deduplication",
      optional = true,
      description = "If the redistribute is using offset deduplication mode.",
      helpText = "If the redistribute is using offset deduplication mode. For example: true",
      example = "true")
  Boolean getOffset_deduplication();

  @TemplateParameter.Boolean(
      order = 11,
      name = "redistribute_by_record_key",
      optional = true,
      description = "If the redistribute keys by the Kafka record key.",
      helpText = "If the redistribute keys by the Kafka record key. For example: true",
      example = "true")
  Boolean getRedistribute_by_record_key();

  @TemplateParameter.Integer(
      order = 12,
      name = "redistribute_num_keys",
      optional = true,
      description = "The number of keys for redistributing Kafka inputs.",
      helpText = "The number of keys for redistributing Kafka inputs. For example: 10",
      example = "10")
  Integer getRedistribute_num_keys();

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
          "The schema in which the data is encoded in the Kafka topic. For example: {'type': 'record', 'name': 'User', 'fields': [{'name': 'name', 'type': 'string'}]}",
      example =
          "{\"type\": \"record\", \"name\": \"User\", \"fields\": [{\"name\": \"name\", \"type\": \"string\"}]}")
  String getSchema();

  @TemplateParameter.Text(
      order = 15,
      name = "table",
      optional = false,
      description = "A fully-qualified table identifier.",
      helpText = "A fully-qualified table identifier, e.g., my_dataset.my_table.",
      example = "my_dataset.my_table")
  @Validation.Required
  String getTable();

  @TemplateParameter.Text(
      order = 16,
      name = "catalogName",
      optional = false,
      description = "Name of the catalog containing the table.",
      helpText = "The name of the Iceberg catalog that contains the table.",
      example = "my_hadoop_catalog")
  @Validation.Required
  String getCatalogName();

  @TemplateParameter.Text(
      order = 17,
      name = "catalogProperties",
      optional = false,
      description = "Properties used to set up the Iceberg catalog.",
      helpText = "A map of properties for setting up the Iceberg catalog.",
      example = "{\"type\": \"hadoop\", \"warehouse\": \"gs://your-bucket/warehouse\"}")
  @Validation.Required
  String getCatalogProperties();

  @TemplateParameter.Text(
      order = 18,
      name = "configProperties",
      optional = true,
      description = "Properties passed to the Hadoop Configuration.",
      helpText = "A map of properties to pass to the Hadoop Configuration.",
      example = "{\"fs.gs.impl\": \"com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem\"}")
  String getConfigProperties();

  @TemplateParameter.Text(
      order = 19,
      name = "drop",
      optional = true,
      description = "A list of field names to drop from the input record before writing.",
      helpText = "A list of field names to drop. Mutually exclusive with 'keep' and 'only'.",
      example = "[\"field_to_drop_1\", \"field_to_drop_2\"]")
  String getDrop();

  @TemplateParameter.Text(
      order = 20,
      name = "keep",
      optional = true,
      description = "A list of field names to keep in the input record.",
      helpText = "A list of field names to keep. Mutually exclusive with 'drop' and 'only'.",
      example = "[\"field_to_keep_1\", \"field_to_keep_2\"]")
  String getKeep();

  @TemplateParameter.Text(
      order = 21,
      name = "only",
      optional = true,
      description = "The name of a single record field that should be written.",
      helpText = "The name of a single field to write. Mutually exclusive with 'keep' and 'drop'.",
      example = "my_record_field")
  String getOnly();

  @TemplateParameter.Text(
      order = 22,
      name = "partitionFields",
      optional = true,
      description = "Fields used to create a partition spec for new tables.",
      helpText = "A list of fields and transforms for partitioning, e.g., ['day(ts)', 'category'].",
      example = "[\"day(ts)\", \"bucket(id, 4)\"]")
  String getPartitionFields();

  @TemplateParameter.Text(
      order = 23,
      name = "tableProperties",
      optional = true,
      description = "Iceberg table properties to be set on table creation.",
      helpText = "A map of Iceberg table properties to set when the table is created.",
      example = "{\"commit.retry.num-retries\": \"2\"}")
  String getTableProperties();

  @TemplateParameter.Integer(
      order = 24,
      name = "triggeringFrequencySeconds",
      optional = false,
      description = "For a streaming pipeline, the frequency at which snapshots are produced.",
      helpText = "The frequency in seconds for producing snapshots in a streaming pipeline.",
      example = "60")
  @Validation.Required
  Integer getTriggeringFrequencySeconds();
}
