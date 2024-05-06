/*
 * Copyright (C) 2024 Google LLC
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

import com.google.cloud.teleport.metadata.TemplateParameter;
import com.google.cloud.teleport.v2.kafka.options.KafkaReadOptions;
import org.apache.beam.sdk.options.Default;

/**
 * The {@link KafkaToBigQueryFlexOptions} class provides the custom execution options passed by the
 * executor at the command-line.
 */
public interface KafkaToBigQueryFlexOptions
    extends KafkaReadOptions, BigQueryStorageApiStreamingOptions {

  @TemplateParameter.BigQueryTable(
      order = 1,
      optional = true,
      description = "BigQuery output table",
      helpText =
          "BigQuery table location to write the output to. The name should be in the format "
              + "`<project>:<dataset>.<table_name>`. The table's schema must match input objects.")
  String getOutputTableSpec();

  void setOutputTableSpec(String value);

  @TemplateParameter.Enum(
      order = 2,
      enumOptions = {
        @TemplateParameter.TemplateEnumOption("WRITE_APPEND"),
        @TemplateParameter.TemplateEnumOption("WRITE_EMPTY"),
        @TemplateParameter.TemplateEnumOption("WRITE_TRUNCATE")
      },
      optional = true,
      description = "Write Disposition to use for BigQuery",
      helpText =
          "BigQuery WriteDisposition. For example, WRITE_APPEND, WRITE_EMPTY or WRITE_TRUNCATE.")
  @Default.String("WRITE_APPEND")
  String getWriteDisposition();

  void setWriteDisposition(String writeDisposition);

  @TemplateParameter.Enum(
      order = 3,
      enumOptions = {
        @TemplateParameter.TemplateEnumOption("CREATE_IF_NEEDED"),
        @TemplateParameter.TemplateEnumOption("CREATE_NEVER")
      },
      optional = true,
      description = "Create Disposition to use for BigQuery",
      helpText = "BigQuery CreateDisposition. For example, CREATE_IF_NEEDED, CREATE_NEVER.")
  @Default.String("CREATE_IF_NEEDED")
  String getCreateDisposition();

  void setCreateDisposition(String createDisposition);

  @TemplateParameter.BigQueryTable(
      order = 4,
      optional = true,
      description = "The dead-letter table name to output failed messages to BigQuery",
      helpText =
          "BigQuery table for failed messages. Messages failed to reach the output table for different reasons "
              + "(e.g., mismatched schema, malformed json) are written to this table. If it doesn't exist, it will"
              + " be created during pipeline execution. If not specified, \"outputTableSpec_error_records\" is used instead.",
      example = "your-project-id:your-dataset.your-table-name")
  String getOutputDeadletterTable();

  void setOutputDeadletterTable(String outputDeadletterTable);

  @TemplateParameter.Enum(
      order = 6,
      enumOptions = {
        @TemplateParameter.TemplateEnumOption("AVRO"),
        @TemplateParameter.TemplateEnumOption("JSON")
      },
      optional = true,
      description = "The message format",
      helpText = "The message format. Can be AVRO or JSON.")
  @Default.String("AVRO")
  String getMessageFormat();

  void setMessageFormat(String value);

  // TODO: Sync the enum options with all the Kafka Templates.
  @TemplateParameter.Enum(
      order = 7,
      enumOptions = {
        @TemplateParameter.TemplateEnumOption("CONFLUENT_WIRE_FORMAT"),
        @TemplateParameter.TemplateEnumOption("NON_WIRE_FORMAT")
      },
      optional = true,
      description = "Use the confluent wire format for avro messages.",
      helpText =
          "This parameter is used to indicate if the avro messages use confluent wire format. Default is true (Confluent Wire Format)")
  @Default.String("CONFLUENT_WIRE_FORMAT")
  String getAvroFormat();

  void setAvroFormat(String value);

  @TemplateParameter.GcsReadFile(
      order = 8,
      optional = true,
      description = "Cloud Storage path to the Avro schema file",
      helpText = "Cloud Storage path to Avro schema file. For example, gs://MyBucket/file.avsc.")
  String getAvroSchemaPath();

  void setAvroSchemaPath(String schemaPath);

  @TemplateParameter.Text(
      order = 9,
      optional = true,
      description = "Schema Registry Connection URL.",
      helpText =
          "Schema Registry Connection URL for a registry which supports Confluent wire format.")
  String getSchemaRegistryConnectionUrl();

  void setSchemaRegistryConnectionUrl(String schemaRegistryConnectionUrl);

  @TemplateParameter.Text(
      order = 11,
      optional = true,
      description = "BigQuery output dataset",
      helpText =
          "BigQuery output dataset to write the output to."
              + "Tables will be created dynamically in the dataset.")
  String getOutputDataset();

  void setOutputDataset(String value);

  @TemplateParameter.Text(
      order = 12,
      optional = true,
      description = "Name prefix to be used while creating BigQuery output tables.",
      helpText =
          "Name prefix to be used while creating BigQuery output tables. Only applicable when using schema registry.")
  @Default.String("")
  String getBQTableNamePrefix();

  void setBQTableNamePrefix(String value);

  @TemplateParameter.Boolean(
      order = 13,
      optional = true,
      description = "Use at at-least-once semantics in BigQuery Storage Write API",
      helpText =
          "This parameter takes effect only if \"Use BigQuery Storage Write API\" is enabled. If"
              + " enabled the at-least-once semantics will be used for Storage Write API, otherwise"
              + " exactly-once semantics will be used.",
      hiddenUi = true)
  @Default.Boolean(false)
  @Override
  Boolean getUseStorageWriteApiAtLeastOnce();

  void setUseStorageWriteApiAtLeastOnce(Boolean value);
}
