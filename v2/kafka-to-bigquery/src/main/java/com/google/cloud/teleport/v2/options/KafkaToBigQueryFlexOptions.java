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
import com.google.cloud.teleport.v2.kafka.dlq.BigQueryDeadLetterQueueOptions;
import com.google.cloud.teleport.v2.kafka.options.KafkaReadOptions;
import com.google.cloud.teleport.v2.kafka.options.SchemaRegistryOptions;
import com.google.cloud.teleport.v2.transforms.JavascriptTextTransformer.JavascriptTextTransformerOptions;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Default;

/**
 * The {@link KafkaToBigQueryFlexOptions} class provides the custom execution options passed by the
 * executor at the command-line.
 */
public interface KafkaToBigQueryFlexOptions
    extends DataflowPipelineOptions,
        KafkaReadOptions,
        BigQueryStorageApiStreamingOptions,
        SchemaRegistryOptions,
        BigQueryDeadLetterQueueOptions,
        JavascriptTextTransformerOptions {
  // This is a duplicate option that already exist in KafkaReadOptions but keeping it here
  // so the KafkaTopic appears above the authentication enum on the Templates UI.
  @TemplateParameter.KafkaReadTopic(
      order = 1,
      name = "readBootstrapServerAndTopic",
      groupName = "Source",
      description = "Source Kafka Topic",
      helpText = "Kafka Topic to read the input from.")
  String getReadBootstrapServerAndTopic();

  void setReadBootstrapServerAndTopic(String value);

  @TemplateParameter.Boolean(
      order = 3,
      groupName = "Source",
      optional = true,
      description = "Persist the Kafka Message Key to the BigQuery table",
      helpText =
          "If true, the pipeline will persist the Kafka message key in the BigQuery table, in a `_key` field of type `BYTES`. Default is `false` (Key is ignored).")
  @Default.Boolean(false)
  Boolean getPersistKafkaKey();

  void setPersistKafkaKey(Boolean value);

  @TemplateParameter.Enum(
      order = 4,
      name = "writeMode",
      groupName = "Destination",
      enumOptions = {
        @TemplateParameter.TemplateEnumOption("SINGLE_TABLE_NAME"),
        @TemplateParameter.TemplateEnumOption("DYNAMIC_TABLE_NAMES"),
      },
      optional = false,
      description = "Table Name Strategy",
      helpText =
          "Write records to one table or multiple tables (based on schema)."
              + " The `DYNAMIC_TABLE_NAMES` mode is supported only for `AVRO_CONFLUENT_WIRE_FORMAT` Source Message Format"
              + " and `SCHEMA_REGISTRY` Schema Source. The target table name is auto-generated based on the Avro"
              + " schema name of each message, it could either be a single schema (creating a single table) or"
              + " multiple schemas (creating multiple tables). The `SINGLE_TABLE_NAME` mode writes to a single"
              + " table (single schema) specified by the user. Defaults to `SINGLE_TABLE_NAME`.")
  @Default.String("SINGLE_TABLE_NAME")
  String getWriteMode();

  void setWriteMode(String value);

  @TemplateParameter.BigQueryTable(
      order = 3,
      parentName = "writeMode",
      parentTriggerValues = {"SINGLE_TABLE_NAME"},
      groupName = "Destination",
      optional = true,
      description = "BigQuery output table",
      helpText =
          "BigQuery table location to write the output to. The name should be in the format "
              + "`<project>:<dataset>.<table_name>`. The table's schema must match input objects.")
  String getOutputTableSpec();

  void setOutputTableSpec(String value);

  @TemplateParameter.Text(
      order = 5,
      groupName = "Destination",
      parentName = "writeMode",
      parentTriggerValues = {"DYNAMIC_TABLE_NAMES"},
      optional = true,
      description = "BigQuery output project",
      helpText =
          "BigQuery output project in wehich the dataset resides. Tables will be created dynamically in the dataset.")
  @Default.String("")
  String getOutputProject();

  void setOutputProject(String value);

  @TemplateParameter.Text(
      order = 6,
      groupName = "Destination",
      parentName = "writeMode",
      parentTriggerValues = {"DYNAMIC_TABLE_NAMES"},
      optional = true,
      description = "BigQuery output dataset",
      helpText =
          "BigQuery output dataset to write the output to. Tables will be created dynamically in the dataset."
              + " If the tables are created beforehand, the table names should follow the specified naming convention."
              + " The name should be `bqTableNamePrefix + Avro Schema FullName` ,"
              + " each word will be separated by a hyphen `-`.")
  @Default.String("")
  String getOutputDataset();

  void setOutputDataset(String value);

  @TemplateParameter.Text(
      order = 7,
      parentName = "writeMode",
      parentTriggerValues = {"DYNAMIC_TABLE_NAMES"},
      optional = true,
      description = "BigQuery Table naming prefix",
      helpText =
          "Naming prefix to be used while creating BigQuery output tables. Only applicable when using schema registry.")
  @Default.String("")
  String getBqTableNamePrefix();

  void setBqTableNamePrefix(String value);

  @TemplateParameter.Enum(
      order = 8,
      groupName = "Destination",
      enumOptions = {
        @TemplateParameter.TemplateEnumOption("WRITE_APPEND"),
        @TemplateParameter.TemplateEnumOption("WRITE_EMPTY"),
        @TemplateParameter.TemplateEnumOption("WRITE_TRUNCATE")
      },
      optional = true,
      description = "Write Disposition to use for BigQuery",
      helpText =
          "BigQuery WriteDisposition. For example: `WRITE_APPEND`, `WRITE_EMPTY` or `WRITE_TRUNCATE`.",
      hiddenUi = true)
  @Default.String("WRITE_APPEND")
  String getWriteDisposition();

  void setWriteDisposition(String writeDisposition);

  @TemplateParameter.Enum(
      order = 8,
      groupName = "Destination",
      enumOptions = {
        @TemplateParameter.TemplateEnumOption("CREATE_IF_NEEDED"),
        @TemplateParameter.TemplateEnumOption("CREATE_NEVER")
      },
      optional = true,
      description = "Create Disposition to use for BigQuery",
      helpText = "BigQuery CreateDisposition. For example: `CREATE_IF_NEEDED`, `CREATE_NEVER`.",
      hiddenUi = true)
  @Default.String("CREATE_IF_NEEDED")
  String getCreateDisposition();

  void setCreateDisposition(String createDisposition);

  @TemplateParameter.Boolean(
      order = 9,
      groupName = "Destination",
      optional = true,
      description = "Use BigQuery Storage Write API",
      helpText =
          "Do not use this parameter, Storage Write API is the only available method to write to BigQuery. "
              + "Setting this to `false` will have no effect. ",
      hiddenUi = true)
  @Default.Boolean(false)
  @Override
  Boolean getUseStorageWriteApi();

  @TemplateParameter.Boolean(
      order = 10,
      groupName = "Destination",
      optional = true,
      description = "Use auto-sharding when writing to BigQuery",
      helpText =
          "If true, the pipeline uses auto-sharding when writng to BigQuery"
              + "The default value is `true`.",
      hiddenUi = true)
  @Default.Boolean(true)
  Boolean getUseAutoSharding();

  void setUseAutoSharding(Boolean value);

  @TemplateParameter.Integer(
      order = 11,
      groupName = "Destination",
      optional = true,
      description = "Number of streams for BigQuery Storage Write API",
      helpText =
          "Specifies the number of write streams, this parameter must be set. Default is `0`.")
  @Override
  @Default.Integer(0)
  Integer getNumStorageWriteApiStreams();

  @TemplateParameter.Integer(
      order = 12,
      groupName = "Destination",
      optional = true,
      description = "Triggering frequency in seconds for BigQuery Storage Write API",
      helpText =
          "Specifies the triggering frequency in seconds, this parameter must be set. "
              + "Default is 5 seconds.")
  @Override
  Integer getStorageWriteApiTriggeringFrequencySec();

  @TemplateParameter.Boolean(
      order = 13,
      groupName = "Destination",
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
