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
package com.google.cloud.teleport.v2.templates;

import com.google.cloud.teleport.metadata.TemplateParameter;
import com.google.cloud.teleport.v2.options.CommonTemplateOptions;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.options.Validation.Required;

public interface CdcDataGeneratorOptions
    extends CommonTemplateOptions, GcpOptions, StreamingOptions {

  @TemplateParameter.Enum(
      order = 1,
      enumOptions = {
        @TemplateParameter.TemplateEnumOption("SPANNER"),
        @TemplateParameter.TemplateEnumOption("MYSQL")
      },
      description = "Type of sink to generate data for",
      helpText = "The type of sink to generate data for. Supported values: SPANNER, MYSQL.")
  @Required
  SinkType getSinkType();

  void setSinkType(SinkType value);

  @TemplateParameter.GcsReadFile(
      order = 2,
      optional = false,
      description = "Sink Options JSON File Path",
      helpText =
          "GCS Path to a file containing JSON Options for the sink. For Spanner: {\"projectId\": \"...\", \"instanceId\": \"...\", \"databaseId\": \"...\"}. "
              + "For MySQL: {\"driverClassName\": \"...\", \"connectionUrl\": \"...\", \"username\": \"...\", \"password\": \"...\"} or {\"shardFilePath\": \"gs://...\"}",
      example = "gs://your-bucket/path/to/sink_options.json")
  String getSinkOptions();

  void setSinkOptions(String value);

  @TemplateParameter.Integer(
      order = 3,
      optional = true,
      description = "Batch Size",
      helpText =
          "The batch size for writing to the sink. Crucially, this threshold is evaluated per table (partitioned by table name, shard, and operation type). Default is 100.")
  @Default.Integer(100)
  Integer getBatchSize();

  void setBatchSize(Integer value);

  @TemplateParameter.Integer(
      order = 4,
      optional = true,
      description = "Insert QPS per table",
      helpText = "The target Insert QPS for each table. Default is 1000.")
  @Default.Integer(1000)
  Integer getInsertQps();

  void setInsertQps(Integer value);

  @TemplateParameter.Integer(
      order = 5,
      optional = true,
      description = "Update QPS per table",
      helpText = "The target Update QPS for each table. Default is 0.")
  @Default.Integer(0)
  Integer getUpdateQps();

  void setUpdateQps(Integer value);

  @TemplateParameter.Integer(
      order = 6,
      optional = true,
      description = "Delete QPS per table",
      helpText = "The target Delete QPS for each table. Default is 0.")
  @Default.Integer(0)
  Integer getDeleteQps();

  void setDeleteQps(Integer value);

  @TemplateParameter.Integer(
      order = 7,
      optional = true,
      description = "JDBC Connection Pool Size per Shard",
      helpText =
          "The maximum number of connections to open per logical shard for JDBC (MySQL) sink. Default is 10.")
  @Default.Integer(10)
  Integer getJdbcPoolSize();

  void setJdbcPoolSize(Integer value);

  @TemplateParameter.Integer(
      order = 8,
      optional = true,
      description = "Update Interval in Seconds",
      helpText =
          "Interval cadence between successive UPDATEs for a given row. Default is 5 seconds.")
  @Default.Integer(5)
  Integer getUpdateInterval();

  void setUpdateInterval(Integer value);

  @TemplateParameter.Integer(
      order = 9,
      optional = true,
      description = "Delete Interval in Seconds",
      helpText =
          "Interval cadence trailing standard updates before a trailing DELETE is scheduled. Default is 5 seconds.")
  @Default.Integer(5)
  Integer getDeleteInterval();

  void setDeleteInterval(Integer value);

  @TemplateParameter.GcsReadFile(
      order = 10,
      optional = true,
      description = "Schema Config JSON File Path",
      helpText = "GCS Path to a file containing JSON overrides for data generation.",
      example = "gs://your-bucket/path/to/schema_config.json")
  String getSchemaConfig();

  void setSchemaConfig(String value);

  @TemplateParameter.GcsWriteFolder(
      order = 11,
      optional = true,
      description = "Dead-letter queue directory",
      helpText = "The GCS directory to write dead-letter queue records to.",
      example = "gs://your-bucket/dlq")
  String getDlqDirectory();

  void setDlqDirectory(String value);

  enum SinkType {
    SPANNER,
    MYSQL
  }
}
