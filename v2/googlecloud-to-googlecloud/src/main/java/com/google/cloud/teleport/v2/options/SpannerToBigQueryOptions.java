/*
 * Copyright (C) 2022 Google LLC
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

import com.google.cloud.spanner.Options.RpcPriority;
import com.google.cloud.teleport.metadata.TemplateParameter;
import com.google.cloud.teleport.v2.options.BigQueryCommonOptions.WriteOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptions;

/** Custom options for {@link com.google.cloud.teleport.v2.templates.SpannerToBigQuery} pipeline. */
public interface SpannerToBigQueryOptions
    extends PipelineOptions, WriteOptions, BigQueryStorageApiBatchOptions {

  @TemplateParameter.ProjectId(
      order = 1,
      groupName = "Source",
      optional = true,
      description = "Spanner Project ID",
      helpText =
          "The project where the Spanner instance to read from is located. The default for this parameter is the project "
              + "where the Dataflow pipeline is running.")
  @Default.String("")
  String getSpannerProjectId();

  void setSpannerProjectId(String projectId);

  @TemplateParameter.Text(
      order = 2,
      groupName = "Source",
      description = "Spanner instance ID",
      helpText = "The Spanner instance to read from.")
  String getSpannerInstanceId();

  void setSpannerInstanceId(String spannerInstanceId);

  @TemplateParameter.Text(
      order = 3,
      groupName = "Source",
      description = "Spanner database ID",
      helpText = "The Spanner database to read from.")
  String getSpannerDatabaseId();

  void setSpannerDatabaseId(String spannerDatabaseId);

  @TemplateParameter.Text(
      order = 4,
      groupName = "Source",
      description = "Spanner table name",
      helpText = "The Spanner table to read from.")
  String getSpannerTableId();

  void setSpannerTableId(String spannerTableId);

  @TemplateParameter.Enum(
      order = 5,
      groupName = "Source",
      enumOptions = {
        @TemplateParameter.TemplateEnumOption("HIGH"),
        @TemplateParameter.TemplateEnumOption("MEDIUM"),
        @TemplateParameter.TemplateEnumOption("LOW")
      },
      optional = true,
      description = "Priority for Spanner RPC invocations",
      helpText =
          "The priority of Spanner job. Must be one of the following: [HIGH, MEDIUM, LOW]. Default is HIGH.")
  RpcPriority getSpannerRpcPriority();

  void setSpannerRpcPriority(RpcPriority spannerRpcPriority);

  @TemplateParameter.Text(
      order = 6,
      groupName = "Source",
      description = "Spanner query",
      helpText = "Query used to read Spanner table.")
  String getSqlQuery();

  void setSqlQuery(String sqlQuery);

  @TemplateParameter.GcsReadFile(
      order = 7,
      groupName = "Target",
      optional = true,
      description = "Cloud Storage path to BigQuery JSON schema",
      helpText =
          "The Cloud Storage path for the BigQuery JSON schema. If `createDisposition` is not set, or set to CREATE_IF_NEEDED, this parameter must be specified.",
      example = "gs://your-bucket/your-schema.json")
  String getBigQuerySchemaPath();

  void setBigQuerySchemaPath(String bigQuerySchemaPath);
}
