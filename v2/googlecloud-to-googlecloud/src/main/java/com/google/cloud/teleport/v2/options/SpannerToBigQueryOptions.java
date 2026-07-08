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
          "The ID of the project that the Spanner database resides in. The default value for this parameter is the project "
              + "where the Dataflow pipeline is running.")
  @Default.String("")
  String getSpannerProjectId();

  void setSpannerProjectId(String projectId);

  @TemplateParameter.Text(
      order = 2,
      groupName = "Source",
      description = "Spanner instance ID",
      helpText = "The instance ID of the Spanner database to read from.")
  String getSpannerInstanceId();

  void setSpannerInstanceId(String spannerInstanceId);

  @TemplateParameter.Text(
      order = 3,
      groupName = "Source",
      description = "Spanner database ID",
      helpText = "The database ID of the Spanner database to export.")
  String getSpannerDatabaseId();

  void setSpannerDatabaseId(String spannerDatabaseId);

  @TemplateParameter.Text(
      order = 4,
      groupName = "Source",
      optional = true,
      description = "Spanner table name",
      helpText = "The table name of the Spanner database to export. Ignored if sqlQuery is set.")
  String getSpannerTableId();

  void setSpannerTableId(String spannerTableId);

  @TemplateParameter.Enum(
      order = 5,
      enumOptions = {
        @TemplateParameter.TemplateEnumOption("HIGH"),
        @TemplateParameter.TemplateEnumOption("MEDIUM"),
        @TemplateParameter.TemplateEnumOption("LOW")
      },
      optional = true,
      description = "Priority for Spanner RPC invocations",
      helpText =
          "The request priority (https://cloud.google.com/spanner/docs/reference/rest/v1/RequestOptions) for Spanner calls. Possible values are `HIGH`, `MEDIUM`, and `LOW`. The default value is `HIGH`.")
  RpcPriority getSpannerRpcPriority();

  void setSpannerRpcPriority(RpcPriority spannerRpcPriority);

  @TemplateParameter.Text(
      order = 6,
      optional = true,
      description = "Spanner query",
      helpText =
          "The SQL query to use to read data from the Spanner database. Required if spannerTableId is empty.")
  String getSqlQuery();

  void setSqlQuery(String sqlQuery);

  @TemplateParameter.GcsReadFile(
      order = 7,
      optional = true,
      description = "Cloud Storage path to BigQuery JSON schema",
      helpText =
          "The Cloud Storage path (gs://) to the JSON file that defines your BigQuery schema. This is required if the Create Disposition is not CREATE_NEVER",
      example = "gs://your-bucket/your-schema.json")
  String getBigQuerySchemaPath();

  void setBigQuerySchemaPath(String bigQuerySchemaPath);
}
