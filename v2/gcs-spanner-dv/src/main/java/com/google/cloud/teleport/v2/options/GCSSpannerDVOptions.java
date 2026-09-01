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
package com.google.cloud.teleport.v2.options;

import com.google.cloud.spanner.Options.RpcPriority;
import com.google.cloud.teleport.metadata.TemplateParameter;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptions;

/**
 * Options supported by the pipeline.
 *
 * <p>Inherits standard configuration options.
 */
  public interface GCSSpannerDVOptions extends PipelineOptions {

    @TemplateParameter.GcsReadFolder(
        order = 1,
        optional = true,
        description = "GCS directory for AVRO files",
        helpText = "This directory is used to read the AVRO files of the records read from source.",
        example = "gs://your-bucket/your-path")
    String getGcsInputDirectory();

    void setGcsInputDirectory(String value);

    @TemplateParameter.ProjectId(
        order = 2,
        optional = true,
        description = "Cloud Spanner Project Id.",
        helpText = "This is the name of the Cloud Spanner project.")
    String getProjectId();

    void setProjectId(String projectId);

    @TemplateParameter.Text(
        order = 3,
        optional = true,
        description = "Cloud Spanner Endpoint to call",
        helpText = "The Cloud Spanner endpoint to call in the template.",
        example = "https://batch-spanner.googleapis.com")
    @Default.String("https://batch-spanner.googleapis.com")
    String getSpannerHost();

    void setSpannerHost(String value);

    @TemplateParameter.Text(
        order = 4,
        groupName = "Target",
        description = "Cloud Spanner Instance Id.",
        helpText = "The destination Cloud Spanner instance.")
    String getInstanceId();

    void setInstanceId(String value);

    @TemplateParameter.Text(
        order = 5,
        regexes = {"^[a-z]([a-z0-9_-]{0,28})[a-z0-9]$"},
        description = "Cloud Spanner Database Id.",
        helpText = "The destination Cloud Spanner database.")
    String getDatabaseId();

    void setDatabaseId(String value);

    @TemplateParameter.Enum(
        order = 6,
        enumOptions = {
          @TemplateParameter.TemplateEnumOption("LOW"),
          @TemplateParameter.TemplateEnumOption("MEDIUM"),
          @TemplateParameter.TemplateEnumOption("HIGH")
        },
        optional = true,
        description = "Priority for Spanner RPC invocations",
        helpText =
            "The request priority for Cloud Spanner calls. The value must be one of:"
                + " [`HIGH`,`MEDIUM`,`LOW`]. Defaults to `HIGH`.")
    @Default.Enum("HIGH")
    RpcPriority getSpannerPriority();

    void setSpannerPriority(RpcPriority value);

    @TemplateParameter.GcsReadFile(
        order = 7,
        optional = true,
        description =
            "Session File Path in Cloud Storage, to provide mapping information in the form of a session file",
        helpText =
            "Session file path in Cloud Storage that contains mapping information from"
                + " Spanner Migration Tool")
    @Default.String("")
    String getSessionFilePath();

    void setSessionFilePath(String value);

    @TemplateParameter.GcsReadFile(
        order = 8,
        optional = true,
        description = "File based overrides from source to spanner",
        helpText =
            "A file which specifies the table and the column name overrides from source to spanner.")
    @Default.String("")
    String getSchemaOverridesFilePath();

    void setSchemaOverridesFilePath(String value);

    @TemplateParameter.Text(
        order = 9,
        optional = true,
        description = "Table name overrides from source to spanner",
        regexes =
            "^\\[([[:space:]]*\\{[[:graph:]]+[[:space:]]*,[[:space:]]*[[:graph:]]+[[:space:]]*\\}[[:space:]]*(,[[:space:]]*)*)*\\]$",
        example = "[{Singers, Vocalists}, {Albums, Records}]",
        helpText =
            "These are the table name overrides from source to spanner. They are written in the"
                + " following format: [{SourceTableName1, SpannerTableName1}, {SourceTableName2, SpannerTableName2}]"
                + " This example shows mapping Singers table to Vocalists and Albums table to Records.")
    @Default.String("")
    String getTableOverrides();

    void setTableOverrides(String value);

    @TemplateParameter.Text(
        order = 10,
        optional = true,
        regexes =
            "^\\[([[:space:]]*\\{[[:space:]]*[[:graph:]]+\\.[[:graph:]]+[[:space:]]*,[[:space:]]*[[:graph:]]+\\.[[:graph:]]+[[:space:]]*\\}[[:space:]]*(,[[:space:]]*)*)*\\]$",
        description = "Column name overrides from source to spanner",
        example =
            "[{Singers.SingerName, Singers.TalentName}, {Albums.AlbumName, Albums.RecordName}]",
        helpText =
            "These are the column name overrides from source to spanner. They are written in"
                + " the following format: [{SourceTableName1.SourceColumnName1,"
                + " SourceTableName1.SpannerColumnName1}, {SourceTableName2.SourceColumnName1,"
                + " SourceTableName2.SpannerColumnName1}]Note that the SourceTableName should"
                + " remain the same in both the source and spanner pair. To override table names,"
                + " use tableOverrides.The example shows mapping SingerName to TalentName and"
                + " AlbumName to RecordName in Singers and Albums table respectively.")
    @Default.String("")
    String getColumnOverrides();

    void setColumnOverrides(String value);

    @TemplateParameter.Text(
        order = 11,
        optional = false,
        regexes = {"^[^ ;]*$"},
        description = "BigQuery dataset for reporting",
        helpText = "The BigQuery dataset ID where the validation results will be stored.",
        example = "validation_report_dataset")
    String getBigQueryDataset();

    void setBigQueryDataset(String value);

    @TemplateParameter.Text(
        order = 12,
        optional = true,
        regexes = {"^[^ ;]*$"},
        description = "Run ID for the validation job",
        helpText =
            "A unique identifier for the validation run. If not provided, the Dataflow Job Name"
                + " will be used.",
        example = "run_20230101_120000")
    String getRunId();

    void setRunId(String value);

    @TemplateParameter.GcsReadFile(
        order = 13,
        optional = true,
        description = "Custom jar location in Cloud Storage",
        helpText =
            "Custom jar location in Cloud Storage that contains the custom transformation logic for"
                + " processing records.")
    @Default.String("")
    String getTransformationJarPath();

    void setTransformationJarPath(String value);

    @TemplateParameter.Text(
        order = 14,
        optional = true,
        description = "Custom class name",
        helpText =
            "Fully qualified class name having the custom transformation logic. It is a"
                + " mandatory field in case transformationJarPath is specified")
    @Default.String("")
    String getTransformationClassName();

    void setTransformationClassName(String value);

    @TemplateParameter.Text(
        order = 15,
        optional = true,
        description = "Custom parameters for transformation",
        helpText =
            "String containing any custom parameters to be passed to the custom transformation"
                + " class.")
    @Default.String("")
    String getTransformationCustomParameters();

    void setTransformationCustomParameters(String value);
  }
