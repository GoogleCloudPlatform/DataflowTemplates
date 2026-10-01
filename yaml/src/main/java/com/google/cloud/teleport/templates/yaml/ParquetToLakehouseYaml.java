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
import org.apache.beam.sdk.options.Validation;

@Template(
    name = "Parquet_To_Lakehouse_Yaml",
    category = TemplateCategory.BATCH,
    type = Template.TemplateType.YAML,
    displayName = "Parquet files to Lakehouse",
    description =
        "The Parquet files to Lakehouse template is a batch pipeline that matches Parquet files, optionally copies them to Google Cloud Storage, and adds them to a Lakehouse table.",
    flexContainerName = "pipeline-yaml",
    yamlTemplateFile = "ParquetToLakehouse.yaml",
    filesToCopy = {"main.py", "requirements.txt", "options/lakehouse_options.yaml"},
    documentation = "",
    contactInformation = "https://cloud.google.com/support",
    requirements = {
      "The Input Parquet files must exist and be accessible.",
      "The Output Lakehouse table must exist or be created, and the warehouse must be accessible."
    },
    streaming = false,
    hidden = false)
public interface ParquetToLakehouseYaml {

  @TemplateParameter.Text(
      order = 1,
      name = "filePattern",
      optional = false,
      description = "Input file pattern for Parquet files.",
      helpText =
          "A file pattern (glob) matching the input Parquet files to add to the Lakehouse table.",
      example = "gs://your-bucket/path/*.parquet")
  @Validation.Required
  String getFilePattern();

  @TemplateParameter.Text(
      order = 2,
      name = "gcsFilePath",
      optional = true,
      description = "Optional destination GCS directory path to copy files to.",
      helpText =
          "An optional Google Cloud Storage directory path to copy the matched Parquet files into before registering them in the Lakehouse table.",
      example = "gs://your-bucket/warehouse/data/")
  String getGcsFilePath();

  @TemplateParameter.Text(
      order = 3,
      name = "lakehouseTable",
      optional = false,
      description = "A fully-qualified table identifier.",
      helpText = "A fully-qualified table identifier, e.g., my_dataset.my_table.",
      example = "my_dataset.my_table")
  @Validation.Required
  String getLakehouseTable();

  @TemplateParameter.Text(
      order = 4,
      name = "lakehouseCatalogProperties",
      optional = false,
      description = "Properties used to set up the Lakehouse catalog.",
      helpText = "A map of properties for setting up the Lakehouse catalog.",
      example = "{\"type\": \"hadoop\", \"warehouse\": \"gs://your-bucket/warehouse\"}")
  @Validation.Required
  String getLakehouseCatalogProperties();

  @TemplateParameter.Text(
      order = 5,
      name = "lakehouseConfigProperties",
      optional = true,
      description = "Properties passed to the Hadoop Configuration.",
      helpText = "A map of properties to pass to the Hadoop Configuration.",
      example = "{\"fs.gs.impl\": \"com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem\"}")
  String getLakehouseConfigProperties();

  @TemplateParameter.Text(
      order = 6,
      name = "lakehousePartitionFields",
      optional = true,
      description = "Fields used to create a partition spec for new tables.",
      helpText = "A list of fields and transforms for partitioning, e.g., ['day(ts)', 'category'].",
      example = "[\"day(ts)\", \"bucket(id, 4)\"]")
  String getLakehousePartitionFields();

  @TemplateParameter.Text(
      order = 7,
      name = "lakehouseTableProperties",
      optional = true,
      description = "Lakehouse table properties to be set on table creation.",
      helpText = "A map of Lakehouse table properties to set when the table is created.",
      example = "{\"commit.retry.num-retries\": \"2\"}")
  String getLakehouseTableProperties();

  @TemplateParameter.Text(
      order = 8,
      name = "lakehouseLocationPrefix",
      optional = true,
      description = "An optional location prefix for data files.",
      helpText =
          "A location prefix used by the catalog when registering data files in the Lakehouse table.",
      example = "gs://your-bucket/warehouse")
  String getLakehouseLocationPrefix();

  @TemplateParameter.Text(
      order = 9,
      name = "errorPath",
      optional = false,
      description = "Output path for error records.",
      helpText = "The Cloud Storage path where failed records will be written in JSON format.",
      example = "gs://your-bucket/errors/error.json")
  @Validation.Required
  String getErrorPath();
}
