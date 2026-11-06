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
    name = "Iceberg_To_Bigtable_Yaml",
    category = TemplateCategory.BATCH,
    type = Template.TemplateType.YAML,
    displayName = "Iceberg to Bigtable (YAML)",
    description =
        "The Iceberg to Bigtable template is a batch pipeline that reads data from an Iceberg table, executes a user-defined mapping, and writes the resulting records to Bigtable. Any errors which occur in the transformation of the data are written to Cloud Storage in JSON format.",
    flexContainerName = "pipeline-yaml",
    yamlTemplateFile = "IcebergToBigtable.yaml",
    filesToCopy = {
      "main.py",
      "requirements.txt",
      "options/iceberg_options.yaml",
      "options/maptofields_options.yaml",
      "options/bigtable_options.yaml",
      "options/json_options.yaml"
    },
    documentation =
        "https://cloud.google.com/dataflow/docs/guides/templates/provided-yaml/iceberg-to-bigtable",
    contactInformation = "https://cloud.google.com/support",
    requirements = {
      "The input Iceberg table must exist and be accessible through the provided catalog.",
      "The output Bigtable table must exist.",
      "The Cloud Storage directory for error records must be accessible."
    },
    streaming = false,
    hidden = false)
public interface IcebergToBigtableYaml {

  @TemplateParameter.Text(
      order = 1,
      name = "table",
      optional = false,
      description = "A fully-qualified table identifier.",
      helpText = "A fully-qualified table identifier, e.g., my_dataset.my_table.",
      example = "my_dataset.my_table")
  @Validation.Required
  String getTable();

  @TemplateParameter.Text(
      order = 2,
      name = "catalogName",
      optional = false,
      description = "Name of the catalog containing the table.",
      helpText = "The name of the Iceberg catalog that contains the table.",
      example = "my_hadoop_catalog")
  @Validation.Required
  String getCatalogName();

  @TemplateParameter.Text(
      order = 3,
      name = "catalogProperties",
      optional = false,
      description = "Properties used to set up the Iceberg catalog.",
      helpText = "A map of properties for setting up the Iceberg catalog.",
      example = "{\"type\": \"hadoop\", \"warehouse\": \"gs://your-bucket/warehouse\"}")
  @Validation.Required
  String getCatalogProperties();

  @TemplateParameter.Text(
      order = 4,
      name = "configProperties",
      optional = true,
      description = "Properties passed to the Hadoop Configuration.",
      helpText = "A map of properties to pass to the Hadoop Configuration.",
      example = "{\"fs.gs.impl\": \"com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem\"}")
  String getConfigProperties();

  @TemplateParameter.Text(
      order = 5,
      name = "drop",
      optional = true,
      description = "A list of field names to drop from the input record before writing.",
      helpText = "A list of field names to drop. Mutually exclusive with 'keep' and 'only'.",
      example = "[\"field_to_drop_1\", \"field_to_drop_2\"]")
  String getDrop();

  @TemplateParameter.Text(
      order = 6,
      name = "keep",
      optional = true,
      description = "A list of field names to keep in the input record.",
      helpText = "A list of field names to keep. Mutually exclusive with 'drop' and 'only'.",
      example = "[\"field_to_keep_1\", \"field_to_keep_2\"]")
  String getKeep();

  @TemplateParameter.Text(
      order = 7,
      name = "filter",
      optional = true,
      description = "An optional filter expression to apply to the input records.",
      helpText = "A filter expression to apply to records from the Iceberg table.",
      example = "age > 18")
  String getFilter();

  @TemplateParameter.Text(
      order = 8,
      name = "language",
      optional = true,
      description = "Language used to define the expressions.",
      helpText =
          "The language used to define (and execute) the expressions and/or  callables in fields. Defaults to generic.",
      example = "python")
  @Default.String("generic")
  String getLanguage();

  @TemplateParameter.Text(
      order = 9,
      name = "fields",
      optional = false,
      description = "Field mapping configuration",
      helpText =
          "The output fields to compute, each mapping to the expression or callable that creates them.",
      example = "{\"key\": {\"expression\": \"key.encode('utf-8')\", \"output_type\": \"bytes\"}}")
  @Validation.Required
  String getFields();

  @TemplateParameter.Text(
      order = 10,
      name = "projectId",
      optional = false,
      description = "Bigtable project ID",
      helpText = "The Google Cloud project ID of the Bigtable instance.",
      example = "my-gcp-project")
  @Validation.Required
  String getProjectId();

  @TemplateParameter.Text(
      order = 11,
      name = "instanceId",
      optional = false,
      description = "Bigtable instance ID",
      helpText = "The Bigtable instance ID.",
      example = "my-bigtable-instance")
  @Validation.Required
  String getInstanceId();

  @TemplateParameter.Text(
      order = 12,
      name = "tableId",
      optional = false,
      description = "Bigtable output table",
      helpText = "Bigtable table ID to write the output to.",
      example = "my-bigtable-table")
  @Validation.Required
  String getTableId();

  @TemplateParameter.Text(
      order = 13,
      name = "errorPath",
      optional = false,
      description = "Output path for error records.",
      helpText = "The Cloud Storage path where failed records will be written in JSON format.",
      example = "gs://your-bucket/errors/error.json")
  @Validation.Required
  String getErrorPath();
}
