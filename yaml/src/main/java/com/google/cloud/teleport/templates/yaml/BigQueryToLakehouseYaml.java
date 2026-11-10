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
    name = "BigQuery_To_Lakehouse_Yaml",
    category = TemplateCategory.BATCH,
    type = Template.TemplateType.YAML,
    displayName = "BigQuery to Lakehouse",
    description =
        "The BigQuery to Lakehouse template is a batch pipeline that reads data from a BigQuery table and outputs the records to a Lakehouse table.",
    flexContainerName = "pipeline-yaml",
    yamlTemplateFile = "BigQueryToLakehouse.yaml",
    filesToCopy = {
      "main.py",
      "requirements.txt",
      "options/bigquery_options.yaml",
      "options/lakehouse_options.yaml"
    },
    documentation = "",
    contactInformation = "https://cloud.google.com/support",
    requirements = {
      "The Input BigQuery table must exist and be accessible.",
      "The Output Lakehouse table must exist or be created, and the warehouse must be accessible."
    },
    streaming = false,
    hidden = false)
public interface BigQueryToLakehouseYaml {

  @TemplateParameter.Text(
      order = 1,
      name = "table",
      optional = false,
      description = "BigQuery table",
      helpText =
          "BigQuery table location to write the output to or read from. The name  should be in the format <project>:<dataset>.<table_name>. For write,  the table's schema must match input objects.",
      example = "my-project:my_dataset.my_table")
  @Validation.Required
  String getTable();

  @TemplateParameter.Text(
      order = 2,
      name = "fields",
      optional = true,
      description = "A list of field names to read from the BigQuery table.",
      helpText =
          "A list of field (column) names to read from the BigQuery table. If not specified, all fields are read.",
      example = "[\"field1\", \"field2\"]")
  String getFields();

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
      optional = true,
      description = "Properties used to set up the Lakehouse catalog.",
      helpText = "A map of properties for setting up the Lakehouse catalog.",
      example = "{\"type\": \"hadoop\", \"warehouse\": \"gs://your-bucket/warehouse\"}")
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
      name = "lakehouseCatalogName",
      optional = false,
      description = "Name of the catalog containing the table.",
      helpText = "The name of the Lakehouse catalog that contains the table.",
      example = "my_hadoop_catalog")
  @Validation.Required
  String getLakehouseCatalogName();

  @TemplateParameter.Text(
      order = 9,
      name = "lakehouseDrop",
      optional = true,
      description = "A list of field names to drop from the input record before writing.",
      helpText = "A list of field names to drop. Mutually exclusive with 'keep' and 'only'.",
      example = "[\"field_to_drop_1\", \"field_to_drop_2\"]")
  String getLakehouseDrop();

  @TemplateParameter.Text(
      order = 10,
      name = "lakehouseKeep",
      optional = true,
      description = "A list of field names to keep in the input record.",
      helpText = "A list of field names to keep. Mutually exclusive with 'drop' and 'only'.",
      example = "[\"field_to_keep_1\", \"field_to_keep_2\"]")
  String getLakehouseKeep();

  @TemplateParameter.Text(
      order = 11,
      name = "lakehouseOnly",
      optional = true,
      description = "The name of a single record field that should be written.",
      helpText = "The name of a single field to write. Mutually exclusive with 'keep' and 'drop'.",
      example = "my_record_field")
  String getLakehouseOnly();

  @TemplateParameter.Text(
      order = 12,
      name = "lakehouseDistributionMode",
      optional = true,
      description = "Defines distribution of write data.",
      helpText =
          "Defines distribution of write data. Supported distributions are 'none' (don't shuffle rows, default) and 'hash' (shuffle rows by partition key before writing data).",
      example = "none")
  String getLakehouseDistributionMode();

  @TemplateParameter.Boolean(
      order = 13,
      name = "lakehouseAutosharding",
      optional = true,
      description =
          "Enables dynamic sharding to automatically adjust the number of parallel writers based on data volume.",
      helpText =
          "If true, enables dynamic sharding to automatically adjust the number of parallel writers based on data volume. Only available with 'hash' distribution mode.",
      example = "False")
  Boolean getLakehouseAutosharding();
}
