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
    name = "DeltaLake_To_Iceberg_Yaml",
    category = TemplateCategory.BATCH,
    type = Template.TemplateType.YAML,
    displayName = "Delta Lake to Iceberg (YAML)",
    description = "The Delta Lake to Iceberg template is a batch pipeline that reads data from a Delta Lake table and outputs the records to an Apache Iceberg table.",
    flexContainerName = "pipeline-yaml",
    yamlTemplateFile = "DeltaLakeToIceberg.yaml",
    filesToCopy = {"main.py", "requirements.txt", "options/deltalake_options.yaml", "options/iceberg_options.yaml"},
    documentation = "",
    contactInformation = "https://cloud.google.com/support",
    requirements = {"The Input Delta Lake table must exist and be accessible.",
      "The Output Iceberg table must exist or be created, and the warehouse must be accessible."
    },
    streaming = false,
    hidden = false)
public interface DeltaLakeToIcebergYaml {

  @TemplateParameter.Text(
      order = 1,
      name = "deltaLakeTable",
      optional = false,
      description = "A GCS path to the Delta Lake table.",
      helpText = "The GCS path to the Delta Lake table, e.g., gs://your-bucket/path/to/table.",
      example = "gs://your-bucket/path/to/table"
    )
  @Validation.Required
  String getDeltaLakeTable();

  @TemplateParameter.Text(
      order = 2,
      name = "deltaLakeHadoopConfig",
      optional = true,
      description = "Properties passed to Hadoop Configuration.",
      helpText = "A map of properties to pass to Hadoop Configuration, e.g. key-value pairs.",
      example = "{\"fs.gs.impl\": \"com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem\"}"
    )
  String getDeltaLakeHadoopConfig();

  @TemplateParameter.Text(
      order = 3,
      name = "table",
      optional = false,
      description = "A fully-qualified table identifier.",
      helpText = "A fully-qualified table identifier, e.g., my_dataset.my_table.",
      example = "my_dataset.my_table"
    )
  @Validation.Required
  String getTable();

  @TemplateParameter.Text(
      order = 4,
      name = "catalogName",
      optional = false,
      description = "Name of the catalog containing the table.",
      helpText = "The name of the Iceberg catalog that contains the table.",
      example = "my_hadoop_catalog"
    )
  @Validation.Required
  String getCatalogName();

  @TemplateParameter.Text(
      order = 5,
      name = "catalogProperties",
      optional = false,
      description = "Properties used to set up the Iceberg catalog.",
      helpText = "A map of properties for setting up the Iceberg catalog.",
      example = "{\"type\": \"hadoop\", \"warehouse\": \"gs://your-bucket/warehouse\"}"
    )
  @Validation.Required
  String getCatalogProperties();

  @TemplateParameter.Text(
      order = 6,
      name = "configProperties",
      optional = true,
      description = "Properties passed to the Hadoop Configuration.",
      helpText = "A map of properties to pass to the Hadoop Configuration.",
      example = "{\"fs.gs.impl\": \"com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem\"}"
    )
  String getConfigProperties();

  @TemplateParameter.Text(
      order = 7,
      name = "drop",
      optional = true,
      description = "A list of field names to drop from the input record before writing.",
      helpText = "A list of field names to drop. Mutually exclusive with 'keep' and 'only'.",
      example = "[\"field_to_drop_1\", \"field_to_drop_2\"]"
    )
  String getDrop();

  @TemplateParameter.Text(
      order = 8,
      name = "filter",
      optional = true,
      description = "An optional filter expression to apply to the input records.",
      helpText = "A filter expression to apply to records from the Iceberg table.",
      example = "age > 18"
    )
  String getFilter();

  @TemplateParameter.Text(
      order = 9,
      name = "keep",
      optional = true,
      description = "A list of field names to keep in the input record.",
      helpText = "A list of field names to keep. Mutually exclusive with 'drop' and 'only'.",
      example = "[\"field_to_keep_1\", \"field_to_keep_2\"]"
    )
  String getKeep();

  @TemplateParameter.Text(
      order = 10,
      name = "only",
      optional = true,
      description = "The name of a single record field that should be written.",
      helpText = "The name of a single field to write. Mutually exclusive with 'keep' and 'drop'.",
      example = "my_record_field"
    )
  String getOnly();

  @TemplateParameter.Text(
      order = 11,
      name = "partitionFields",
      optional = true,
      description = "Fields used to create a partition spec for new tables.",
      helpText = "A list of fields and transforms for partitioning, e.g., ['day(ts)', 'category'].",
      example = "[\"day(ts)\", \"bucket(id, 4)\"]"
    )
  String getPartitionFields();

  @TemplateParameter.Text(
      order = 12,
      name = "tableProperties",
      optional = true,
      description = "Iceberg table properties to be set on table creation.",
      helpText = "A map of Iceberg table properties to set when the table is created.",
      example = "{\"commit.retry.num-retries\": \"2\"}"
    )
  String getTableProperties();
}
