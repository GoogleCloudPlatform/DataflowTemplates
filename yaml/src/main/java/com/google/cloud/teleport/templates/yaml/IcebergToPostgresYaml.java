/*
 * Copyright (C) 2025 Google LLC
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
    name = "Iceberg_To_Postgres_Yaml",
    category = TemplateCategory.BATCH,
    type = Template.TemplateType.YAML,
    displayName = "Iceberg to Postgres (YAML)",
    description =
        "The Iceberg to Postgres template is a batch pipeline that reads data from an Iceberg table and outputs the records to a Postgres database table.",
    flexContainerName = "pipeline-yaml",
    yamlTemplateFile = "IcebergToPostgres.yaml",
    filesToCopy = {
      "main.py",
      "requirements.txt",
      "options/iceberg_options.yaml",
      "options/postgres_options.yaml"
    },
    documentation = "",
    contactInformation = "https://cloud.google.com/support",
    requirements = {
      "The Input Iceberg table must exist and be accessible through the provided catalog.",
      "The Output Postgres instance must exist and the target table must exist or be created."
    },
    streaming = false,
    hidden = false)
public interface IcebergToPostgresYaml {

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
      name = "filter",
      optional = true,
      description = "An optional filter expression to apply to the input records.",
      helpText = "A filter expression to apply to records from the Iceberg table.",
      example = "age > 18")
  String getFilter();

  @TemplateParameter.Text(
      order = 7,
      name = "keep",
      optional = true,
      description = "A list of field names to keep in the input record.",
      helpText = "A list of field names to keep. Mutually exclusive with 'drop' and 'only'.",
      example = "[\"field_to_keep_1\", \"field_to_keep_2\"]")
  String getKeep();

  @TemplateParameter.Text(
      order = 8,
      name = "jdbcUrl",
      optional = false,
      description = "Connection URL for the JDBC source/sink.",
      helpText = "The JDBC connection URL.",
      example = "jdbc:postgresql://your-host:5432/your-db")
  @Validation.Required
  String getJdbcUrl();

  @TemplateParameter.Text(
      order = 9,
      name = "username",
      optional = true,
      description = "Username for the JDBC connection.",
      helpText = "The database username.",
      example = "my_user")
  String getUsername();

  @TemplateParameter.Password(
      order = 10,
      name = "password",
      optional = true,
      description = "Password for the JDBC connection.",
      helpText = "The database password.",
      example = "my_secret_password")
  String getPassword();

  @TemplateParameter.Text(
      order = 11,
      name = "connectionProperties",
      optional = true,
      description = "JDBC connection properties.",
      helpText = "A semicolon-separated list of key-value pairs for the JDBC connection.",
      example = "key1=value1;key2=value2")
  String getConnectionProperties();

  @TemplateParameter.Text(
      order = 12,
      name = "postgresTable",
      optional = false,
      description = "The name of the table to write to.",
      helpText = "The name of the database table to write data to.",
      example = "public.my_destination_table")
  @Validation.Required
  String getPostgresTable();

  @TemplateParameter.Text(
      order = 13,
      name = "query",
      optional = true,
      description = "The SQL query to execute for reading data.",
      helpText = "The SQL query to execute on the source to extract data.",
      example = "SELECT * FROM my_table WHERE status = 'active'")
  String getQuery();

  @TemplateParameter.Integer(
      order = 14,
      name = "batchSize",
      optional = true,
      description = "The number of records to group for each write operation.",
      helpText = "The number of records to group together for each write.",
      example = "1000")
  @Default.Integer(1000)
  Integer getBatchSize();

  @TemplateParameter.Boolean(
      order = 15,
      name = "autosharding",
      optional = true,
      description = "If true, enables using a dynamically determined number of shards to write.",
      helpText = "If true, a dynamic number of shards will be used for writing.",
      example = "False")
  Boolean getAutosharding();
}
