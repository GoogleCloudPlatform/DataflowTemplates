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
    name = "Postgres_To_Iceberg_Yaml",
    category = TemplateCategory.BATCH,
    type = Template.TemplateType.YAML,
    displayName = "Postgres to Iceberg (YAML)",
    description =
        "The Postgres to Iceberg template is a batch pipeline executes the user provided SQL query to read data from Postgres table and outputs the records to Iceberg table.",
    flexContainerName = "postgres-to-iceberg-yaml",
    yamlTemplateFile = "PostgresToIceberg.yaml",
    filesToCopy = {
      "template.yaml",
      "main.py",
      "requirements.txt",
      "options/postgres_options.yaml",
      "options/iceberg_options.yaml"
    },
    documentation = "",
    contactInformation = "https://cloud.google.com/support",
    requirements = {
      "The Input Postgres instance and table must exist.",
      "The Output Iceberg table need not exist, but the storage must exist and passed through catalog_properties."
    },
    streaming = false,
    hidden = false)
public interface PostgresToIcebergYaml {

  @TemplateParameter.Text(
      order = 1,
      name = "jdbcUrl",
      optional = false,
      description = "Connection URL for the JDBC source/sink.",
      helpText = "The JDBC connection URL.",
      example = "jdbc:postgresql://your-host:5432/your-db")
  @Validation.Required
  String getJdbcUrl();

  @TemplateParameter.Text(
      order = 2,
      name = "username",
      optional = true,
      description = "Username for the JDBC connection.",
      helpText = "The database username.",
      example = "my_user")
  String getUsername();

  @TemplateParameter.Password(
      order = 3,
      name = "password",
      optional = true,
      description = "Password for the JDBC connection.",
      helpText = "The database password.",
      example = "my_secret_password")
  String getPassword();

  @TemplateParameter.Text(
      order = 4,
      name = "driverClassName",
      optional = true,
      description =
          "The fully-qualified class name of the JDBC driver. Default: org.postgresql.Driver",
      helpText = "The fully-qualified class name of the JDBC driver to use.",
      example = "org.postgresql.Driver")
  @Default.String("org.postgresql.Driver")
  String getDriverClassName();

  @TemplateParameter.Text(
      order = 5,
      name = "driverJars",
      optional = true,
      description = "Comma-separated GCS paths of the JDBC driver jars.",
      helpText = "A comma-separated list of GCS paths to the JDBC driver JAR files.",
      example = "gs://your-bucket/postgresql-42.2.23.jar")
  String getDriverJars();

  @TemplateParameter.Text(
      order = 6,
      name = "connectionProperties",
      optional = true,
      description = "JDBC connection properties.",
      helpText = "A semicolon-separated list of key-value pairs for the JDBC connection.",
      example = "key1=value1;key2=value2")
  String getConnectionProperties();

  @TemplateParameter.Text(
      order = 7,
      name = "connectionInitSql",
      optional = true,
      description = "A list of SQL statements to execute upon connection initialization.",
      helpText = "A list of SQL statements to execute when a new connection is established.",
      example = "[\"SET TIME ZONE UTC\"]")
  String getConnectionInitSql();

  @TemplateParameter.Text(
      order = 8,
      name = "jdbcType",
      optional = true,
      description = "Type of JDBC source. Default: postgres.",
      helpText =
          "Specifies the type of JDBC source. An appropriate default driver will be packaged.",
      example = "postgres")
  @Default.String("postgres")
  String getJdbcType();

  @TemplateParameter.Text(
      order = 9,
      name = "location",
      optional = true,
      description = "The name of the table to read from.",
      helpText = "The name of the database table to read data from.",
      example = "public.my_table")
  String getLocation();

  @TemplateParameter.Text(
      order = 10,
      name = "readQuery",
      optional = true,
      description = "The SQL query to execute for reading data.",
      helpText = "The SQL query to execute on the source to extract data.",
      example = "SELECT * FROM my_table WHERE status = 'active'")
  String getReadQuery();

  @TemplateParameter.Text(
      order = 11,
      name = "partitionColumn",
      optional = true,
      description = "The name of a numeric column to be used for partitioning.",
      helpText = "The name of a numeric column that will be used for partitioning the data.",
      example = "id")
  String getPartitionColumn();

  @TemplateParameter.Integer(
      order = 12,
      name = "numPartitions",
      optional = true,
      description = "The number of partitions to divide the data into.",
      helpText = "The number of partitions to create for parallel reading.",
      example = "10")
  Integer getNumPartitions();

  @TemplateParameter.Integer(
      order = 13,
      name = "fetchSize",
      optional = true,
      description = "The number of rows to fetch from the database at a time.",
      helpText =
          "The number of rows to fetch per database call. It should ONLY be used if the default value throws memory errors.",
      example = "50000")
  Integer getFetchSize();

  @TemplateParameter.Boolean(
      order = 14,
      name = "disableAutoCommit",
      optional = true,
      description = "Whether to disable auto-commit on read.",
      helpText =
          "Whether to disable auto-commit on read. Required for some databases like Postgres.",
      example = "True")
  Boolean getDisableAutoCommit();

  @TemplateParameter.Boolean(
      order = 15,
      name = "outputParallelization",
      optional = true,
      description = "Whether to reshuffle the PCollection to distribute results to all workers.",
      helpText = "If true, the resulting PCollection will be reshuffled.",
      example = "True")
  Boolean getOutputParallelization();

  @TemplateParameter.Text(
      order = 16,
      name = "table",
      optional = false,
      description = "A fully-qualified table identifier.",
      helpText = "A fully-qualified table identifier, e.g., my_dataset.my_table.",
      example = "my_dataset.my_table")
  @Validation.Required
  String getTable();

  @TemplateParameter.Text(
      order = 17,
      name = "catalogName",
      optional = false,
      description = "Name of the catalog containing the table.",
      helpText = "The name of the Iceberg catalog that contains the table.",
      example = "my_hadoop_catalog")
  @Validation.Required
  String getCatalogName();

  @TemplateParameter.Text(
      order = 18,
      name = "catalogProperties",
      optional = false,
      description = "Properties used to set up the Iceberg catalog.",
      helpText = "A map of properties for setting up the Iceberg catalog.",
      example = "{\"type\": \"hadoop\", \"warehouse\": \"gs://your-bucket/warehouse\"}")
  @Validation.Required
  String getCatalogProperties();

  @TemplateParameter.Text(
      order = 19,
      name = "configProperties",
      optional = true,
      description = "Properties passed to the Hadoop Configuration.",
      helpText = "A map of properties to pass to the Hadoop Configuration.",
      example = "{\"fs.gs.impl\": \"com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem\"}")
  String getConfigProperties();

  @TemplateParameter.Text(
      order = 20,
      name = "drop",
      optional = true,
      description = "A list of field names to drop from the input record before writing.",
      helpText = "A list of field names to drop. Mutually exclusive with 'keep' and 'only'.",
      example = "[\"field_to_drop_1\", \"field_to_drop_2\"]")
  String getDrop();

  @TemplateParameter.Text(
      order = 21,
      name = "keep",
      optional = true,
      description = "A list of field names to keep in the input record.",
      helpText = "A list of field names to keep. Mutually exclusive with 'drop' and 'only'.",
      example = "[\"field_to_keep_1\", \"field_to_keep_2\"]")
  String getKeep();

  @TemplateParameter.Text(
      order = 22,
      name = "only",
      optional = true,
      description = "The name of a single record field that should be written.",
      helpText = "The name of a single field to write. Mutually exclusive with 'keep' and 'drop'.",
      example = "my_record_field")
  String getOnly();

  @TemplateParameter.Text(
      order = 23,
      name = "partitionFields",
      optional = true,
      description = "Fields used to create a partition spec for new tables.",
      helpText = "A list of fields and transforms for partitioning, e.g., ['day(ts)', 'category'].",
      example = "[\"day(ts)\", \"bucket(id, 4)\"]")
  String getPartitionFields();

  @TemplateParameter.Text(
      order = 24,
      name = "tableProperties",
      optional = true,
      description = "Iceberg table properties to be set on table creation.",
      helpText = "A map of Iceberg table properties to set when the table is created.",
      example = "{\"commit.retry.num-retries\": \"2\"}")
  String getTableProperties();
}
