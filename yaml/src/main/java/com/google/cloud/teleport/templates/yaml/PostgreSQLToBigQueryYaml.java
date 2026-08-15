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
    name = "PostgreSQL_to_BigQuery_Yaml",
    category = TemplateCategory.BATCH,
    type = Template.TemplateType.YAML,
    displayName = "PostgreSQL to BigQuery (YAML)",
    description =
        "The PostgreSQL to BigQuery template is a batch pipeline that copies data from a PostgreSQL table into an existing BigQuery table. This pipeline uses JDBC to connect to PostgreSQL.",
    flexContainerName = "postgresql-to-bigquery-yaml",
    yamlTemplateFile = "PostgreSQLToBigQuery.yaml",
    filesToCopy = {
      "main.py",
      "requirements.txt",
      "options/postgres_options.yaml",
      "options/bigquery_options.yaml"
    },
    documentation = "",
    contactInformation = "https://cloud.google.com/support",
    requirements = {
      "The PostgreSQL database must be accessible from the Dataflow workers.",
      "The target BigQuery dataset and table must exist."
    },
    streaming = false,
    hidden = false)
public interface PostgreSQLToBigQueryYaml {

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
      name = "connectionProperties",
      optional = true,
      description = "JDBC connection properties.",
      helpText = "A semicolon-separated list of key-value pairs for the JDBC connection.",
      example = "key1=value1;key2=value2")
  String getConnectionProperties();

  @TemplateParameter.Text(
      order = 5,
      name = "postgresTable",
      optional = true,
      description = "The name of the Postgres table.",
      helpText = "The name of the database table.",
      example = "public.my_table")
  String getPostgresTable();

  @TemplateParameter.Text(
      order = 6,
      name = "query",
      optional = true,
      description = "The SQL query/statement to execute.",
      helpText = "The SQL query/statement to execute on the source/sink.",
      example = "SELECT * FROM my_table WHERE status = 'active'")
  String getQuery();

  @TemplateParameter.Text(
      order = 7,
      name = "partitionColumn",
      optional = true,
      description = "The name of a numeric column to be used for partitioning.",
      helpText = "The name of a numeric column that will be used for partitioning the data.",
      example = "id")
  String getPartitionColumn();

  @TemplateParameter.Integer(
      order = 8,
      name = "numPartitions",
      optional = true,
      description = "The number of partitions to divide the data into.",
      helpText = "The number of partitions to create for parallel reading.",
      example = "10")
  Integer getNumPartitions();

  @TemplateParameter.Integer(
      order = 9,
      name = "fetchSize",
      optional = true,
      description = "The number of rows to fetch from the database at a time.",
      helpText =
          "The number of rows to fetch per database call. It should ONLY be used if the default value throws memory errors.",
      example = "50000")
  Integer getFetchSize();

  @TemplateParameter.Boolean(
      order = 10,
      name = "disableAutoCommit",
      optional = true,
      description = "Whether to disable auto-commit on read.",
      helpText =
          "Whether to disable auto-commit on read. Required for some databases like Postgres.",
      example = "True")
  Boolean getDisableAutoCommit();

  @TemplateParameter.Boolean(
      order = 11,
      name = "outputParallelization",
      optional = true,
      description = "Whether to reshuffle the PCollection to distribute results to all workers.",
      helpText = "If true, the resulting PCollection will be reshuffled.",
      example = "True")
  Boolean getOutputParallelization();

  @TemplateParameter.Text(
      order = 12,
      name = "table",
      optional = false,
      description = "BigQuery table",
      helpText =
          "BigQuery table location to write the output to or read from. The name  should be in the format <project>:<dataset>.<table_name>`. For write,  the table's schema must match input objects.",
      example = "")
  @Validation.Required
  String getTable();

  @TemplateParameter.Text(
      order = 13,
      name = "createDisposition",
      optional = true,
      description = "How to create",
      helpText =
          "Specifies whether a table should be created if it does not exist.  Valid inputs are 'Never' and 'IfNeeded'.",
      example = "")
  @Default.String("CREATE_IF_NEEDED")
  String getCreateDisposition();

  @TemplateParameter.Text(
      order = 14,
      name = "writeDisposition",
      optional = true,
      description = "How to write",
      helpText =
          "How to specify if a write should append to an existing table, replace the table, or verify that the table is empty. Note that the my_dataset being written to must already exist. Unbounded collections can only be written using 'WRITE_EMPTY' or 'WRITE_APPEND'.",
      example = "")
  @Default.String("WRITE_APPEND")
  String getWriteDisposition();

  @TemplateParameter.Integer(
      order = 15,
      name = "numStreams",
      optional = true,
      description = "Number of streams for BigQuery Storage Write API",
      helpText =
          "Number of streams defines the parallelism of the BigQueryIO’s Write  transform and roughly corresponds to the number of Storage Write API’s  streams which will be used by the pipeline. See https://cloud.google.com/blog/products/data-analytics/streaming-data-into-bigquery-using-storage-write-api for the recommended values. The default value is 1.",
      example = "")
  @Default.Integer(1)
  Integer getNumStreams();
}
