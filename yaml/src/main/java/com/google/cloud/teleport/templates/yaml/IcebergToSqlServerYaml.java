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
    name = "Iceberg_To_SqlServer_Yaml",
    category = TemplateCategory.BATCH,
    type = Template.TemplateType.YAML,
    displayName = "Iceberg to SqlServer (YAML)",
    description =
        "The Iceberg to SqlServer template is a batch pipeline that reads data from an Iceberg table and outputs the records to a SqlServer database table.",
    flexContainerName = "iceberg-to-sqlserver-yaml",
    yamlTemplateFile = "IcebergToSqlServer.yaml",
    filesToCopy = {
      "template.yaml",
      "main.py",
      "requirements.txt",
      "options/iceberg_options.yaml",
      "options/sqlserver_options.yaml"
    },
    documentation = "",
    contactInformation = "https://cloud.google.com/support",
    requirements = {
      "The Input Iceberg table must exist and be accessible through the provided catalog.",
      "The Output SqlServer instance must exist and the target table must exist or be created."
    },
    streaming = false,
    hidden = false)
public interface IcebergToSqlServerYaml {

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
      order = 11,
      name = "jdbcUrl",
      optional = false,
      description = "Connection URL for the JDBC source/sink.",
      helpText = "The JDBC connection URL.",
      example = "jdbc:sqlserver://localhost:12345;databaseName=your-db")
  @Validation.Required
  String getJdbcUrl();

  @TemplateParameter.Text(
      order = 12,
      name = "username",
      optional = true,
      description = "Username for the JDBC connection.",
      helpText = "The database username.",
      example = "my_user")
  String getUsername();

  @TemplateParameter.Password(
      order = 13,
      name = "password",
      optional = true,
      description = "Password for the JDBC connection.",
      helpText = "The database password.",
      example = "my_secret_password")
  String getPassword();

  @TemplateParameter.Text(
      order = 14,
      name = "driverClassName",
      optional = true,
      description =
          "The fully-qualified class name of the JDBC driver. Default: com.microsoft.sqlserver.jdbc.SQLServerDriverr",
      helpText = "The fully-qualified class name of the JDBC driver to use.",
      example = "com.microsoft.sqlserver.jdbc.SQLServerDriver")
  @Default.String("com.microsoft.sqlserver.jdbc.SQLServerDriver")
  String getDriverClassName();

  @TemplateParameter.Text(
      order = 15,
      name = "driverJars",
      optional = true,
      description = "Comma-separated GCS paths of the JDBC driver jars.",
      helpText = "A comma-separated list of GCS paths to the JDBC driver JAR files.",
      example = "gs://your-bucket/mssql-jdbc-12.2.0.jre11.jar")
  String getDriverJars();

  @TemplateParameter.Text(
      order = 16,
      name = "connectionProperties",
      optional = true,
      description = "JDBC connection properties.",
      helpText = "A semicolon-separated list of key-value pairs for the JDBC connection.",
      example = "key1=value1;key2=value2")
  String getConnectionProperties();

  @TemplateParameter.Text(
      order = 17,
      name = "connectionInitSql",
      optional = true,
      description = "A list of SQL statements to execute upon connection initialization.",
      helpText = "A list of SQL statements to execute when a new connection is established.",
      example = "[\"SET TIME ZONE UTC\"]")
  String getConnectionInitSql();

  @TemplateParameter.Text(
      order = 18,
      name = "jdbcType",
      optional = true,
      description = "Type of JDBC source. Default: mssql.",
      helpText =
          "Specifies the type of JDBC source. An appropriate default driver will be packaged.",
      example = "mssql")
  @Default.String("mssql")
  String getJdbcType();

  @TemplateParameter.Text(
      order = 19,
      name = "location",
      optional = false,
      description = "The name of the table to write to.",
      helpText = "The name of the database table to write data to.",
      example = "public.my_destination_table")
  @Validation.Required
  String getLocation();

  @TemplateParameter.Text(
      order = 20,
      name = "writeStatement",
      optional = true,
      description = "The SQL statement to use for inserting records.",
      helpText = "The SQL query for inserting records, with placeholders for values.",
      example = "INSERT INTO my_table (col1, col2) VALUES(?, ?)")
  String getWriteStatement();

  @TemplateParameter.Integer(
      order = 21,
      name = "batchSize",
      optional = true,
      description = "The number of records to group for each write operation.",
      helpText = "The number of records to group together for each write.",
      example = "1000")
  @Default.Integer(1000)
  Integer getBatchSize();

  @TemplateParameter.Boolean(
      order = 22,
      name = "autosharding",
      optional = true,
      description = "If true, enables using a dynamically determined number of shards to write.",
      helpText = "If true, a dynamic number of shards will be used for writing.",
      example = "False")
  Boolean getAutosharding();
}
