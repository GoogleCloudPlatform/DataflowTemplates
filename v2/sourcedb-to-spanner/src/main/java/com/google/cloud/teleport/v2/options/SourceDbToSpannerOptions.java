/*
 * Copyright (C) 2024 Google LLC
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

import com.google.cloud.teleport.metadata.TemplateParameter;
import org.apache.beam.sdk.options.Default;

/** Interface used by the SourcedbToSpanner pipeline to accept user input. */
public interface SourceDbToSpannerOptions extends CommonTemplateOptions {

  @TemplateParameter.Text(
      order = 1,
      optional = true,
      regexes = {"^.+$"},
      description = "Comma-separated Cloud Storage path(s) of the JDBC driver(s)",
      helpText = "The comma-separated list of driver JAR files.",
      example = "gs://your-bucket/driver_jar1.jar,gs://your-bucket/driver_jar2.jar")
  @Default.String("")
  String getJdbcDriverJars();

  void setJdbcDriverJars(String driverJar);

  @TemplateParameter.Text(
      order = 2,
      optional = true,
      regexes = {"^.+$"},
      description = "JDBC driver class name",
      helpText = "The JDBC driver class name.",
      example = "com.mysql.jdbc.Driver")
  @Default.String("com.mysql.jdbc.Driver")
  String getJdbcDriverClassName();

  void setJdbcDriverClassName(String driverClassName);

  @TemplateParameter.Text(
      order = 3,
      regexes = {"(^jdbc:mysql://[^\\n\\r]+$)"},
      groupName = "Source",
      description =
          "Connection URL to connect to the source database host. Must contain the host, port and source db name. Can optionally contain properties like autoReconnect, maxReconnects etc. Format: `jdbc:mysql://{host}:{port}/{dbName}?{parameters}`",
      helpText =
          "The JDBC connection URL string. For example, `jdbc:mysql://127.4.5.30:3306/my-db?autoReconnect=true&maxReconnects=10&unicode=true&characterEncoding=UTF-8`.")
  String getSourceDbURL();

  void setSourceDbURL(String url);

  @TemplateParameter.Text(
      order = 4,
      optional = true,
      regexes = {"^.+$"},
      description = "JDBC connection username.",
      helpText = "The username to be used for the JDBC connection.")
  @Default.String("")
  String getUsername();

  void setUsername(String username);

  @TemplateParameter.Password(
      order = 5,
      optional = true,
      description = "JDBC connection password.",
      helpText = "The password to be used for the JDBC connection.")
  @Default.String("")
  String getPassword();

  void setPassword(String password);

  @TemplateParameter.Text(
      order = 6,
      optional = true,
      description = "Comma-separated names of the tables in the source database.",
      helpText = "Tables to read from using partitions.")
  @Default.String("")
  String getTables();

  void setTables(String table);

  /* TODO(pipelineController) allow per table NumPartitions. */
  @TemplateParameter.Integer(
      order = 7,
      optional = true,
      description = "The number of partitions.",
      helpText =
          "The number of partitions. This, along with the lower and upper bound, form partitions"
              + " strides for generated WHERE clause expressions used to split the partition column"
              + " evenly. When the input is less than 1, the number is set to 1.")
  @Default.Integer(0) /* Use Auto Inference */
  Integer getNumPartitions();

  void setNumPartitions(Integer value);

  @TemplateParameter.Text(
      order = 8,
      groupName = "Target",
      description = "Cloud Spanner Instance Id.",
      helpText = "The destination Cloud Spanner instance.")
  String getInstanceId();

  void setInstanceId(String value);

  @TemplateParameter.Text(
      order = 9,
      groupName = "Target",
      regexes = {"^[a-z]([a-z0-9_-]{0,28})[a-z0-9]$"},
      description = "Cloud Spanner Database Id.",
      helpText = "The destination Cloud Spanner database.")
  String getDatabaseId();

  void setDatabaseId(String value);

  @TemplateParameter.ProjectId(
      order = 10,
      groupName = "Target",
      description = "Cloud Spanner Project Id.",
      helpText = "This is the name of the Cloud Spanner project.")
  String getProjectId();

  void setProjectId(String projectId);

  @TemplateParameter.Text(
      order = 11,
      optional = true,
      description = "Cloud Spanner Endpoint to call",
      helpText = "The Cloud Spanner endpoint to call in the template.",
      example = "https://batch-spanner.googleapis.com")
  @Default.String("https://batch-spanner.googleapis.com")
  String getSpannerHost();

  void setSpannerHost(String value);

  @TemplateParameter.Integer(
      order = 12,
      optional = true,
      description = "Maximum number of connections to Source database per worker",
      helpText =
          "Configures the JDBC connection pool on each worker with maximum number of connections. Use a negative number for no limit.",
      example = "-1")
  @Default.Integer(0) // Take Dialect Specific default in the wrapper
  Integer getMaxConnections();

  void setMaxConnections(Integer value);

  @TemplateParameter.GcsReadFile(
      order = 13,
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
      order = 14,
      description = "Dead letter queue directory",
      helpText = "This directory is used to dump the failed records in a migration.")
  String getDLQDirectory();

  void setDLQDirectory(String value);
}
