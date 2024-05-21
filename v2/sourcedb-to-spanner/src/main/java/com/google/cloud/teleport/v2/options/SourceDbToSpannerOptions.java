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
      groupName = "Source",
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
      groupName = "Source",
      description = "JDBC driver class name",
      helpText = "The JDBC driver class name.",
      example = "com.mysql.jdbc.Driver")
  String getJdbcDriverClassName();

  void setJdbcDriverClassName(String driverClassName);

  @TemplateParameter.Text(
      order = 3,
      regexes = {"(^jdbc:[a-zA-Z0-9/:@.]+$)"},
      groupName = "Source",
      description =
          "Connection URL to connect to the source database host. Port number and connection properties must be supplied separately.",
      helpText = "The JDBC connection URL string. For example, `jdbc:mysql://some-host`.")
  String getSourceHost();

  void setSourceHost(String host);

  @TemplateParameter.Text(
      order = 4,
      optional = false,
      regexes = {"(^[0-9]+$)"},
      groupName = "Source",
      description = "Port number of source database.",
      helpText = "Port Number of Source Database. For example, `3306`.")
  String getSourcePort();

  void setSourcePort(String port);

  /* TODO: (support Sharding, PG namespaces) */
  @TemplateParameter.Text(
      order = 5,
      groupName = "Source",
      description = "source database name.",
      helpText = "Name of the Source Database. For example, `person9`.")
  String getSourceDB();

  void setSourceDB(String db);

  @TemplateParameter.Text(
      order = 6,
      optional = true,
      regexes = {"^[a-zA-Z0-9_;!*&=@#-:\\/]+$"},
      groupName = "Source",
      description = "JDBC connection property string.",
      helpText =
          "Properties string to use for the JDBC connection. Format of the string must be"
              + " [propertyName=property;]*.",
      example = "unicode=true;characterEncoding=UTF-8")
  @Default.String("")
  String getSourceConnectionProperties();

  void setSourceConnectionProperties(String connectionProperties);

  @TemplateParameter.Text(
      order = 7,
      optional = true,
      regexes = {"^.+$"},
      groupName = "Source",
      description = "JDBC connection username.",
      helpText =
          "The username to be used for the JDBC connection. Can be passed in as a Base64-encoded"
              + " string encrypted with a Cloud KMS key.")
  @Default.String("")
  String getUsername();

  void setUsername(String username);

  @TemplateParameter.Password(
      order = 8,
      optional = true,
      groupName = "Source",
      description = "JDBC connection password.",
      helpText =
          "The password to be used for the JDBC connection. Can be passed in as a Base64-encoded"
              + " string encrypted with a Cloud KMS key.")
  @Default.String("")
  String getPassword();

  void setPassword(String password);

  @TemplateParameter.Text(
      order = 9,
      optional = true,
      groupName = "Source Parameters",
      description = "The name of a column of numeric type that will be used for partitioning.",
      helpText =
          "If this parameter is provided (along with `table`), JdbcIO reads the table in parallel"
              + " by executing multiple instances of the query on the same table (subquery) using"
              + " ranges. Currently, only Long partition columns are supported."
              + " The partition columns are expected to be the same in number as the tables")
  String getPartitionColumns();

  void setPartitionColumns(String partitionColumns);

  @TemplateParameter.Text(
      order = 10,
      optional = true,
      groupName = "Source Parameters",
      description = "Comma-separated names of the tables in the source database.",
      helpText = "Tables to read from using partitions.")
  String getTables();

  void setTables(String table);

  /* TODO(pipelineController) allow per table NumPartitions. */
  @TemplateParameter.Integer(
      order = 11,
      optional = true,
      groupName = "Source",
      description = "The number of partitions.",
      helpText =
          "The number of partitions. This, along with the lower and upper bound, form partitions"
              + " strides for generated WHERE clause expressions used to split the partition column"
              + " evenly. When the input is less than 1, the number is set to 1.")
  @Default.Integer(0) /* Use Auto Inference */
  Integer getNumPartitions();

  void setNumPartitions(Integer value);

  /* TODO(pipelineController) allow per table FetchSize. */
  @TemplateParameter.Integer(
      order = 12,
      optional = true,
      groupName = "Source",
      description = "Table Read Fetch Size.",
      helpText = "The fetch size of a single table read.")
  @Default.Integer(0) /* Use Beam Default */
  Integer getFetchSize();

  void setFetchSize(Integer numPartitions);

  @TemplateParameter.Text(
      order = 13,
      description = "Cloud Spanner Instance Id.",
      helpText = "The destination Cloud Spanner instance.")
  String getInstanceId();

  void setInstanceId(String value);

  @TemplateParameter.Text(
      order = 14,
      description = "Cloud Spanner Database Id.",
      helpText = "The destination Cloud Spanner database.")
  String getDatabaseId();

  void setDatabaseId(String value);

  @TemplateParameter.ProjectId(
      order = 15,
      description = "Cloud Spanner Project Id.",
      helpText = "This is the name of the Cloud Spanner project.")
  String getProjectId();

  void setProjectId(String projectId);

  @TemplateParameter.Text(
      order = 16,
      optional = true,
      description = "Cloud Spanner Endpoint to call",
      helpText = "The Cloud Spanner endpoint to call in the template.",
      example = "https://batch-spanner.googleapis.com")
  @Default.String("https://batch-spanner.googleapis.com")
  String getSpannerHost();

  void setSpannerHost(String value);

  @TemplateParameter.Text(
      order = 17,
      optional = true,
      description = "Source database columns to ignore",
      helpText =
          "A comma separated list of (table:column1;column2) to exclude from writing to Spanner",
      example = "table1:column1;column2,table2:column1")
  String getIgnoreColumns();

  void setIgnoreColumns(String value);

  @TemplateParameter.Integer(
      order = 18,
      optional = true,
      description = "Maximum number of connections to Source database per worker",
      helpText =
          "Configures the JDBC connection pool on each worker with maximum number of connections. Use a negative number for no limit.",
      example = "-1")
  @Default.Integer(0) // Take Dialect Specific default in the wrapper
  Integer getMaxConnections();

  void setMaxConnections(Integer value);

  @TemplateParameter.Boolean(
      order = 19,
      optional = true,
      description = "enable connection reconnects",
      helpText = "Enables the JDBC connection reconnects.",
      example = "10")
  @Default.Boolean(true) // Take Dialect Specific default in the wrapper.
  Boolean getReconnectsEnabled();

  void setReconnectsEnabled(Boolean value);

  @TemplateParameter.Integer(
      order = 20,
      optional = true,
      description = "Maximum number of connection reconnect attempts, if reconnects are enabled",
      helpText = "Configures the JDBC connection reconnect attempts.",
      example = "10")
  @Default.Integer(0) // Take Dialect Specific default in the wrapper.
  Integer getReconnectAttempts();

  void setReconnectAttempts(Integer value);

  @TemplateParameter.GcsReadFile(
      order = 21,
      optional = true,
      description =
          "Session File Path in Cloud Storage, to provide mapping information in the form of a session file",
      helpText =
          "Session file path in Cloud Storage that contains mapping information from"
              + " Spanner Migration Tool")
  String getSessionFilePath();

  void setSessionFilePath(String value);
}
