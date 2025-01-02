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
  String CASSANDRA_SOURCE_DIALECT = "CASSANDRA";
  String MYSQL_SOURCE_DIALECT = "MYSQL";
  String PG_SOURCE_DIALECT = "POSTGRESQL";

  @TemplateParameter.Enum(
      order = 1,
      optional = true,
      enumOptions = {
        @TemplateParameter.TemplateEnumOption(CASSANDRA_SOURCE_DIALECT),
        @TemplateParameter.TemplateEnumOption(MYSQL_SOURCE_DIALECT),
        @TemplateParameter.TemplateEnumOption(PG_SOURCE_DIALECT)
      },
      description = "Dialect of the source database",
      helpText = "Possible values are `CASSANDRA`, `MYSQL` and `POSTGRESQL`.")
  @Default.String("MYSQL")
  String getSourceDbDialect();

  void setSourceDbDialect(String sourceDatabaseDialect);

  @TemplateParameter.Text(
      order = 2,
      optional = true,
      regexes = {"^.+$"},
      description = "Comma-separated Cloud Storage path(s) of the JDBC driver(s)",
      helpText = "The comma-separated list of driver JAR files.",
      example = "gs://your-bucket/driver_jar1.jar,gs://your-bucket/driver_jar2.jar")
  @Default.String("")
  String getJdbcDriverJars();

  void setJdbcDriverJars(String driverJar);

  @TemplateParameter.Text(
      order = 3,
      optional = true,
      regexes = {"^.+$"},
      description = "JDBC driver class name",
      helpText = "The JDBC driver class name.",
      example = "com.mysql.jdbc.Driver")
  @Default.String("com.mysql.jdbc.Driver")
  String getJdbcDriverClassName();

  void setJdbcDriverClassName(String driverClassName);

  @TemplateParameter.Text(
      order = 4,
      regexes = {"(^jdbc:mysql://.*|^jdbc:postgresql://.*|^gs://.*)"},
      groupName = "Source",
      description =
          "URL to connect to the source database host. It can be either of "
              + "1. The JDBC connection URL - which must contain the host, port and source db name and can optionally contain properties like autoReconnect, maxReconnects etc. Format: `jdbc:{mysql|postgresql}://{host}:{port}/{dbName}?{parameters}`"
              + "2. The shard config path",
      helpText =
          "The JDBC connection URL string. For example, `jdbc:mysql://127.4.5.30:3306/my-db?autoReconnect=true&maxReconnects=10&unicode=true&characterEncoding=UTF-8` or the shard config")
  String getSourceConfigURL();

  void setSourceConfigURL(String url);

  @TemplateParameter.Text(
      order = 5,
      optional = true,
      regexes = {"^.+$"},
      description = "JDBC connection username.",
      helpText = "The username to be used for the JDBC connection.")
  @Default.String("")
  String getUsername(); // Make optional

  void setUsername(String username);

  @TemplateParameter.Password(
      order = 6,
      optional = true,
      description = "JDBC connection password.",
      helpText = "The password to be used for the JDBC connection.")
  @Default.String("")
  String getPassword(); // make optional

  void setPassword(String password);

  @TemplateParameter.Text(
      order = 7,
      optional = true,
      description = "colon-separated names of the tables in the source database.",
      helpText = "Tables to migrate from source.")
  @Default.String("")
  String getTables();

  void setTables(String table);

  /* TODO(pipelineController) allow per table NumPartitions. */
  @TemplateParameter.Integer(
      order = 8,
      optional = true,
      description = "The number of partitions.",
      helpText =
          "The number of partitions. This, along with the lower and upper bound, form partitions"
              + " strides for generated WHERE clause expressions used to split the partition column"
              + " evenly. When the input is less than 1, the number is set to 1.")
  @Default.Integer(0) /* Use Auto Inference */
  Integer getNumPartitions();

  void setNumPartitions(Integer value);

  @TemplateParameter.Integer(
      order = 9,
      optional = true,
      description = "The number of rows to fetch per page read for JDBC source.",
      helpText =
          "The number of rows to fetch per page read for JDBC source. If not set, the default of JdbcIO of 50_000 rows gets used. If source dialect is Mysql, please see the note below."
              + " This ultimately translated to Statement.setFetchSize call at Jdbc layer. It should ONLY be used if the default value throws memory errors."
              + "Note for MySql Source:  FetchSize is ignored by the Mysql connector unless, `useCursorFetch=true` is also part of the connection properties."
              + "In case, the fetchSize parameter is explicitly set, for MySql dialect, the pipeline will add `useCursorFetch=true` to the connection properties by default.")
  Integer getFetchSize();

  void setFetchSize(Integer value);

  @TemplateParameter.Text(
      order = 10,
      groupName = "Target",
      description = "Cloud Spanner Instance Id.",
      helpText = "The destination Cloud Spanner instance.")
  String getInstanceId();

  void setInstanceId(String value);

  @TemplateParameter.Text(
      order = 11,
      groupName = "Target",
      regexes = {"^[a-z]([a-z0-9_-]{0,28})[a-z0-9]$"},
      description = "Cloud Spanner Database Id.",
      helpText = "The destination Cloud Spanner database.")
  String getDatabaseId();

  void setDatabaseId(String value);

  @TemplateParameter.ProjectId(
      order = 12,
      groupName = "Target",
      description = "Cloud Spanner Project Id.",
      helpText = "This is the name of the Cloud Spanner project.")
  String getProjectId();

  void setProjectId(String projectId);

  @TemplateParameter.Text(
      order = 13,
      optional = true,
      description = "Cloud Spanner Endpoint to call",
      helpText = "The Cloud Spanner endpoint to call in the template.",
      example = "https://batch-spanner.googleapis.com")
  @Default.String("https://batch-spanner.googleapis.com")
  String getSpannerHost();

  void setSpannerHost(String value);

  @TemplateParameter.Integer(
      order = 14,
      optional = true,
      description = "Maximum number of connections to Source database per worker",
      helpText =
          "Configures the JDBC connection pool on each worker with maximum number of connections. Use a negative number for no limit.",
      example = "-1")
  @Default.Integer(0) // Take Dialect Specific default in the wrapper
  Integer getMaxConnections();

  void setMaxConnections(Integer value);

  @TemplateParameter.GcsReadFile(
      order = 15,
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
      order = 16,
      description = "Output directory for failed/skipped/filtered events",
      helpText =
          "This directory is used to dump the failed/skipped/filtered records in a migration.")
  String getOutputDirectory();

  void setOutputDirectory(String value);

  @TemplateParameter.GcsReadFile(
      order = 17,
      optional = true,
      description = "Custom jar location in Cloud Storage",
      helpText =
          "Custom jar location in Cloud Storage that contains the custom transformation logic for processing records.")
  @Default.String("")
  String getTransformationJarPath();

  void setTransformationJarPath(String value);

  @TemplateParameter.Text(
      order = 18,
      optional = true,
      description = "Custom class name",
      helpText =
          "Fully qualified class name having the custom transformation logic. It is a"
              + " mandatory field in case transformationJarPath is specified")
  @Default.String("")
  String getTransformationClassName();

  void setTransformationClassName(String value);

  @TemplateParameter.Text(
      order = 19,
      optional = true,
      description = "Custom parameters for transformation",
      helpText =
          "String containing any custom parameters to be passed to the custom transformation class.")
  @Default.String("")
  String getTransformationCustomParameters();

  void setTransformationCustomParameters(String value);

  @TemplateParameter.Text(
      order = 20,
      optional = true,
      description = "Namespace",
      helpText =
          "Namespace to exported. For PostgreSQL, if no namespace is provided, 'public' will be used")
  @Default.String("")
  String getNamespace();

  void setNamespace(String value);
}
