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

import com.google.cloud.spanner.Options;
import com.google.cloud.teleport.metadata.TemplateParameter;
import org.apache.beam.sdk.options.Default;

/** Interface used by the SourcedbToSpanner pipeline to accept user input. */
public interface SourceDbToSpannerOptions extends CommonTemplateOptions {
  String CASSANDRA_SOURCE_DIALECT = "CASSANDRA";
  String ASTRA_DB_SOURCE_DIALECT = "ASTRA_DB";
  String MYSQL_SOURCE_DIALECT = "MYSQL";
  String PG_SOURCE_DIALECT = "POSTGRESQL";

  @TemplateParameter.Enum(
      order = 1,
      optional = true,
      enumOptions = {
        @TemplateParameter.TemplateEnumOption(ASTRA_DB_SOURCE_DIALECT),
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
      optional = true,
      regexes = {"(^jdbc:mysql://.*|^jdbc:postgresql://.*|^gs://.*|^$)"},
      groupName = "Source",
      description = "Source database connection URL or shard config path.",
      helpText =
          "The URL to connect to the source database host. This can be either:"
              + " 1. A JDBC connection URL for a single source database, which"
              + " must contain the  host, port and source db name and can"
              + " optionally contain properties like  autoReconnect,"
              + " maxReconnects etc. Format: `jdbc:{mysql|postgresql}://{host}:{port}/{dbName}?{parameters}`."
              + " For example,`jdbc:mysql://127.4.5.30:3306/my-db?autoReconnect=true&maxReconnects=10&unicode=true&characterEncoding=UTF-8`."
              + " 2. A Cloud Storage path to a shard config file for sharded"
              + " migrations. For example, `gs://my-bucket/my-shard-config.yaml`."
              + " This parameter is required except for ASTRA_DB source.")
  @Default.String("")
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

  @TemplateParameter.Text(
      order = 21,
      optional = true,
      description = "Use Inserts instead of Upserts for spanner mutations.",
      helpText =
          "By default the pipeline uses Upserts to write rows to spanner. Which means existing rows would get overwritten. If InsertOnly mode is enabled, inserts would be used instead of upserts and existing rows won't be overwritten.")
  @Default.Boolean(false)
  Boolean getInsertOnlyModeForSpannerMutations();

  void setInsertOnlyModeForSpannerMutations(Boolean value);

  @TemplateParameter.Text(
      order = 22,
      optional = true,
      description = "BatchSize for Spanner Mutation.",
      helpText =
          "BatchSize in bytes for Spanner Mutations. if set less than 0, default of Apache Beam's SpannerIO is used, which is 1MB. Set this to 0 or 10, to disable batching mutations.")
  @Default.Long(-1)
  Long getBatchSizeForSpannerMutations();

  void setBatchSizeForSpannerMutations(Long value);

  @TemplateParameter.Enum(
      order = 23,
      enumOptions = {
        @TemplateParameter.TemplateEnumOption("LOW"),
        @TemplateParameter.TemplateEnumOption("MEDIUM"),
        @TemplateParameter.TemplateEnumOption("HIGH")
      },
      optional = true,
      description = "Priority for Spanner RPC invocations",
      helpText =
          "The request priority for Cloud Spanner calls. The value must be one of:"
              + " [`HIGH`,`MEDIUM`,`LOW`]. Defaults to `HIGH`.")
  @Default.Enum("HIGH")
  Options.RpcPriority getSpannerPriority();

  void setSpannerPriority(Options.RpcPriority value);

  @TemplateParameter.Text(
      order = 24,
      optional = true,
      description = "Table name overrides from source to spanner",
      regexes =
          "^\\[([[:space:]]*\\{[[:graph:]]+[[:space:]]*,[[:space:]]*[[:graph:]]+[[:space:]]*\\}[[:space:]]*(,[[:space:]]*)*)*\\]$",
      example = "[{Singers, Vocalists}, {Albums, Records}]",
      helpText =
          "These are the table name overrides from source to spanner. They are written in the"
              + "following format: [{SourceTableName1, SpannerTableName1}, {SourceTableName2, SpannerTableName2}]"
              + "This example shows mapping Singers table to Vocalists and Albums table to Records.")
  @Default.String("")
  String getTableOverrides();

  void setTableOverrides(String value);

  @TemplateParameter.Text(
      order = 25,
      optional = true,
      regexes =
          "^\\[([[:space:]]*\\{[[:space:]]*[[:graph:]]+\\.[[:graph:]]+[[:space:]]*,[[:space:]]*[[:graph:]]+\\.[[:graph:]]+[[:space:]]*\\}[[:space:]]*(,[[:space:]]*)*)*\\]$",
      description = "Column name overrides from source to spanner",
      example = "[{Singers.SingerName, Singers.TalentName}, {Albums.AlbumName, Albums.RecordName}]",
      helpText =
          "These are the column name overrides from source to spanner. They are written in the"
              + "following format: [{SourceTableName1.SourceColumnName1, SourceTableName1.SpannerColumnName1}, {SourceTableName2.SourceColumnName1, SourceTableName2.SpannerColumnName1}]"
              + "Note that the SourceTableName should remain the same in both the source and spanner pair. To override table names, use tableOverrides."
              + "The example shows mapping SingerName to TalentName and AlbumName to RecordName in Singers and Albums table respectively.")
  @Default.String("")
  String getColumnOverrides();

  void setColumnOverrides(String value);

  @TemplateParameter.Text(
      order = 26,
      optional = true,
      description = "File based overrides from source to spanner",
      helpText =
          "A file which specifies the table and the column name overrides from source to spanner.")
  @Default.String("")
  String getSchemaOverridesFilePath();

  void setSchemaOverridesFilePath(String value);

  @TemplateParameter.Text(
      order = 27,
      optional = true,
      description =
          "Hint for number of uniformization stages. Currently Applicable only for jdc based sources like MySql or PG. Leave 0 or default to disable uniformization. Set to -1 for a log(numPartition) number of stages.",
      helpText =
          "Hint for number of uniformization stages."
              + " Currently Applicable only for jdbc based sources like MySQL or PostgreSQL."
              + " Leave 0 or default to disable uniformization."
              + " Set to -1 for a log(numPartition) number of stages."
              + " If your source primary key space is uniformly distributed (for example an auto-incrementing key with sparse holes), it's based to leave it disabled."
              + " If your keyspace is not uniform, you might encounter a laggard VM in your dataflow run."
              + " In such a case, you can set it to -1 to enable uniformization."
              + " Manually setting it to values other than 0 or -1 would help you fine tune the tradeoff of the overhead added by uniformization stages and the  performance improvement due to better distribution of work.")
  @Default.Long(0)
  Long getUniformizationStageCountHint();

  void setUniformizationStageCountHint(Long value);

  @TemplateParameter.Text(
      order = 28,
      optional = true,
      description = "Astra DB token",
      helpText =
          "AstraDB token, ignored for non-AstraDB dialects. This token is used to automatically download the securebundle by the tempalte.")
  @Default.String("")
  String getAstraDBToken();

  void setAstraDBToken(String value);

  @TemplateParameter.Text(
      order = 29,
      optional = true,
      description = "Astra DB databaseID",
      helpText = "AstraDB databaseID, ignored for non-AstraDB dialects")
  @Default.String("")
  String getAstraDBDatabaseId();

  void setAstraDBDatabaseId(String value);

  @TemplateParameter.Text(
      order = 30,
      optional = true,
      description = "Astra DB keySpace",
      helpText = "AstraDB keySpace, ignored for non-AstraDB dialects")
  @Default.String("")
  String getAstraDBKeySpace();

  void setAstraDBKeySpace(String value);

  @TemplateParameter.Text(
      order = 31,
      optional = true,
      description = "Astra DB Region",
      helpText = "AstraDB region, ignored for non-AstraDB dialects")
  @Default.String("")
  String getAstraDBRegion();

  void setAstraDBRegion(String value);

  @TemplateParameter.Text(
      order = 32,
      optional = true,
      description = "Failure injection parameter",
      helpText = "Failure injection parameter. Only used for testing.")
  @Default.String("")
  String getFailureInjectionParameter();

  void setFailureInjectionParameter(String value);

  @TemplateParameter.Text(
      order = 33,
      optional = true,
      description =
          "Maximum commit delay time (in milliseconds) to optimize write throughput in Spanner. Reference https://cloud.google.com/spanner/docs/throughput-optimized-writes",
      helpText =
          "Maximum commit delay time to optimize write throughput in Spanner. Reference https://cloud.google.com/spanner/docs/throughput-optimized-writes."
              + "Set -1 to let spanner choose the default. Set to a positive value to override for best suited tradeoff of throughput vs latency."
              + "Defaults to -1.")
  @Default.Long(-1)
  Long getMaxCommitDelay();

  void setMaxCommitDelay(Long value);

  @TemplateParameter.GcsWriteFolder(
      order = 34,
      optional = true,
      description = "GCS directory for AVRO files",
      helpText = "This directory is used to write the AVRO files of the records read from source.",
      example = "gs://your-bucket/your-path")
  @Default.String("")
  String getGcsOutputDirectory();

  void setGcsOutputDirectory(String value);
}
