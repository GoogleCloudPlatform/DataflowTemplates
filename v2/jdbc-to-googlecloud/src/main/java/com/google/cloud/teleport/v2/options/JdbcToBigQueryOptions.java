/*
 * Copyright (C) 2022 Google LLC
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

/** Interface used by the JdbcToBigQuery pipeline to accept user input. */
public interface JdbcToBigQueryOptions
    extends CommonTemplateOptions, BigQueryStorageApiBatchOptions {

  @TemplateParameter.Text(
      order = 1,
      optional = false,
      regexes = {"^.+$"},
      description = "Comma-separated Cloud Storage path(s) of the JDBC driver(s)",
      helpText = "The comma-separated list of driver JAR files.",
      example = "gs://your-bucket/driver_jar1.jar,gs://your-bucket/driver_jar2.jar")
  String getDriverJars();

  void setDriverJars(String driverJar);

  @TemplateParameter.Text(
      order = 2,
      optional = false,
      regexes = {"^.+$"},
      description = "JDBC driver class name",
      helpText = "The JDBC driver class name.",
      example = "com.mysql.jdbc.Driver")
  String getDriverClassName();

  void setDriverClassName(String driverClassName);

  @TemplateParameter.Text(
      order = 3,
      optional = false,
      regexes = {
        "(^jdbc:[a-zA-Z0-9/:@.?_+!*=&-;]+$)|(^([A-Za-z0-9+/]{4}){1,}([A-Za-z0-9+/]{0,3})={0,3})"
      },
      groupName = "Source",
      description = "JDBC connection URL string.",
      helpText =
          "The JDBC connection URL string. For example, `jdbc:mysql://some-host:3306/sampledb`. You can pass in this value as a string that's encrypted with a Cloud KMS key and then Base64-encoded. Remove whitespace characters from the Base64-encoded string. Note the difference between an Oracle non-RAC database connection string (`jdbc:oracle:thin:@some-host:<port>:<sid>`) and an Oracle RAC database connection string (`jdbc:oracle:thin:@//some-host[:<port>]/<service_name>`).",
      example = "jdbc:mysql://some-host:3306/sampledb")
  String getConnectionURL();

  void setConnectionURL(String connectionURL);

  @TemplateParameter.Text(
      order = 4,
      optional = true,
      regexes = {"^[a-zA-Z0-9_;!*&=@#-:\\/]+$"},
      description = "JDBC connection property string.",
      helpText =
          "The properties string to use for the JDBC connection. The format of the string must "
              + "be `[propertyName=property;]*`."
              + "For more information, see "
              + "Configuration Properties (https://dev.mysql.com/doc/connector-j/en/connector-j-reference-configuration-properties.html) "
              + "in the MySQL documentation.",
      example = "unicode=true;characterEncoding=UTF-8")
  String getConnectionProperties();

  void setConnectionProperties(String connectionProperties);

  @TemplateParameter.Text(
      order = 5,
      optional = true,
      regexes = {"(^.+$|projects/.*/secrets/.*/versions/.*)"},
      description = "JDBC connection username.",
      helpText =
          "The username to use for the JDBC connection. Can be passed in as a string that's encrypted with a Cloud KMS key, or can be a Secret Manager secret in the form projects/{project}/secrets/{secret}/versions/{secret_version}.")
  String getUsername();

  void setUsername(String username);

  @TemplateParameter.Password(
      order = 6,
      optional = true,
      description = "JDBC connection password.",
      helpText =
          "The password to use for the JDBC connection. Can be passed in as a string that's encrypted with a Cloud KMS key, or can be a Secret Manager secret in the form projects/{project}/secrets/{secret}/versions/{secret_version}.")
  String getPassword();

  void setPassword(String password);

  @TemplateParameter.Text(
      order = 7,
      optional = true,
      regexes = {"^.+$"},
      description = "JDBC source SQL query",
      helpText =
          "The query to run on the source to extract the data. Note that some JDBC SQL and BigQuery types, although sharing the same name, have some differences. "
              + "Some important SQL -> BigQuery type mappings to keep in mind are:\n"
              + "DATETIME --> TIMESTAMP\n"
              + "\nType casting may be required if your schemas do not match. "
              + "This parameter can be set to a gs:// path pointing to a file in Cloud Storage to load the query from. "
              + "The file encoding should be UTF-8.",
      example = "select * from sampledb.sample_table")
  String getQuery();

  void setQuery(String query);

  void setOutputTable(String value);

  @TemplateParameter.BigQueryTable(
      order = 8,
      groupName = "Target",
      description = "BigQuery output table",
      helpText = "The BigQuery output table location.",
      example = "<PROJECT_ID>:<DATASET_NAME>.<TABLE_NAME>")
  String getOutputTable();

  @TemplateParameter.GcsWriteFolder(
      order = 9,
      optional = false,
      description = "Temporary directory for BigQuery loading process",
      helpText = "The temporary directory for the BigQuery loading process.",
      example = "gs://your-bucket/your-files/temp_dir")
  String getBigQueryLoadingTemporaryDirectory();

  void setBigQueryLoadingTemporaryDirectory(String directory);

  @TemplateParameter.KmsEncryptionKey(
      order = 10,
      optional = true,
      description = "Google Cloud KMS key",
      helpText =
          "The Cloud KMS encryption key to use to decrypt the username, password, and connection string. If you  "
              + "pass in a Cloud KMS key, you must also encrypt the username, password, and connection string.",
      example = "projects/your-project/locations/global/keyRings/your-keyring/cryptoKeys/your-key")
  String getKMSEncryptionKey();

  void setKMSEncryptionKey(String keyName);

  @TemplateParameter.Boolean(
      order = 11,
      optional = true,
      description = "Whether to use column alias to map the rows.",
      helpText =
          "If set to `true`, the pipeline uses the column alias (`AS`) instead of the column name to map the rows to BigQuery. Defaults to `false`.")
  @Default.Boolean(false)
  Boolean getUseColumnAlias();

  void setUseColumnAlias(Boolean useColumnAlias);

  @TemplateParameter.Boolean(
      order = 12,
      optional = true,
      description = "Whether to truncate data before writing",
      helpText =
          "If set to `true`, the pipeline truncates before loading data into BigQuery. Defaults to `false`, which causes the pipeline to append data.")
  @Default.Boolean(false)
  Boolean getIsTruncate();

  void setIsTruncate(Boolean isTruncate);

  @TemplateParameter.Text(
      order = 13,
      optional = true,
      description = "The name of a column of numeric type that will be used for partitioning.",
      helpText =
          "If this parameter is provided with the name of the `table` defined as an optional parameter, JdbcIO reads the table in parallel by executing multiple instances of the query on the same table (subquery) using ranges. Currently, only supports `Long` partition columns.")
  String getPartitionColumn();

  void setPartitionColumn(String partitionColumn);

  @TemplateParameter.Text(
      order = 14,
      optional = true,
      description = "Name of the table in the external database.",
      helpText =
          "The table to read from when using partitions. This parameter also accepts a subquery in parentheses.",
      example = "(select id, name from Person) as subq")
  String getTable();

  void setTable(String table);

  @TemplateParameter.Integer(
      order = 15,
      optional = true,
      description = "The number of partitions.",
      helpText =
          "The number of partitions. With the lower and upper bound, this value forms partition strides for generated `WHERE` clause expressions that are used to split the partition column evenly. When the input is less than `1`, the number is set to `1`.")
  Integer getNumPartitions();

  void setNumPartitions(Integer numPartitions);

  @TemplateParameter.Long(
      order = 16,
      optional = true,
      description = "Lower bound of partition column.",
      helpText =
          "The lower bound to use in the partition scheme. If not provided, this value is automatically inferred by Apache Beam for the supported types.")
  Long getLowerBound();

  void setLowerBound(Long lowerBound);

  @TemplateParameter.Long(
      order = 17,
      optional = true,
      description = "Upper bound of partition column",
      helpText =
          "The upper bound to use in the partition scheme. If not provided, this value is automatically inferred by Apache Beam for the supported types.")
  Long getUpperBound();

  void setUpperBound(Long lowerBound);

  @TemplateParameter.Integer(
      order = 18,
      optional = true,
      description = "Fetch Size",
      // TODO: remove the "Not used for partitioned reads" once
      // https://github.com/apache/beam/pull/28999 is released.
      helpText =
          "The number of rows to be fetched from database at a time. Not used for partitioned reads.")
  @Default.Integer(50000)
  Integer getFetchSize();

  void setFetchSize(Integer fetchSize);

  @TemplateParameter.Enum(
      order = 19,
      enumOptions = {
        @TemplateParameter.TemplateEnumOption("CREATE_IF_NEEDED"),
        @TemplateParameter.TemplateEnumOption("CREATE_NEVER")
      },
      optional = true,
      description = "Create Disposition to use for BigQuery",
      helpText =
          "The BigQuery CreateDisposition to use. For example, `CREATE_IF_NEEDED` or `CREATE_NEVER`.")
  @Default.String("CREATE_NEVER")
  String getCreateDisposition();

  void setCreateDisposition(String createDisposition);

  @TemplateParameter.GcsReadFile(
      order = 20,
      optional = true,
      description = "Cloud Storage path to BigQuery JSON schema",
      helpText =
          "The Cloud Storage path for the BigQuery JSON schema. If `createDisposition` is set to CREATE_IF_NEEDED, this parameter must be specified.",
      example = "gs://your-bucket/your-schema.json")
  String getBigQuerySchemaPath();

  void setBigQuerySchemaPath(String path);

  @TemplateParameter.BigQueryTable(
      order = 21,
      optional = true,
      description =
          "Table for messages that failed to reach the output table (i.e., Deadletter table) when using Storage Write API",
      helpText =
          "The BigQuery table to use for messages that failed to reach the output table, "
              + "formatted as `\"PROJECT_ID:DATASET_NAME.TABLE_NAME\"`. If the table "
              + "doesn't exist, it is created when the pipeline runs. "
              + "If this parameter is not specified, the pipeline will fail on write errors."
              + "This parameter can only be specified if `useStorageWriteApi` or `useStorageWriteApiAtLeastOnce` is set to true.")
  String getOutputDeadletterTable();

  void setOutputDeadletterTable(String value);
}
