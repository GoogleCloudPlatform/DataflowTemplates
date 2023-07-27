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
      groupName = "Source Parameters",
      description = "Cloud Storage paths for JDBC drivers",
      helpText = "Comma separate Cloud Storage paths for JDBC drivers.",
      example = "gs://your-bucket/driver_jar1.jar,gs://your-bucket/driver_jar2.jar")
  String getDriverJars();

  void setDriverJars(String driverJar);

  @TemplateParameter.Text(
      order = 2,
      optional = false,
      regexes = {"^.+$"},
      groupName = "Source Parameters",
      description = "JDBC driver class name.",
      helpText = "JDBC driver class name to use.",
      example = "com.mysql.jdbc.Driver")
  String getDriverClassName();

  void setDriverClassName(String driverClassName);

  @TemplateParameter.Text(
      order = 3,
      optional = false,
      regexes = {
        "(^jdbc:[a-zA-Z0-9/:@.?_+!*=&-;]+$)|(^([A-Za-z0-9+/]{4}){1,}([A-Za-z0-9+/]{0,3})={0,3})"
      },
      groupName = "Source Parameters",
      description = "JDBC connection URL string.",
      helpText =
          "Url connection string to connect to the JDBC source. Connection string can be passed in"
              + " as plaintext or as a base64 encoded string encrypted by Google Cloud KMS.",
      example = "jdbc:mysql://some-host:3306/sampledb")
  String getConnectionURL();

  void setConnectionURL(String connectionURL);

  @TemplateParameter.Text(
      order = 4,
      optional = true,
      regexes = {"^[a-zA-Z0-9_;!*&=@#-:\\/]+$"},
      groupName = "Source Parameters",
      description = "JDBC connection property string.",
      helpText =
          "Properties string to use for the JDBC connection. Format of the string must be"
              + " [propertyName=property;]*.",
      example = "unicode=true;characterEncoding=UTF-8")
  String getConnectionProperties();

  void setConnectionProperties(String connectionProperties);

  @TemplateParameter.Text(
      order = 5,
      optional = true,
      regexes = {"^.+$"},
      groupName = "Source Parameters",
      description = "JDBC connection username.",
      helpText =
          "User name to be used for the JDBC connection. User name can be passed in as plaintext "
              + "or as a base64 encoded string encrypted by Google Cloud KMS.")
  String getUsername();

  void setUsername(String username);

  @TemplateParameter.Password(
      order = 6,
      optional = true,
      groupName = "Source Parameters",
      description = "JDBC connection password.",
      helpText =
          "Password to be used for the JDBC connection. Password can be passed in as plaintext "
              + "or as a base64 encoded string encrypted by Google Cloud KMS.")
  String getPassword();

  void setPassword(String password);

  @TemplateParameter.Text(
      order = 7,
      optional = true,
      regexes = {"^.+$"},
      groupName = "Source Parameters",
      description = "JDBC source SQL query.",
      helpText = "Query to be executed on the source to extract the data.",
      example = "select * from sampledb.sample_table")
  String getQuery();

  void setQuery(String query);

  void setOutputTable(String value);

  @TemplateParameter.BigQueryTable(
      order = 8,
      groupName = "Target Parameters",
      description = "BigQuery output table",
      helpText =
          "BigQuery table location to write the output to. The name should be in the format"
              + " <project>:<dataset>.<table_name>. The table's schema must match input objects.")
  String getOutputTable();

  @TemplateParameter.GcsWriteFolder(
      order = 9,
      optional = false,
      groupName = "Target Parameters",
      description = "Temporary directory for BigQuery loading process",
      helpText = "Temporary directory for BigQuery loading process",
      example = "gs://your-bucket/your-files/temp_dir")
  String getBigQueryLoadingTemporaryDirectory();

  void setBigQueryLoadingTemporaryDirectory(String directory);

  @TemplateParameter.KmsEncryptionKey(
      order = 10,
      optional = true,
      groupName = "Source Parameters",
      description = "Google Cloud KMS key",
      helpText =
          "If this parameter is provided, password, user name and connection string should all be"
              + " passed in encrypted. Encrypt parameters using the KMS API encrypt endpoint. See:"
              + " https://cloud.google.com/kms/docs/reference/rest/v1/projects.locations.keyRings.cryptoKeys/encrypt",
      example = "projects/your-project/locations/global/keyRings/your-keyring/cryptoKeys/your-key")
  String getKMSEncryptionKey();

  void setKMSEncryptionKey(String keyName);

  @TemplateParameter.Boolean(
      order = 11,
      optional = true,
      groupName = "Source Parameters",
      description = "Whether to use column alias to map the rows.",
      helpText =
          "If enabled (set to true) the pipeline will consider column alias (\"AS\") instead of the"
              + " column name to map the rows to BigQuery. Defaults to false.")
  @Default.Boolean(false)
  Boolean getUseColumnAlias();

  void setUseColumnAlias(Boolean useColumnAlias);

  @TemplateParameter.Boolean(
      order = 12,
      optional = true,
      groupName = "Target Parameters",
      description = "Whether to truncate data before writing",
      helpText =
          "If enabled (set to true) the pipeline will truncate before loading data into BigQuery."
              + " Defaults to false, which is used to only append data.")
  @Default.Boolean(false)
  Boolean getIsTruncate();

  void setIsTruncate(Boolean isTruncate);

  @TemplateParameter.Text(
      order = 13,
      optional = true,
      groupName = "Source Parameters",
      description = "The name of a column of numeric type that will be used for partitioning.",
      helpText =
          "If this parameter is provided (along with `table`), JdbcIO reads the table in parallel by executing multiple instances of the query on the same table (subquery) using ranges. Currently, only Long partition columns are supported.")
  String getPartitionColumn();

  void setPartitionColumn(String partitionColumn);

  @TemplateParameter.Text(
      order = 14,
      optional = true,
      groupName = "Source Parameters",
      description = "Name of the table in the external database.",
      helpText =
          "Table to read from using partitions. This parameter also accepts a subquery in parentheses.",
      example = "(select id, name from Person) as subq")
  String getTable();

  void setTable(String table);

  @TemplateParameter.Integer(
      order = 15,
      optional = true,
      groupName = "Source Parameters",
      description = "The number of partitions.",
      helpText =
          "The number of partitions. This, along with the lower and upper bound, form partitions strides for generated WHERE clause expressions used to split the partition column evenly. When the input is less than 1, the number is set to 1.")
  Integer getNumPartitions();

  void setNumPartitions(Integer numPartitions);

  @TemplateParameter.Long(
      order = 16,
      optional = true,
      groupName = "Source Parameters",
      description = "Lower bound of partition column.",
      helpText =
          "Lower bound used in the partition scheme. If not provided, it is automatically inferred by Beam (for the supported types)")
  Long getLowerBound();

  void setLowerBound(Long lowerBound);

  @TemplateParameter.Long(
      order = 17,
      optional = true,
      groupName = "Source Parameters",
      description = "Upper bound of partition column",
      helpText =
          "Upper bound used in partition scheme. If not provided, it is automatically inferred by Beam (for the supported types)")
  Long getUpperBound();

  void setUpperBound(Long lowerBound);

  @TemplateParameter.Text(
      order = 18,
      optional = true,
      groupName = "Sink Parameters",
      description = "BigQuery schema for rows to be written.",
      helpText =
          "BigQuery schema of the output table. If not provided, it is automatically inferred by Beam.")
  String getJsonSchema();

  void setJsonSchema(String jsonSchema);
}
