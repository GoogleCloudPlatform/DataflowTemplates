/*
 * Copyright (C) 2021 Google LLC
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
import com.google.cloud.teleport.v2.utils.DataplexJdbcPartitionUtils.PartitioningSchema;
import com.google.cloud.teleport.v2.utils.FileFormat.FileFormatOptions;
import com.google.cloud.teleport.v2.utils.JdbcIngestionWriteDisposition.WriteDispositionOptions;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;

/**
 * The {@link DataplexJdbcIngestionOptions} class provides the custom execution options passed by
 * the executor at the command-line.
 */
public interface DataplexJdbcIngestionOptions
    extends GcpOptions, PipelineOptions, DataplexUpdateMetadataOptions, BigQueryOptions {

  @TemplateParameter.Text(
      order = 1,
      optional = false,
      regexes = {
        "(^jdbc:[a-zA-Z0-9/:@.?_+!*=&-;]+$)|(^([A-Za-z0-9+/]{4}){1,}([A-Za-z0-9+/]{0,3})={0,3})"
      },
      description = "JDBC connection URL string.",
      helpText =
          "Url connection string to connect to the JDBC source. Connection string can be passed in"
              + " as plaintext or as a base64 encoded string encrypted by Google Cloud KMS.",
      example = "jdbc:mysql://some-host:3306/sampledb")
  @Validation.Required
  String getConnectionURL();

  void setConnectionURL(String connectionURL);

  @TemplateParameter.Text(
      order = 2,
      optional = false,
      regexes = {"^.+$"},
      description = "JDBC driver class name.",
      helpText = "JDBC driver class name to use.",
      example = "com.mysql.jdbc.Driver")
  @Validation.Required
  String getDriverClassName();

  void setDriverClassName(String driverClassName);

  @TemplateParameter.Text(
      order = 3,
      optional = false,
      regexes = {"^.+$"},
      description = "Cloud Storage paths for JDBC drivers",
      helpText = "Comma separated Cloud Storage paths for JDBC drivers.",
      example = "gs://your-bucket/driver_jar1.jar,gs://your-bucket/driver_jar2.jar")
  @Validation.Required
  String getDriverJars();

  void setDriverJars(String driverJar);

  @TemplateParameter.Text(
      order = 4,
      optional = true,
      regexes = {"^[a-zA-Z0-9_;!*&=@#-:\\/]+$"},
      description = "JDBC connection property string.",
      helpText =
          "Properties string to use for the JDBC connection. Format of the string must be"
              + " [propertyName=property;]*.",
      example = "unicode=true;characterEncoding=UTF-8")
  @Validation.Required
  String getConnectionProperties();

  void setConnectionProperties(String connectionProperties);

  @TemplateParameter.Text(
      order = 5,
      optional = true,
      regexes = {"^.+$"},
      description = "JDBC connection username.",
      helpText =
          "User name to be used for the JDBC connection. User name can be passed in as plaintext "
              + "or as a base64 encoded string encrypted by Google Cloud KMS.")
  @Validation.Required
  String getUsername();

  void setUsername(String username);

  @TemplateParameter.Password(
      order = 6,
      optional = true,
      description = "JDBC connection password.",
      helpText =
          "Password to be used for the JDBC connection. Password can be passed in as plaintext "
              + "or as a base64 encoded string encrypted by Google Cloud KMS.")
  @Validation.Required
  String getPassword();

  void setPassword(String password);

  @TemplateParameter.Text(
      order = 7,
      optional = false,
      regexes = {"^.+$"},
      description = "JDBC source SQL query.",
      helpText = "Query to be executed on the source to extract the data.",
      example = "select * from sampledb.sample_table")
  @Validation.Required
  String getQuery();

  void setQuery(String query);

  @TemplateParameter.Text(
      order = 8,
      optional = false,
      regexes = {"^.+$"},
      description = "BigQuery output table or Cloud Storage top folder name",
      helpText =
          "BigQuery table location or Cloud Storage top folder name to write the output to. If it's"
              + " a BigQuery table location, the table’s schema must match the source query schema"
              + " and should in the format of some-project-id:somedataset.sometable. If it's a"
              + " Cloud Storage top folder, just provide the top folder name.")
  @Validation.Required
  String getOutputTable();

  void setOutputTable(String value);

  @TemplateParameter.KmsEncryptionKey(
      order = 9,
      optional = true,
      description = "Google Cloud KMS key",
      helpText =
          "If this parameter is provided, password, user name and connection string should all be"
              + " passed in encrypted. Encrypt parameters using the KMS API encrypt endpoint. See:"
              + " https://cloud.google.com/kms/docs/reference/rest/v1/projects.locations.keyRings.cryptoKeys/encrypt",
      example = "projects/your-project/locations/global/keyRings/your-keyring/cryptoKeys/your-key")
  String getKMSEncryptionKey();

  void setKMSEncryptionKey(String keyName);

  @TemplateParameter.Text(
      order = 10,
      optional = false,
      regexes = {
        "^projects\\/[^\\n"
            + "\\r"
            + "\\/]+\\/locations\\/[^\\n"
            + "\\r"
            + "\\/]+\\/lakes\\/[^\\n"
            + "\\r"
            + "\\/]+\\/zones\\/[^\\n"
            + "\\r"
            + "\\/]+\\/assets\\/[^\\n"
            + "\\r"
            + "\\/]+$"
      },
      description = "Dataplex output asset ID",
      helpText =
          "Dataplex output asset ID to which the results are stored to. Should be in the format of"
              + " projects/your-project/locations/<loc>/lakes/<lake-name>/zones/<zone-name>/assets/<asset-name>")
  @Validation.Required
  String getOutputAsset();

  void setOutputAsset(String outputAsset);

  @TemplateParameter.Enum(
      order = 11,
      enumOptions = {"DAILY", "HOURLY", "MONTHLY"},
      optional = true,
      description = "The partition scheme when writing the file.",
      helpText = "The partition scheme when writing the file. Format: DAILY or MONTHLY or HOURLY.")
  @Default.Enum("DAILY")
  PartitioningSchema getPartitioningScheme();

  void setPartitioningScheme(PartitioningSchema partitioningScheme);

  @TemplateParameter.Text(
      order = 12,
      optional = true,
      description = "The partition column on which the partition is based.",
      helpText =
          "The partition column on which the partition is based. The column type must be of"
              + " timestamp/date format.")
  String getParitionColumn();

  void setParitionColumn(String partitionColumn);

  @TemplateParameter.Enum(
      order = 13,
      enumOptions = {"WRITE_APPEND", "WRITE_TRUNCATE", "WRITE_EMPTY"},
      optional = true,
      description = "BigQuery write disposition type",
      helpText =
          "Strategy to employ if the target file/table exists. If the table exists - should it"
              + " overwrite/append or fail the load. Format: WRITE_APPEND or WRITE_TRUNCATE or"
              + " WRITE_EMPTY. Only supported for writing to BigQuery.")
  @Default.Enum("WRITE_EMPTY")
  WriteDispositionOptions getWriteDisposition();

  void setWriteDisposition(WriteDispositionOptions writeDisposition);

  @TemplateParameter.Enum(
      order = 14,
      enumOptions = {"AVRO", "PARQUET"},
      optional = true,
      description = "Output file format in Cloud Storage.",
      helpText = "Output file format in Cloud Storage. Format: PARQUET or AVRO.")
  @Default.Enum("PARQUET")
  FileFormatOptions getFileFormat();

  void setFileFormat(FileFormatOptions fileFormat);

  @TemplateParameter.Boolean(
      order = 15,
      optional = true,
      description = "Whether to use column alias to map the rows.",
      helpText =
          "If enabled (set to true) the pipeline will consider column alias (\"AS\") instead of the"
              + " column name to map the rows to BigQuery. Defaults to false.")
  @Default.Boolean(false)
  Boolean getUseColumnAlias();

  void setUseColumnAlias(Boolean useColumnAlias);
}
