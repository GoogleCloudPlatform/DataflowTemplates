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

import com.google.cloud.teleport.v2.utils.DataplexJdbcPartitionUtils.PartitioningSchema;
import com.google.cloud.teleport.v2.utils.FileFormat.FileFormatOptions;
import com.google.cloud.teleport.v2.utils.JdbcIngestionWriteDisposition.WriteDispositionOptions;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;

/**
 * The {@link DataplexJdbcIngestionOptions} class provides the custom execution options passed by
 * the executor at the command-line.
 */
public interface DataplexJdbcIngestionOptions
    extends GcpOptions, PipelineOptions, DataplexUpdateMetadataOptions {
  @Description(
      "Comma separate list of driver class/dependency jar file GCS paths "
          + "for example "
          + "gs://<some-bucket>/driver_jar1.jar,gs://<some_bucket>/driver_jar2.jar")
  @Validation.Required
  String getDriverJars();

  void setDriverJars(String driverJar);

  @Description("The JDBC driver class name. " + "for example: com.mysql.jdbc.Driver")
  @Validation.Required
  String getDriverClassName();

  void setDriverClassName(String driverClassName);

  @Description(
      "The JDBC connection URL string. " + "for example: jdbc:mysql://some-host:3306/sampledb")
  @Validation.Required
  String getConnectionURL();

  void setConnectionURL(String connectionURL);

  @Description(
      "JDBC connection property string. " + "for example: unicode=true&characterEncoding=UTF-8")
  @Validation.Required
  String getConnectionProperties();

  void setConnectionProperties(String connectionProperties);

  @Description("JDBC connection user name. ")
  @Validation.Required
  String getUsername();

  void setUsername(String username);

  @Description("JDBC connection password. ")
  @Validation.Required
  String getPassword();

  void setPassword(String password);

  @Description("Source data query string. " + "for example: select * from sampledb.sample_table")
  @Validation.Required
  String getQuery();

  void setQuery(String query);

  @Description(
      "BigQuery output table or GCS top folder name to write to. "
          + "If it's a BigQuery table, it should be in the format of "
          + "some-project-id:somedataset.sometable."
          + "If it's a GCS top folder, just provide the folder name.")
  @Validation.Required
  String getOutputTable();

  void setOutputTable(String value);

  @Description(
      "Optional KMS Encryption Key to decrypt the 'username', 'password' and 'url' parameters. If"
          + " not set, username/password/url have to be provided unencrypted. Should be in the"
          + " format of"
          + " projects/{gcp_project}/locations/{key_region}/keyRings/{key_ring}/cryptoKeys/{kms_key_name}")
  String getKMSEncryptionKey();

  void setKMSEncryptionKey(String keyName);

  @Description(
      "Dataplex output asset ID to which the results are stored to. Should be in the format of"
          + " projects/<name>/locations/<loc>/lakes/<lake-name>/zones/<zone-name>/assets/<asset"
          + " name>")
  @Validation.Required
  String getOutputAsset();

  void setOutputAsset(String outputAsset);

  @Description(
      "The partition scheme when writing the file. Default: daily. "
          + "Allowed formats are: "
          + "DAILY, MONTHLY, HOURLY")
  @Default.Enum("DAILY")
  PartitioningSchema getPartitioningScheme();

  void setPartitioningScheme(PartitioningSchema partitioningScheme);

  @Description(
      "The partition column on which the partition is based. "
          + "Must be of "
          + "timestamp / date format")
  String getParitionColumn();

  void setParitionColumn(String partitionColumn);

  @Description(
      "Strategy to employ if the target file/table exists. For BigQuery, allowed formats are:"
          + " WRITE_APPEND / WRITE_TRUNCATE / WRITE_EMPTY. For GCS, allowed"
          + " formats are: SKIP / WRITE_TRUNCATE / WRITE_EMPTY. Default: WRITE_EMPTY. ")
  @Default.Enum("WRITE_EMPTY")
  WriteDispositionOptions getWriteDisposition();

  void setWriteDisposition(WriteDispositionOptions writeDisposition);

  @Description("Output file format in GCS. Format: PARQUET, AVRO, or ORC. Default: PARQUET.")
  @Default.Enum("PARQUET")
  FileFormatOptions getFileFormat();

  void setFileFormat(FileFormatOptions fileFormat);

  @Description(
      "If enabled (set to true) the pipeline will consider column alias (\"AS\") instead of the column name to map the rows to BigQuery")
  @Default.Boolean(false)
  Boolean getUseColumnAlias();

  void setUseColumnAlias(Boolean useColumnAlias);
}
