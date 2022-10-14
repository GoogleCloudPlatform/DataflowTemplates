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

import org.apache.beam.sdk.io.gcp.bigquery.BigQueryOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;

/** Interface used by the JdbcToBigQuery pipeline to accept user input. */
public interface JdbcToBigQueryOptions extends CommonTemplateOptions, BigQueryOptions {

  @Description(
      "Comma separate list of driver class/dependency jar file GCS paths "
          + "for example "
          + "gs://<some-bucket>/driver_jar1.jar,gs://<some_bucket>/driver_jar2.jar")
  String getDriverJars();

  void setDriverJars(String driverJar);

  @Description("The JDBC driver class name. " + "for example: com.mysql.jdbc.Driver")
  String getDriverClassName();

  void setDriverClassName(String driverClassName);

  @Description(
      "The JDBC connection URL string. " + "for example: jdbc:mysql://some-host:3306/sampledb")
  String getConnectionURL();

  void setConnectionURL(String connectionURL);

  @Description(
      "JDBC connection property string. " + "for example: unicode=true&characterEncoding=UTF-8")
  String getConnectionProperties();

  void setConnectionProperties(String connectionProperties);

  @Description("JDBC connection user name. ")
  String getUsername();

  void setUsername(String username);

  @Description("JDBC connection password. ")
  String getPassword();

  void setPassword(String password);

  @Description("Source data query string. " + "for example: select * from sampledb.sample_table")
  String getQuery();

  void setQuery(String query);

  @Description(
      "BigQuery Table spec to write the output to"
          + "for example: some-project-id:somedataset.sometable")
  String getOutputTable();

  void setOutputTable(String value);

  @Description("Temporary directory for BigQuery loading process")
  String getBigQueryLoadingTemporaryDirectory();

  void setBigQueryLoadingTemporaryDirectory(String directory);

  @Description(
      "KMS Encryption Key should be in the format"
          + " projects/{gcp_project}/locations/{key_region}/keyRings/{key_ring}/cryptoKeys/{kms_key_name}")
  String getKMSEncryptionKey();

  void setKMSEncryptionKey(String keyName);

  @Description(
      "If enabled (set to true) the pipeline will consider column alias (\"AS\") instead of the column name to map the rows to BigQuery")
  @Default.Boolean(false)
  Boolean getUseColumnAlias();

  void setUseColumnAlias(Boolean useColumnAlias);

  // Adding new option for deciding if load will be APPEND only or TRUNCATE/LOAD
  // Dt: 13-10-2022
  // Auth: @suddhasatwa
  @Description(
      "If enabled (set to true) the pipeline will truncate/load data into BigQuery, else by default, it will Append.")
  @Default.Boolean(false)
  Boolean getIsTruncate();

  void setIsTruncate(Boolean isTruncate);
}
