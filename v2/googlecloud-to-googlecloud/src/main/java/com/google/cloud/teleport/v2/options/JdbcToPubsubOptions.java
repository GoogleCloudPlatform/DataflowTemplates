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

import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation;

/**
 * The {@link JdbcToPubsubOptions} interface provides the custom execution options passed by the
 * executor at the command-line.
 */
public interface JdbcToPubsubOptions extends CommonTemplateOptions {
  @Description("The JDBC driver class name. " + "For example: com.mysql.jdbc.Driver")
  String getDriverClassName();

  void setDriverClassName(String driverClassName);

  @Description(
      "The JDBC connection URL string. " + "For example: jdbc:mysql://some-host:3306/sampledb")
  String getConnectionUrl();

  void setConnectionUrl(String connectionUrl);

  @Description("JDBC connection user name. ")
  String getUsername();

  void setUsername(String username);

  @Description("JDBC connection password. ")
  String getPassword();

  void setPassword(String password);

  @Description(
      "Comma separate list of driver class/dependency jar file GCS paths "
          + "for example "
          + "gs://<some-bucket>/driver_jar1.jar,gs://<some_bucket>/driver_jar2.jar")
  String getDriverJars();

  void setDriverJars(String driverJar);

  @Description(
      "JDBC connection property string. " + "For example: unicode=true&characterEncoding=UTF-8")
  String getConnectionProperties();

  void setConnectionProperties(String connectionProperties);

  @Description(
      "Executing the SQL query to read from the database. For example: select col1,col2 from"
          + " tablename")
  String getQuery();

  void setQuery(String query);

  @Description(
      "The Cloud Pub/Sub topic to publish to. "
          + "The name should be in the format of "
          + "projects/<project-id>/topics/<topic-name>.")
  @Validation.Required
  String getOutputTopic();

  void setOutputTopic(String outputTopic);

  @Description(
      "KMS Encryption Key. The key should be in the format"
          + " projects/{gcp_project}/locations/{key_region}/keyRings/{key_ring}/cryptoKeys/{kms_key_name}")
  String getKMSEncryptionKey();

  void setKMSEncryptionKey(String keyName);
}
