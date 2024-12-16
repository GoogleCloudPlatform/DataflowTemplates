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
import org.apache.beam.sdk.options.Validation;

/**
 * The {@link JdbcToPubsubOptions} interface provides the custom execution options passed by the
 * executor at the command-line.
 */
public interface JdbcToPubsubOptions extends CommonTemplateOptions {
  @TemplateParameter.Text(
      order = 1,
      optional = false,
      regexes = {"^.+$"},
      description = "JDBC driver class name.",
      helpText = "The JDBC driver class name.",
      example = "com.mysql.jdbc.Driver")
  String getDriverClassName();

  void setDriverClassName(String driverClassName);

  @TemplateParameter.Text(
      order = 2,
      groupName = "Source",
      optional = false,
      regexes = {
        "(^jdbc:[a-zA-Z0-9/:@.?_+!*=&-;]+$)|(^([A-Za-z0-9+/]{4}){1,}([A-Za-z0-9+/]{0,3})={0,3})"
      },
      description = "JDBC connection URL string.",
      helpText =
          "The JDBC connection URL string. You can pass in this value as a string that's encrypted with a Cloud KMS key and then Base64-encoded. "
              + "For example: 'echo -n \"jdbc:mysql://some-host:3306/sampledb\" | gcloud kms encrypt --location=<location> --keyring=<keyring> --key=<key> --plaintext-file=- --ciphertext-file=- | base64'",
      example = "jdbc:mysql://some-host:3306/sampledb")
  String getConnectionUrl();

  void setConnectionUrl(String connectionUrl);

  @TemplateParameter.Text(
      order = 3,
      optional = true,
      regexes = {"^.+$"},
      description = "JDBC connection username.",
      helpText =
          "The username to use for the JDBC connection. You can pass in this value as a string that's encrypted with a Cloud KMS key and then Base64-encoded. For example, `echo -n 'some_username' | glcloud kms encrypt --location=my_location --keyring=mykeyring --key=mykey --plaintext-file=- --ciphertext-file=- | base64`")
  String getUsername();

  void setUsername(String username);

  @TemplateParameter.Password(
      order = 4,
      optional = true,
      description = "JDBC connection password.",
      helpText =
          "The password to use for the JDBC connection. You can pass in this value as a string that's encrypted with a Cloud KMS key and then Base64-encoded. For example, `echo -n 'some_password' | glcloud kms encrypt --location=my_location --keyring=mykeyring --key=mykey --plaintext-file=- --ciphertext-file=- | base64`")
  String getPassword();

  void setPassword(String password);

  @TemplateParameter.Text(
      order = 5,
      optional = false,
      regexes = {"^.+$"},
      description = "Cloud Storage paths for JDBC drivers",
      helpText = "Comma-separated Cloud Storage paths for JDBC drivers.",
      example = "gs://your-bucket/driver_jar1.jar,gs://your-bucket/driver_jar2.jar")
  String getDriverJars();

  void setDriverJars(String driverJar);

  @TemplateParameter.Text(
      order = 6,
      optional = true,
      regexes = {"^[a-zA-Z0-9_;!*&=@#-:\\/]+$"},
      description = "JDBC connection property string.",
      helpText =
          "The properties string to use for the JDBC connection. The format of the string must be `[propertyName=property;]*`. ",
      example = "unicode=true;characterEncoding=UTF-8")
  String getConnectionProperties();

  void setConnectionProperties(String connectionProperties);

  @TemplateParameter.Text(
      order = 7,
      optional = false,
      regexes = {"^.+$"},
      description = "JDBC source SQL query.",
      helpText = "The query to run on the source to extract the data.",
      example = "select * from sampledb.sample_table")
  String getQuery();

  void setQuery(String query);

  @TemplateParameter.PubsubTopic(
      order = 8,
      groupName = "Target",
      description = "Output Pub/Sub topic",
      helpText = "The Pub/Sub topic to publish to.",
      example = "projects/<PROJECT_ID>/topics/<TOPIC_NAME>")
  @Validation.Required
  String getOutputTopic();

  void setOutputTopic(String outputTopic);

  @TemplateParameter.KmsEncryptionKey(
      order = 9,
      optional = true,
      description = "Google Cloud KMS key",
      helpText =
          "The Cloud KMS Encryption Key to use to decrypt the username, password, and connection string. If a Cloud KMS key is passed in, the username, password, and connection string must all be passed in encrypted and base64 encoded.",
      example = "projects/your-project/locations/global/keyRings/your-keyring/cryptoKeys/your-key")
  String getKMSEncryptionKey();

  void setKMSEncryptionKey(String keyName);
}
