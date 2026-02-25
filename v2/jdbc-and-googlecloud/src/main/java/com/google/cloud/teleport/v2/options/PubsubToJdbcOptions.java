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
 * The {@link PubsubToJdbcOptions} interface provides the custom execution options passed by the
 * executor at the command-line.
 */
public interface PubsubToJdbcOptions extends CommonTemplateOptions {

  @TemplateParameter.PubsubSubscription(
      order = 1,
      groupName = "Source",
      description = "Pub/Sub input subscription",
      helpText = "The Pub/Sub input subscription to read from.",
      example = "projects/<PROJECT_ID>/subscriptions/<SUBSCRIPTION_NAME>")
  @Validation.Required
  String getInputSubscription();

  void setInputSubscription(String inputSubscription);

  @TemplateParameter.Text(
      order = 2,
      optional = false,
      regexes = {"^.+$"},
      description = "JDBC driver class name.",
      helpText = "The JDBC driver class name.",
      example = "com.mysql.jdbc.Driver")
  String getDriverClassName();

  void setDriverClassName(String driverClassName);

  @TemplateParameter.Text(
      order = 3,
      groupName = "Target",
      optional = false,
      regexes = {
        "(^jdbc:[a-zA-Z0-9/:@.?_+!*=&-;]+$)|(^([A-Za-z0-9+/]{4}){1,}([A-Za-z0-9+/]{0,3})={0,3})"
      },
      description = "JDBC connection URL string.",
      helpText =
          "The JDBC connection URL string. "
              + "You can pass in this value as a string that's encrypted with a Cloud KMS "
              + "key and then Base64-encoded. Remove whitespace characters from the Base64-encoded string.",
      example = "jdbc:mysql://some-host:3306/sampledb")
  String getConnectionUrl();

  void setConnectionUrl(String connectionUrl);

  @TemplateParameter.Text(
      order = 4,
      optional = true,
      regexes = {"^.+$"},
      description = "JDBC connection username.",
      helpText =
          "The username to use for the JDBC connection. "
              + "You can pass in this value as a string that's encrypted with a Cloud KMS key and then Base64-encoded.")
  String getUsername();

  void setUsername(String username);

  @TemplateParameter.Password(
      order = 5,
      optional = true,
      description = "JDBC connection password.",
      helpText =
          "The password to use for the JDBC connection. "
              + "You can pass in this value as a string that's encrypted with a Cloud KMS key and then Base64-encoded.")
  String getPassword();

  void setPassword(String password);

  @TemplateParameter.Text(
      order = 6,
      optional = false,
      regexes = {"^.+$"},
      description = "Cloud Storage paths for JDBC drivers",
      helpText = "Comma separated Cloud Storage paths for JDBC drivers. ",
      example = "gs://your-bucket/driver_jar1.jar,gs://your-bucket/driver_jar2.jar")
  String getDriverJars();

  void setDriverJars(String driverJar);

  @TemplateParameter.Text(
      order = 7,
      optional = true,
      regexes = {"^[a-zA-Z0-9_;!*&=@#-:\\/]+$"},
      description = "JDBC connection property string.",
      helpText =
          "The properties string to use for the JDBC connection. "
              + "The string must use the format `[propertyName=property;]*`. ",
      example = "unicode=true;characterEncoding=UTF-8")
  String getConnectionProperties();

  void setConnectionProperties(String connectionProperties);

  @TemplateParameter.Text(
      order = 8,
      optional = false,
      regexes = {"^.+$"},
      description = "Statement which will be executed against the database.",
      helpText =
          "The statement to run against the database. The statement must specify the column "
              + "names of the table in any order. Only the values of the specified column "
              + "names are read from the JSON and added to the statement. ",
      example = "INSERT INTO tableName (column1, column2) VALUES (?,?)")
  String getStatement();

  void setStatement(String statement);

  @TemplateParameter.PubsubTopic(
      order = 9,
      description = "Output deadletter Pub/Sub topic",
      helpText = "The Pub/Sub topic to forward undeliverable messages to. ",
      example = "projects/<PROJECT_ID>/topics/<TOPIC_NAME>")
  @Validation.Required
  String getOutputDeadletterTopic();

  void setOutputDeadletterTopic(String deadletterTopic);

  @TemplateParameter.KmsEncryptionKey(
      order = 10,
      optional = true,
      description = "Google Cloud KMS encryption key",
      helpText =
          "The Cloud KMS Encryption Key to use to decrypt the username, password, and connection string. "
              + "If a Cloud KMS key is passed in, the username, password, and "
              + "connection string must all be passed in encrypted.",
      example =
          "projects/{gcp_project}/locations/{key_region}/keyRings/{key_ring}/cryptoKeys/{kms_key_name}")
  String getKMSEncryptionKey();

  void setKMSEncryptionKey(String keyName);
}
