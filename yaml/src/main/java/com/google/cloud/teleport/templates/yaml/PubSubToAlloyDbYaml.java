/*
 * Copyright (C) 2026 Google LLC
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
package com.google.cloud.teleport.templates.yaml;

import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.metadata.TemplateParameter;
import org.apache.beam.sdk.options.Validation;

@Template(
    name = "PubSub_To_AlloyDb_Yaml",
    category = TemplateCategory.STREAMING,
    type = Template.TemplateType.YAML,
    displayName = "PubSub to AlloyDb (YAML)",
    description =
        "The PubSub to AlloyDb template is a streaming pipeline which ingests data from a PubSub topic, executes a user-defined mapping, and writes the resulting records to AlloyDb. Any errors which occur in the transformation of the data are written to a separate Pub/Sub topic.",
    flexContainerName = "pubsub-to-alloydb-yaml",
    yamlTemplateFile = "PubSubToAlloyDb.yaml",
    filesToCopy = {
      "PubSubToAlloyDb.yaml",
      "main.py",
      "requirements.txt",
      "options/pubsub_options.yaml",
      "options/alloydb_options.yaml",
      "options/maptofields_options.yaml",
      "options/windowinto_options.yaml"
    },
    documentation = "",
    contactInformation = "https://cloud.google.com/support",
    requirements = {
      "The input Pub/Sub topic must exist.",
      "The mapToField Pub/Sub error topic must exist.",
      "The output AlloyDb instance must exist."
    },
    streaming = true,
    hidden = false)
public interface PubSubToAlloyDbYaml {

  @TemplateParameter.Text(
      order = 1,
      name = "topic",
      optional = false,
      description = "Pub/Sub input topic",
      helpText = "Pub/Sub topic to read the input from.",
      example = "projects/your-project-id/topics/your-topic-name")
  @Validation.Required
  String getTopic();

  @TemplateParameter.Text(
      order = 2,
      name = "subscription",
      optional = true,
      description = "Pub/Sub subscription",
      helpText = "Pub/Sub subscription to read the input from.",
      example = "projects/your-project-id/subscriptions/your-subscription-name")
  String getSubscription();

  @TemplateParameter.Text(
      order = 3,
      name = "format",
      optional = false,
      description = "The message format.",
      helpText = "The message format. One of: AVRO, JSON, PROTO, RAW, or STRING.",
      example = "JSON")
  @Validation.Required
  String getFormat();

  @TemplateParameter.Text(
      order = 4,
      name = "schema",
      optional = false,
      description = "Data schema.",
      helpText =
          "A schema is required if data format is JSON, AVRO or PROTO. For JSON, this is a JSON schema. For AVRO and PROTO, this is the full schema definition.",
      example = "")
  @Validation.Required
  String getSchema();

  @TemplateParameter.Text(
      order = 5,
      name = "attributes",
      optional = true,
      description = "List of attribute keys.",
      helpText =
          "List of attribute keys whose values will be flattened into the output message as additional fields.",
      example = "")
  String getAttributes();

  @TemplateParameter.Text(
      order = 6,
      name = "attributeMap",
      optional = true,
      description = "Name of a field in which to store the full set of attributes.",
      helpText =
          "Name of a field in which to store the full set of attributes associated with this message.",
      example = "")
  String getAttributeMap();

  @TemplateParameter.Text(
      order = 7,
      name = "idAttribute",
      optional = true,
      description =
          "The attribute on incoming Pub/Sub messages to use as a unique record identifier.",
      helpText =
          "The attribute on incoming Pub/Sub messages to use as a unique record identifier. When specified, the value of this attribute will be used for deduplication of messages.",
      example = "")
  String getIdAttribute();

  @TemplateParameter.Text(
      order = 8,
      name = "timestampAttribute",
      optional = true,
      description = "Message value to use as element timestamp.",
      helpText =
          "Message value to use as element timestamp. If None, uses message publishing time as the timestamp.",
      example = "")
  String getTimestampAttribute();

  @TemplateParameter.Text(
      order = 9,
      name = "language",
      optional = false,
      description = "Language used to define the expressions.",
      helpText =
          "The language used to define (and execute) the expressions and callables in fields.",
      example = "python")
  @Validation.Required
  String getLanguage();

  @TemplateParameter.Text(
      order = 10,
      name = "fields",
      optional = false,
      description = "Field mapping configuration",
      helpText =
          "The output fields to compute, each mapping to the expression or callable that creates them.",
      example = "")
  @Validation.Required
  String getFields();

  @TemplateParameter.Text(
      order = 11,
      name = "windowing",
      optional = true,
      description = "Windowing options",
      helpText =
          "Windowing options - see https://beam.apache.org/documentation/sdks/yaml/#windowing",
      example = "")
  String getWindowing();

  @TemplateParameter.Text(
      order = 12,
      name = "url",
      optional = false,
      description = "JDBC connection URL for AlloyDb",
      helpText =
          "The JDBC URL for connecting to AlloyDb instance. Format: jdbc:postgresql:///db?socketFactory=com.google.cloud.alloydb.SocketFactory&alloydbInstanceName=projects/<PROJECT>/locations/<REGION>/clusters/<CLUSTER>/instances/<INSTANCE>&alloydbIpType=PRIVATE",
      example =
          "jdbc:postgresql:///mydatabase?socketFactory=com.google.cloud.alloydb.SocketFactory&alloydbInstanceName=projects/my-project/locations/us-central1/clusters/my-cluster/instances/my-instance&alloydbIpType=PRIVATE")
  @Validation.Required
  String getJdbcUrl();

  @TemplateParameter.Text(
      order = 13,
      name = "username",
      optional = false,
      description = "AlloyDb database username",
      helpText = "Username for AlloyDb authentication",
      example = "postgres")
  @Validation.Required
  String getUsername();

  @TemplateParameter.Password(
      order = 14,
      name = "password",
      optional = false,
      description = "AlloyDb database password",
      helpText = "Password for AlloyDb authentication")
  @Validation.Required
  String getPassword();

  @TemplateParameter.Text(
      order = 15,
      name = "driverClassName",
      optional = true,
      description = "JDBC driver class name",
      helpText = "The JDBC driver class name for connecting to AlloyDb",
      example = "org.postgresql.Driver")
  String getDriverClassName();

  @TemplateParameter.Text(
      order = 16,
      name = "connectionProperties",
      optional = true,
      description = "Connection properties for AlloyDb",
      helpText =
          "Optional connection properties as key-value pairs. Example: sslmode=require;connectTimeout=10",
      example = "sslmode=require;connectTimeout=10")
  String getConnectionProperties();

  @TemplateParameter.Text(
      order = 17,
      name = "connectionInitSql",
      optional = true,
      description = "A list of SQL statements to execute upon connection initialization",
      helpText = "A list of SQL statements to execute when a new connection is established.",
      example = "[\"SET TIME ZONE UTC\"]")
  String getConnectionInitSql();

  @TemplateParameter.Text(
      order = 18,
      name = "location",
      optional = false,
      description = "Target table name in AlloyDb",
      helpText = "The name of the table where records will be written",
      example = "my_table")
  @Validation.Required
  String getLocation();

  @TemplateParameter.Text(
      order = 19,
      name = "writeStatement",
      optional = false,
      description = "SQL statement for writing records",
      helpText = "The SQL INSERT or UPSERT statement template for writing records to AlloyDb",
      example = "INSERT INTO table_name (col1, col2) VALUES (?, ?)")
  @Validation.Required
  String getWriteStatement();

  @TemplateParameter.Integer(
      order = 20,
      name = "batchSize",
      optional = true,
      description = "Batch size for write operations",
      helpText = "Number of records to batch before writing to the database",
      example = "100")
  Integer getBatchSize();

  @TemplateParameter.Boolean(
      order = 21,
      name = "autosharding",
      optional = true,
      description = "Enable autosharding for parallel writes",
      helpText = "Whether to use autosharding for distributing writes across multiple connections")
  Boolean getAutosharding();

  @TemplateParameter.Text(
      order = 22,
      name = "network",
      optional = true,
      description = "VPC Network name",
      helpText = "The VPC network where AlloyDB is located.",
      example = "default")
  String getNetwork();

  @TemplateParameter.Text(
      order = 23,
      name = "subnetwork",
      optional = true,
      description = "Subnetwork name",
      helpText = "The subnetwork for Dataflow workers. Required for custom VPCs.",
      example = "regions/us-central1/subnetworks/my-subnet")
  String getSubnetwork();

  @TemplateParameter.Text(
      order = 24,
      name = "outputDeadLetterPubSubTopic",
      optional = false,
      description = "Pub/Sub transformation error topic",
      helpText = "Pub/Sub error topic for failed transformation messages.",
      example = "projects/your-project-id/topics/your-error-topic-name")
  @Validation.Required
  String getOutputDeadLetterPubSubTopic();
}
