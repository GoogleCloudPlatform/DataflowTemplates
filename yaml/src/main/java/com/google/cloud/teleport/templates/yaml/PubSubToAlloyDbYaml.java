/*
 * Copyright (C) 2025 Google LLC
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
    flexContainerName = "pipeline-yaml",
    yamlTemplateFile = "PubSubToAlloyDb.yaml",
    filesToCopy = {
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
      "Either the input Pub/Sub topic or subscription must exist.",
      "The mapToFields Pub/Sub error topic must exist.",
      "The output AlloyDb instance must exist."
    },
    streaming = true,
    hidden = false)
public interface PubSubToAlloyDbYaml {

  @TemplateParameter.Text(
      order = 1,
      name = "topic",
      optional = true,
      description = "Pub/Sub input topic",
      helpText =
          "Pub/Sub topic to read the input from. Exactly one of topic or subscription must be provided. If a topic is given, a new temporary subscription is created for each pipeline run, even if a subscription for that topic already exists.",
      example = "projects/your-project-id/topics/your-topic-name")
  String getTopic();

  @TemplateParameter.Text(
      order = 2,
      name = "format",
      optional = false,
      description = "The message format.",
      helpText = "The message format. One of: AVRO, JSON, PROTO, RAW, or STRING.",
      example = "")
  @Validation.Required
  String getFormat();

  @TemplateParameter.Text(
      order = 3,
      name = "schema",
      optional = false,
      description = "Data schema.",
      helpText =
          "A schema is required if data format is JSON, AVRO or PROTO. For JSON,  this is a JSON schema. For AVRO and PROTO, this is the full schema  definition.",
      example = "")
  @Validation.Required
  String getSchema();

  @TemplateParameter.Text(
      order = 4,
      name = "attributes",
      optional = true,
      description = "List of attribute keys.",
      helpText =
          "List of attribute keys whose values will be flattened into the output message as additional fields.  For example, if the format is `raw` and attributes is `[a, b]` then this read will produce elements of the form `Row(payload=..., a=..., b=...)`.",
      example = "")
  String getAttributes();

  @TemplateParameter.Text(
      order = 5,
      name = "attributesMap",
      optional = true,
      description = "Name of a field in which to store the full set of attributes.",
      helpText =
          "Name of a field in which to store the full set of attributes associated with this message.  For example, if the format is `raw` and `attribute_map` is set to `attrs` then this read will produce elements of the form `Row(payload=..., attrs=...)` where `attrs` is a Map type of string to string. If both `attributes` and `attribute_map` are set, the overlapping attribute values will be present in both the flattened structure and the attribute map.",
      example = "")
  String getAttributesMap();

  @TemplateParameter.Text(
      order = 6,
      name = "idAttribute",
      optional = true,
      description =
          "The attribute on incoming Pub/Sub messages to use as a unique record identifier.",
      helpText =
          "The attribute on incoming Pub/Sub messages to use as a unique record identifier. When specified, the value of this attribute (which can be any string that uniquely identifies the record) will be used for deduplication of messages. If not provided, we cannot guarantee that no duplicate data will be delivered on the Pub/Sub stream. In this case, deduplication of the stream will be strictly best effort.",
      example = "")
  String getIdAttribute();

  @TemplateParameter.Text(
      order = 7,
      name = "timestampAttribute",
      optional = true,
      description = "Message value to use as element timestamp.",
      helpText =
          "Message value to use as element timestamp. If None, uses message  publishing time as the timestamp. Timestamp values should be in one of two formats: 1). A numerical value representing the number of milliseconds since the Unix epoch. 2). A string in RFC 3339 format, UTC timezone. Example: ``2015-10-29T23:41:41.123Z``. The sub-second component of the timestamp is optional, and digits beyond the first three (i.e., time units smaller than milliseconds) may be ignored.",
      example = "")
  String getTimestampAttribute();

  @TemplateParameter.Text(
      order = 8,
      name = "errorHandling",
      optional = true,
      description = "Error handling configuration",
      helpText = "This option specifies whether and where to output error rows.",
      example = "")
  String getErrorHandling();

  @TemplateParameter.Text(
      order = 9,
      name = "subscription",
      optional = true,
      description = "Pub/Sub subscription",
      helpText =
          "Pub/Sub subscription to read the input from. Exactly one of subscription or topic must be provided. Use this when you want to read from an existing subscription without creating a new one.",
      example = "projects/your-project-id/subscriptions/your-subscription-name")
  String getSubscription();

  @TemplateParameter.Text(
      order = 10,
      name = "language",
      optional = false,
      description = "Language used to define the expressions.",
      helpText =
          "The language used to define (and execute) the expressions and/or  callables in fields. Defaults to generic.",
      example = "")
  @Validation.Required
  String getLanguage();

  @TemplateParameter.Text(
      order = 11,
      name = "fields",
      optional = false,
      description = "Field mapping configuration",
      helpText =
          "The output fields to compute, each mapping to the expression or callable that creates them.",
      example = "")
  @Validation.Required
  String getFields();

  @TemplateParameter.Text(
      order = 12,
      name = "windowing",
      optional = true,
      description = "Windowing options",
      helpText =
          "Windowing options - see https://beam.apache.org/documentation/sdks/yaml/#windowing",
      example = "")
  String getWindowing();

  @TemplateParameter.Text(
      order = 13,
      name = "url",
      optional = false,
      description = "JDBC connection URL for AlloyDb",
      helpText =
          "The JDBC URL for connecting to AlloyDb instance. Format: jdbc:postgresql:///db?socketFactory=com.google.cloud.alloydb.SocketFactory&alloydbInstanceName=projects/<PROJECT>/locations/<REGION>/clusters/<CLUSTER>/instances/<INSTANCE>&alloydbIpType=PRIVATE",
      example =
          "jdbc:postgresql:///mydatabase?socketFactory=com.google.cloud.alloydb.SocketFactory&alloydbInstanceName=projects/my-project/locations/us-central1/clusters/my-cluster/instances/my-instance&alloydbIpType=PRIVATE")
  @Validation.Required
  String getUrl();

  @TemplateParameter.Text(
      order = 14,
      name = "username",
      optional = false,
      description = "AlloyDb database username",
      helpText = "Username for AlloyDb authentication",
      example = "postgres")
  @Validation.Required
  String getUsername();

  @TemplateParameter.Password(
      order = 15,
      name = "password",
      optional = false,
      description = "AlloyDb database password",
      helpText = "Password for AlloyDb authentication",
      example = "your-password")
  @Validation.Required
  String getPassword();

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
      name = "network",
      optional = true,
      description = "VPC Network name",
      helpText = "The VPC network where AlloyDB is located.",
      example = "default")
  String getNetwork();

  @TemplateParameter.Text(
      order = 18,
      name = "subnetwork",
      optional = true,
      description = "Subnetwork name",
      helpText = "The subnetwork for Dataflow workers. Required for custom VPCs.",
      example = "regions/us-central1/subnetworks/my-subnet")
  String getSubnetwork();

  @TemplateParameter.Text(
      order = 19,
      name = "table",
      optional = false,
      description = "Target table name in AlloyDb",
      helpText = "The name of the table where records will be written",
      example = "my_table")
  @Validation.Required
  String getTable();

  @TemplateParameter.Text(
      order = 20,
      name = "query",
      optional = false,
      description = "SQL query for writing records",
      helpText = "SQL query used to insert records into the JDBC sink.",
      example = "INSERT INTO table_name (col1, col2) VALUES (?, ?)")
  @Validation.Required
  String getQuery();

  @TemplateParameter.Integer(
      order = 21,
      name = "batchSize",
      optional = true,
      description = "Batch size for write operations",
      helpText = "Number of records to batch before writing to the database",
      example = "100")
  Integer getBatchSize();

  @TemplateParameter.Boolean(
      order = 22,
      name = "autoSharding",
      optional = true,
      description = "Enable autosharding for parallel writes",
      helpText = "If true, enables using a dynamically determined number of shards to write.",
      example = "True")
  Boolean getAutoSharding();

  @TemplateParameter.Text(
      order = 23,
      name = "outputDeadLetterPubSubTopic",
      optional = false,
      description = "Pub/Sub transformation error topic",
      helpText = "Pub/Sub error topic for failed transformation messages.",
      example = "projects/your-project-id/topics/your-error-topic-name")
  @Validation.Required
  String getOutputDeadLetterPubSubTopic();
}
