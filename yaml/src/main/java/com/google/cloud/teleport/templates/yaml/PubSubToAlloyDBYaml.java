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
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Validation;

@Template(
    name = "PubSub_To_AlloyDB_Yaml",
    category = TemplateCategory.STREAMING,
    type = Template.TemplateType.YAML,
    displayName = "Pub/Sub to AlloyDB (YAML)",
    description =
        "The Pub/Sub to AlloyDB template is a streaming pipeline which ingests data from a Pub/Sub topic and writes the resulting records to AlloyDB.",
    flexContainerName = "pipeline-yaml",
    yamlTemplateFile = "PubSubToAlloyDB.yaml",
    filesToCopy = {
      "main.py",
      "requirements.txt",
      "options/pubsub_options.yaml",
      "options/alloydb_options.yaml",
      "options/windowing_options.yaml"
    },
    documentation = "",
    contactInformation = "https://cloud.google.com/support",
    requirements = {
      "The input Pub/Sub topic must exist.",
      "The output AlloyDB instance and target table must exist."
    },
    streaming = true,
    hidden = false)
public interface PubSubToAlloyDBYaml {

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
      name = "format",
      optional = true,
      description = "The message format.",
      helpText = "The message format. One of: AVRO, JSON, PROTO, RAW, or STRING.",
      example = "JSON")
  @Default.String("JSON")
  String getFormat();

  @TemplateParameter.Text(
      order = 3,
      name = "schema",
      optional = true,
      description = "Data schema.",
      helpText =
          "A schema is required if data format is JSON, AVRO or PROTO. For JSON, this is a JSON schema. For AVRO and PROTO, this is the full schema definition.",
      example = "{\"type\": \"object\", \"properties\": {\"field1\": {\"type\": \"string\"}}}")
  String getSchema();

  @TemplateParameter.Text(
      order = 4,
      name = "attributes",
      optional = true,
      description = "List of attribute keys.",
      helpText =
          "List of attribute keys whose values will be flattened into the output message as additional fields. For example, if the format is `raw` and attributes is `[a, b]` then this read will produce elements of the form `Row(payload=..., a=..., b=...)`.",
      example = "[\"attr1\", \"attr2\"]")
  String getAttributes();

  @TemplateParameter.Text(
      order = 5,
      name = "attributesMap",
      optional = true,
      description = "Name of a field in which to store the full set of attributes.",
      helpText =
          "Name of a field in which to store the full set of attributes associated with this message. For example, if the format is `raw` and `attributes_map` is set to `attrs` then this read will produce elements of the form `Row(payload=..., attrs=...)` where `attrs` is a Map type of string to string. If both `attributes` and `attributes_map` are set, the overlapping attribute values will be present in both the flattened structure and the attribute map.",
      example = "attrs")
  String getAttributesMap();

  @TemplateParameter.Text(
      order = 6,
      name = "idAttribute",
      optional = true,
      description =
          "The attribute on incoming Pub/Sub messages to use as a unique record identifier.",
      helpText =
          "The attribute on incoming Pub/Sub messages to use as a unique record identifier. When specified, the value of this attribute (which can be any string that uniquely identifies the record) will be used for deduplication of messages. If not provided, we cannot guarantee that no duplicate data will be delivered on the Pub/Sub stream. In this case, deduplication of the stream will be strictly best effort.",
      example = "id")
  String getIdAttribute();

  @TemplateParameter.Text(
      order = 7,
      name = "timestampAttribute",
      optional = true,
      description = "Message value to use as element timestamp.",
      helpText =
          "Message value to use as element timestamp. If None, uses message  publishing time as the timestamp. Timestamp values should be in one of two formats: 1). A numerical value representing the number of milliseconds since the Unix epoch. 2). A string in RFC 3339 format, UTC timezone. Example: ``2015-10-29T23:41:41.123Z``. The sub-second component of the timestamp is optional, and digits beyond the first three (i.e., time units smaller than milliseconds) may be ignored.",
      example = "timestamp")
  String getTimestampAttribute();

  @TemplateParameter.Text(
      order = 8,
      name = "publishTimeField",
      optional = true,
      description = "Field to add to output messages with the Pub/Sub message publish time.",
      helpText =
          "Field to add to output messages with the Pub/Sub message publish time. If None, no such field is added.",
      example = "publish_time")
  String getPublishTimeField();

  @TemplateParameter.Text(
      order = 9,
      name = "jdbcUrl",
      optional = false,
      description = "Connection URL for the JDBC source/sink.",
      helpText = "The JDBC connection URL.",
      example = "jdbc:postgresql://your-host:5432/your-db")
  @Validation.Required
  String getJdbcUrl();

  @TemplateParameter.Text(
      order = 10,
      name = "username",
      optional = true,
      description = "Username for the JDBC connection.",
      helpText = "The database username.",
      example = "my_user")
  String getUsername();

  @TemplateParameter.Password(
      order = 11,
      name = "password",
      optional = true,
      description = "Password for the JDBC connection.",
      helpText = "The database password.",
      example = "my_secret_password")
  String getPassword();

  @TemplateParameter.Text(
      order = 12,
      name = "connectionProperties",
      optional = true,
      description = "JDBC connection properties.",
      helpText = "A semicolon-separated list of key-value pairs for the JDBC connection.",
      example = "key1=value1;key2=value2")
  String getConnectionProperties();

  @TemplateParameter.Text(
      order = 13,
      name = "alloydbTable",
      optional = true,
      description = "The name of the AlloyDB table.",
      helpText = "The name of the database table.",
      example = "public.my_table")
  String getAlloydbTable();

  @TemplateParameter.Text(
      order = 14,
      name = "query",
      optional = true,
      description = "The SQL query/statement to execute.",
      helpText = "The SQL query/statement to execute on the source/sink.",
      example = "SELECT * FROM my_table WHERE status = 'active'")
  String getQuery();

  @TemplateParameter.Integer(
      order = 15,
      name = "batchSize",
      optional = true,
      description = "The number of records to group for each write operation.",
      helpText = "The number of records to group together for each write.",
      example = "1000")
  @Default.Integer(1000)
  Integer getBatchSize();

  @TemplateParameter.Boolean(
      order = 16,
      name = "autoSharding",
      optional = true,
      description = "If true, enables using a dynamically determined number of shards to write.",
      helpText = "If true, a dynamic number of shards will be used for writing.",
      example = "False")
  Boolean getAutoSharding();

  @TemplateParameter.Text(
      order = 17,
      name = "type",
      optional = true,
      description = "The type of windowing to apply.",
      helpText =
          "The type of windowing to apply (e.g., fixed, sliding, sessions, global). Defaults to fixed.",
      example = "fixed")
  @Default.String("fixed")
  String getType();

  @TemplateParameter.Integer(
      order = 18,
      name = "size",
      optional = true,
      description = "The window size in seconds.",
      helpText = "The window size in seconds. Defaults to 60.",
      example = "60")
  @Default.Integer(60)
  Integer getSize();
}
