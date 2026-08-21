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
    name = "Pubsub_To_Iceberg_Yaml",
    category = TemplateCategory.STREAMING,
    type = Template.TemplateType.YAML,
    displayName = "Pub/Sub to Iceberg (YAML)",
    description =
        "The Pub/Sub to Iceberg template is a streaming pipeline that ingests data from a Pub/Sub topic or subscription and writes the records to an Apache Iceberg table.",
    flexContainerName = "pipeline-yaml",
    yamlTemplateFile = "PubsubToIceberg.yaml",
    filesToCopy = {
      "main.py",
      "requirements.txt",
      "options/pubsub_options.yaml",
      "options/iceberg_options.yaml"
    },
    documentation = "",
    contactInformation = "https://cloud.google.com/support",
    requirements = {
      "The input Pub/Sub topic or subscription must exist.",
      "The target Iceberg table must exist and be accessible through the provided catalog."
    },
    streaming = true,
    hidden = false)
public interface PubsubToIcebergYaml {

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
      helpText = "Pub/Sub subscription to read the input from.",
      example = "projects/your-project-id/subscriptions/your-subscription-name")
  String getSubscription();

  @TemplateParameter.Text(
      order = 10,
      name = "table",
      optional = false,
      description = "A fully-qualified table identifier.",
      helpText = "A fully-qualified table identifier, e.g., my_dataset.my_table.",
      example = "my_dataset.my_table")
  @Validation.Required
  String getTable();

  @TemplateParameter.Text(
      order = 11,
      name = "catalogName",
      optional = false,
      description = "Name of the catalog containing the table.",
      helpText = "The name of the Iceberg catalog that contains the table.",
      example = "my_hadoop_catalog")
  @Validation.Required
  String getCatalogName();

  @TemplateParameter.Text(
      order = 12,
      name = "catalogProperties",
      optional = false,
      description = "Properties used to set up the Iceberg catalog.",
      helpText = "A map of properties for setting up the Iceberg catalog.",
      example = "{\"type\": \"hadoop\", \"warehouse\": \"gs://your-bucket/warehouse\"}")
  @Validation.Required
  String getCatalogProperties();

  @TemplateParameter.Text(
      order = 13,
      name = "configProperties",
      optional = true,
      description = "Properties passed to the Hadoop Configuration.",
      helpText = "A map of properties to pass to the Hadoop Configuration.",
      example = "{\"fs.gs.impl\": \"com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem\"}")
  String getConfigProperties();

  @TemplateParameter.Text(
      order = 14,
      name = "drop",
      optional = true,
      description = "A list of field names to drop from the input record before writing.",
      helpText = "A list of field names to drop. Mutually exclusive with 'keep' and 'only'.",
      example = "[\"field_to_drop_1\", \"field_to_drop_2\"]")
  String getDrop();

  @TemplateParameter.Text(
      order = 15,
      name = "filter",
      optional = true,
      description = "An optional filter expression to apply to the input records.",
      helpText = "A filter expression to apply to records from the Iceberg table.",
      example = "age > 18")
  String getFilter();

  @TemplateParameter.Text(
      order = 16,
      name = "keep",
      optional = true,
      description = "A list of field names to keep in the input record.",
      helpText = "A list of field names to keep. Mutually exclusive with 'drop' and 'only'.",
      example = "[\"field_to_keep_1\", \"field_to_keep_2\"]")
  String getKeep();

  @TemplateParameter.Text(
      order = 17,
      name = "only",
      optional = true,
      description = "The name of a single record field that should be written.",
      helpText = "The name of a single field to write. Mutually exclusive with 'keep' and 'drop'.",
      example = "my_record_field")
  String getOnly();

  @TemplateParameter.Text(
      order = 18,
      name = "partitionFields",
      optional = true,
      description = "Fields used to create a partition spec for new tables.",
      helpText = "A list of fields and transforms for partitioning, e.g., ['day(ts)', 'category'].",
      example = "[\"day(ts)\", \"bucket(id, 4)\"]")
  String getPartitionFields();

  @TemplateParameter.Text(
      order = 19,
      name = "tableProperties",
      optional = true,
      description = "Iceberg table properties to be set on table creation.",
      helpText = "A map of Iceberg table properties to set when the table is created.",
      example = "{\"commit.retry.num-retries\": \"2\"}")
  String getTableProperties();

  @TemplateParameter.Integer(
      order = 20,
      name = "triggeringFrequencySeconds",
      optional = false,
      description = "For a streaming pipeline, the frequency at which snapshots are produced.",
      helpText = "The frequency in seconds for producing snapshots in a streaming pipeline.",
      example = "60")
  @Validation.Required
  Integer getTriggeringFrequencySeconds();

  @TemplateParameter.Text(
      order = 21,
      name = "sdfCheckpointAfterDuration",
      optional = true,
      description = "Dataflow Pipeline Option: Duration after which to checkpoint stateful DoFns.",
      helpText =
          "Duration after which to checkpoint stateful DoFns. For example: 30s. Documentation: https://docs.cloud.google.com/dataflow/docs/reference/service-options",
      example = "30s")
  @Default.String("30s")
  String getSdfCheckpointAfterDuration();

  @TemplateParameter.Integer(
      order = 22,
      name = "sdfCheckpointAfterOutputBytes",
      optional = true,
      description =
          "Dataflow Pipeline Option: Output bytes after which to checkpoint stateful DoFns.",
      helpText =
          "Output bytes after which to checkpoint stateful DoFns. For example: 536870912. Documentation: https://docs.cloud.google.com/dataflow/docs/reference/service-options",
      example = "536870912")
  @Default.Integer(536870912)
  Integer getSdfCheckpointAfterOutputBytes();
}
