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
    name = "PubSub_To_BigTable_Yaml",
    category = TemplateCategory.STREAMING,
    type = Template.TemplateType.YAML,
    displayName = "PubSub to BigTable (YAML)",
    description =
        "The PubSub to BigTable template is a streaming pipeline which ingests data from a PubSub topic, executes a user-defined mapping, and writes the resulting records to BigTable. Any errors which occur in the transformation of the data are written to a separate Pub/Sub topic.",
    flexContainerName = "pipeline-yaml",
    yamlTemplateFile = "PubSubToBigTable.yaml",
    filesToCopy = {"PubSubToBigTable.yaml", "main.py", "requirements.txt"},
    documentation =
        "https://cloud.google.com/dataflow/docs/guides/templates/provided-yaml/pubsub-to-bigtable",
    contactInformation = "https://cloud.google.com/support",
    requirements = {
      "The input Pub/Sub topic must exist.",
      "The mapToField Pub/Sub error topic must exist.",
      "The output BigTable table must exist."
    },
    streaming = true,
    hidden = false)
public interface PubSubToBigTableYaml {

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
      name = "projectId",
      optional = false,
      description = "BigTable project ID",
      helpText = "The Google Cloud project ID of the BigTable instance.",
      example = "")
  @Validation.Required
  String getProjectId();

  @TemplateParameter.Text(
      order = 13,
      name = "instanceId",
      optional = false,
      description = "BigTable instance ID",
      helpText = "The BigTable instance ID.",
      example = "")
  @Validation.Required
  String getInstanceId();

  @TemplateParameter.Text(
      order = 14,
      name = "tableId",
      optional = false,
      description = "BigTable output table",
      helpText = "BigTable table ID to write the output to.",
      example = "")
  @Validation.Required
  String getTableId();

  @TemplateParameter.Text(
      order = 15,
      name = "windowing",
      optional = true,
      description = "Windowing options",
      helpText =
          "Windowing options - see https://beam.apache.org/documentation/sdks/yaml/#windowing",
      example = "")
  String getWindowing();

  @TemplateParameter.Text(
      order = 16,
      name = "outputDeadLetterPubSubTopic",
      optional = false,
      description = "Pub/Sub transformation error topic",
      helpText = "Pub/Sub error topic for failed transformation messages.",
      example = "projects/your-project-id/topics/your-error-topic-name")
  @Validation.Required
  String getOutputDeadLetterPubSubTopic();
}
