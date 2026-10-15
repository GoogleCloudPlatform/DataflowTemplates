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
    name = "PubSub_Subscription_to_PubSub_Topic_Yaml",
    category = TemplateCategory.STREAMING,
    type = Template.TemplateType.YAML,
    displayName = "Pub/Sub subscription to Pub/Sub topic (YAML)",
    description =
        "The Pub/Sub subscription to Pub/Sub topic template is a streaming pipeline that reads messages from a Pub/Sub subscription and writes them to another Pub/Sub topic.",
    flexContainerName = "pipeline-yaml",
    yamlTemplateFile = "PubSubSubscriptionToPubSubTopic.yaml",
    filesToCopy = {"main.py", "requirements.txt", "options/pubsub_options.yaml"},
    documentation = "",
    contactInformation = "https://cloud.google.com/support",
    requirements = {
      "The input Pub/Sub subscription must exist prior to execution.",
      "The output Pub/Sub topic must exist prior to execution."
    },
    streaming = true,
    hidden = false)
public interface PubSubSubscriptionToPubSubTopicYaml {

  @TemplateParameter.Text(
      order = 1,
      name = "subscription",
      optional = false,
      description = "Pub/Sub input subscription",
      helpText = "Pub/Sub subscription to read the input from.",
      example = "projects/your-project-id/subscriptions/your-subscription-name")
  @Validation.Required
  String getSubscription();

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
          "Message value to use as element timestamp. If None, uses message publishing time as the timestamp.",
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
      name = "outputTopic",
      optional = false,
      description = "Output Pub/Sub topic",
      helpText = "The Pub/Sub topic to write the output to.",
      example = "projects/your-project-id/topics/your-topic-name")
  @Validation.Required
  String getOutputTopic();

  @TemplateParameter.Text(
      order = 10,
      name = "outputFormat",
      optional = true,
      description = "Output message format.",
      helpText = "The output message format. One of: AVRO, JSON, PROTO, RAW, or STRING.",
      example = "JSON")
  @Default.String("JSON")
  String getOutputFormat();

  @TemplateParameter.Text(
      order = 11,
      name = "outputSchema",
      optional = true,
      description = "Output data schema.",
      helpText = "Schema specification for the output format, if applicable.",
      example = "{\"type\": \"object\", \"properties\": {\"field1\": {\"type\": \"string\"}}}")
  String getOutputSchema();

  @TemplateParameter.Text(
      order = 12,
      name = "outputAttributes",
      optional = true,
      description = "List of attribute keys for output messages.",
      helpText =
          "List of attribute keys whose values will be pulled out as Pub/Sub message attributes.",
      example = "[\"attr1\", \"attr2\"]")
  String getOutputAttributes();

  @TemplateParameter.Text(
      order = 13,
      name = "outputAttributesMap",
      optional = true,
      description = "Name of a string-to-string map field for output attributes.",
      helpText =
          "Name of a string-to-string map field in which to pull a set of attributes associated with this message.",
      example = "attrs")
  String getOutputAttributesMap();

  @TemplateParameter.Text(
      order = 14,
      name = "outputIdAttribute",
      optional = true,
      description = "Attribute name for unique record identifier on output messages.",
      helpText =
          "If set, will set an attribute for each Cloud Pub/Sub message with the given name and a unique value.",
      example = "id")
  String getOutputIdAttribute();

  @TemplateParameter.Text(
      order = 15,
      name = "outputTimestampAttribute",
      optional = true,
      description = "Attribute name for publish timestamp on output messages.",
      helpText =
          "If set, will set an attribute for each Cloud Pub/Sub message with the given name and the message's publish time as the value.",
      example = "timestamp")
  String getOutputTimestampAttribute();
}
