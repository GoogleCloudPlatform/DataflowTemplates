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
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Validation;

@Template(
    name = "PubSub_To_BigTable_Yaml",
    category = TemplateCategory.STREAMING,
    type = Template.TemplateType.YAML,
    displayName = "PubSub to BigTable (YAML)",
    description =
        "The PubSub to BigTable template is a streaming pipeline which ingests data from a PubSub topic, executes a user-defined mapping, and writes the resulting records to BigTable. Any errors which occur in the transformation of the data are written to a separate Pub/Sub topic.",
    flexContainerName = "pubsub-to-bigtable-yaml",
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
      optional = true,
      description = "The message format.",
      helpText = "The message format. One of: AVRO, JSON, PROTO, RAW, or STRING.",
      example = "")
  @Default.String("JSON")
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
      name = "language",
      optional = false,
      description = "Language used to define the expressions.",
      helpText =
          "The language used to define (and execute) the expressions and/or  callables in fields. Defaults to generic.",
      example = "")
  @Validation.Required
  String getLanguage();

  @TemplateParameter.Text(
      order = 5,
      name = "fields",
      optional = false,
      description = "Field mapping configuration",
      helpText =
          "The output fields to compute, each mapping to the expression or callable that creates them.",
      example = "")
  @Validation.Required
  String getFields();

  @TemplateParameter.Text(
      order = 6,
      name = "project_id",
      optional = false,
      description = "BigTable project ID",
      helpText = "The Google Cloud project ID of the BigTable instance.",
      example = "")
  @Validation.Required
  String getProject_id();

  @TemplateParameter.Text(
      order = 7,
      name = "instance_id",
      optional = false,
      description = "BigTable instance ID",
      helpText = "The BigTable instance ID.",
      example = "")
  @Validation.Required
  String getInstance_id();

  @TemplateParameter.Text(
      order = 8,
      name = "table_id",
      optional = false,
      description = "BigTable output table",
      helpText = "BigTable table ID to write the output to.",
      example = "")
  @Validation.Required
  String getTable_id();

  @TemplateParameter.Text(
      order = 9,
      name = "outputDeadLetterPubSubTopic",
      optional = false,
      description = "Pub/Sub transformation error topic",
      helpText = "Pub/Sub error topic for failed transformation messages.",
      example = "projects/your-project-id/topics/your-error-topic-name")
  @Validation.Required
  String getOutputDeadLetterPubSubTopic();
}
