/*
 * Copyright (C) 2024 Google LLC
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
    name = "Kafka_to_BigQuery_Yaml",
    category = TemplateCategory.STREAMING,
    type = Template.TemplateType.YAML,
    displayName = "Kafka to BigQuery (YAML)",
    description =
        "The Apache Kafka to BigQuery template is a streaming pipeline which ingests text data from Apache Kafka, executes a user-defined function (UDF), and outputs the resulting records to BigQuery. "
            + "Any errors which occur in the transformation of the data, execution of the UDF, or inserting into the output table are inserted into a separate errors table in BigQuery. "
            + "If the errors table does not exist prior to execution, then it is created.",
    flexContainerName = "kafka-to-bigquery-yaml",
    yamlTemplateFile = "KafkaToBigQuery.yaml",
    filesToCopy = {"template.yaml", "main.py", "requirements.txt"},
    documentation =
        "https://cloud.google.com/dataflow/docs/guides/templates/provided/kafka-to-bigquery",
    contactInformation = "https://cloud.google.com/support",
    requirements = {
      "The output BigQuery table must exist.",
      "The Apache Kafka broker server must be running and be reachable from the Dataflow worker machines.",
      "The Apache Kafka topics must exist and the messages must be encoded in a valid JSON format."
    },
    streaming = true,
    hidden = true)
public interface KafkaToBigQueryYaml {
  @TemplateParameter.Text(
      order = 1,
      name = "readBootstrapServers",
      optional = false,
      description = "Kafka Bootstrap Server list",
      helpText =
          "Kafka Bootstrap Server list, separated by commas. This "
              + "parameter should be provided either through this parameter or jinjaVariables.",
      example = "localhost:9092,127.0.0.1:9093")
  @Validation.Required
  String getReadBootstrapServers();

  @TemplateParameter.Text(
      order = 2,
      name = "kafkaReadTopics",
      optional = false,
      description = "Kafka topic(s) to read input from.",
      helpText =
          "Kafka topic(s) to read input from. This parameter should be "
              + "provided either through this parameter or jinjaVariables.\",",
      example = "topic1,topic2")
  @Validation.Required
  String getKafkaReadTopics();

  @TemplateParameter.Text(
      order = 3,
      name = "outputTableSpec",
      optional = false,
      description = "BigQuery output table",
      helpText =
          "BigQuery table location to write the output to. The name should be in the format "
              + "`<project>:<dataset>.<table_name>`. The table's schema must match input objects."
              + "This parameter should be provided either through this parameter or jinjaVariables.")
  @Validation.Required
  String getOutputTableSpec();

  @TemplateParameter.Text(
      order = 4,
      name = "outputDeadletterTable",
      optional = false,
      description = "The dead-letter table name to output failed messages to BigQuery",
      helpText =
          "BigQuery table for failed messages. Messages failed to reach the output table for different reasons "
              + "(e.g., mismatched schema, malformed json) are written to this table. If it doesn't exist, it will"
              + " be created during pipeline execution. If not specified, \"outputTableSpec_error_records\" is used instead."
              + "This parameter should be provided either through this parameter or jinjaVariables.",
      example = "your-project-id:your-dataset.your-table-name")
  @Validation.Required
  String getOutputDeadletterTable();

  @TemplateParameter.Text(
      order = 5,
      name = "messageFormat",
      optional = true,
      description = "The message format",
      helpText =
          "The message format. One of: AVRO, JSON, PROTO, RAW, or STRING."
              + "This parameter should be provided either through this parameter or jinjaVariables.\",")
  @Default.String("JSON")
  String getMessageFormat();

  @TemplateParameter.Text(
      order = 6,
      name = "schema",
      optional = false,
      description = "Kafka schema.",
      helpText = "Kafka schema. A schema is required if data format is JSON, AVRO or PROTO.")
  String getSchema();

  @TemplateParameter.Integer(
      order = 7,
      name = "numStorageWriteApiStreams",
      optional = true,
      description = "Number of streams for BigQuery Storage Write API",
      helpText =
          "Number of streams defines the parallelism of the BigQueryIO’s Write transform and"
              + " roughly corresponds to the number of Storage Write API’s streams which will be"
              + " used by the pipeline. See"
              + " https://cloud.google.com/blog/products/data-analytics/streaming-data-into-bigquery-using-storage-write-api"
              + " for the recommended values. The default value is 1.")
  @Default.Integer(1)
  Integer getNumStorageWriteApiStreams();

  @TemplateParameter.Integer(
      order = 8,
      name = "storageWriteApiTriggeringFrequencySec",
      optional = true,
      description = "Triggering frequency in seconds for BigQuery Storage Write API",
      helpText =
          "Triggering frequency will determine how soon the data will be visible for querying in"
              + " BigQuery. See"
              + " https://cloud.google.com/blog/products/data-analytics/streaming-data-into-bigquery-using-storage-write-api"
              + " for the recommended values. The default value is 5.")
  @Default.Integer(5)
  Integer getStorageWriteApiTriggeringFrequencySec();
}
