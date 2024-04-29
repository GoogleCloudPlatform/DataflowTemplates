/*
 * Copyright (C) 2019 Google LLC
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
import com.google.cloud.teleport.metadata.TemplateParameter.TemplateEnumOption;
import com.google.cloud.teleport.v2.transforms.WriteToGCSAvro;
import com.google.cloud.teleport.v2.transforms.WriteToGCSParquet;
import com.google.cloud.teleport.v2.transforms.WriteToGCSText;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptions;

/**
 * The {@link KafkaToGCSOptions} interface provides the custom execution options passed by the
 * executor at the command-line.
 */
public interface KafkaToGCSOptions
    extends PipelineOptions,
        DataflowPipelineOptions,
        WriteToGCSText.WriteToGCSTextOptions,
        WriteToGCSParquet.WriteToGCSParquetOptions,
        WriteToGCSAvro.WriteToGCSAvroOptions {

  @TemplateParameter.Text(
      order = 1,
      optional = false,
      regexes = {"[,:a-zA-Z0-9._-]+"},
      description = "Kafka Bootstrap Server list",
      helpText = "Kafka Bootstrap Server list, separated by commas.",
      example = "localhost:9092,127.0.0.1:9093")
  //  @Validation.Required
  String getBootstrapServers();

  void setBootstrapServers(String bootstrapServers);

  @TemplateParameter.Text(
      order = 2,
      optional = false,
      regexes = {"[,a-zA-Z0-9._-]+"},
      description = "Kafka topic(s) to read the input from",
      helpText = "Kafka topic(s) to read the input from.",
      example = "topic1,topic2")
  //  @Validation.Required
  String getInputTopics();

  void setInputTopics(String inputTopics);

  @TemplateParameter.Enum(
      order = 3,
      groupName = "MessageFormat",
      enumOptions = {
        @TemplateEnumOption("TEXT"),
        @TemplateEnumOption("AVRO"),
        @TemplateEnumOption("PARQUET")
      },
      optional = false,
      description = "File format of the desired output files. (TEXT, AVRO or PARQUET)",
      helpText =
          "The file format of the desired output files. Can be TEXT, AVRO or PARQUET. Defaults to TEXT")
  @Default.String("TEXT")
  String getOutputFileFormat();

  void setOutputFileFormat(String outputFileFormat);

  @TemplateParameter.Duration(
      order = 4,
      optional = true,
      description = "Window duration",
      helpText =
          "The window duration/size in which data will be written to Cloud Storage. Allowed formats are: Ns (for "
              + "seconds, example: 5s), Nm (for minutes, example: 12m), Nh (for hours, example: 2h).",
      example = "5m")
  @Default.String("5m")
  String getWindowDuration();

  void setWindowDuration(String windowDuration);

  @TemplateParameter.Text(
      order = 5,
      groupName = "MessageFormat",
      optional = true,
      description = "Schema Registry URL for Confluent wire format messages.",
      helpText =
          "Schema Registry URL used to fetch schemas to convert the Kafka messages to Avro Records.")
  String getSchemaRegistryURL();

  void setSchemaRegistryURL(String schemaRegistryURL);

  @TemplateParameter.Text(
      order = 6,
      optional = true,
      groupName = "MessageFormat",
      description = "Specify an Avro schema path",
      example = "gs://<bucket_name>/schema1.avsc",
      helpText = "Specify an Avro Schema Path.")
  String getSchemaPath();

  void setSchemaPath(String schema);

  @TemplateParameter.Enum(
      order = 7,
      groupName = "MessageFormat",
      enumOptions = {
        @TemplateEnumOption("CONFLUENT_WIRE_FORMAT"),
        @TemplateEnumOption("AVRO_BINARY_ENCODING"),
        @TemplateEnumOption("AVRO_SINGLE_OBJECT_ENCODING")
      },
      optional = true,
      description = "Messaging format for the incoming Kafka Messages",
      helpText =
          "Messaging format for the incoming Kafka messages. For Confluent wire format messages",
      example = "CONFLUENT_WIRE_FORMAT")
  @Default.String("CONFLUENT_WIRE_FORMAT")
  String getMessageFormat();

  void setMessageFormat(String messageFormat);

  @TemplateParameter.Text(
      order = 7,
      groupName = "Kafka SASL_PLAIN Authentication parameter",
      description =
          "Username to be used with SASL_PLAIN mechanism for Kafka, stored in Google Cloud Secret Manager",
      helpText =
          "Secret Manager secret ID for the SASL_PLAIN username. Should be in the format projects/{project}/secrets/{secret}/versions/{secret_version}",
      example = "projects/your-project-id/secrets/your-secret/versions/your-secret-version")
  String getUserNameSecretID();

  void setUserNameSecretID(String userNameSecretID);

  @TemplateParameter.Text(
      order = 8,
      groupName = "Kafka SASL_PLAIN Authentication parameter",
      description =
          "Password to be used with SASL_PLAIN mechanism for Kafka, stored in Google Cloud Secret Manager",
      helpText =
          "Secret Manager secret ID for the SASL_PLAIN password. Should be in the format projects/{project}/secrets/{secret}/versions/{secret_version}",
      example = "projects/your-project-id/secrets/your-secret/versions/your-secret-version")
  String getPasswordSecretID();

  void setPasswordSecretID(String passwordSecretID);

  @TemplateParameter.Enum(
      order = 9,
      description = "Set Kafka offset",
      enumOptions = {
        @TemplateEnumOption("latest"),
        @TemplateEnumOption("earliest"),
        @TemplateEnumOption("none")
      },
      helpText = "Set the Kafka offset to earliest or latest(default)",
      optional = true)
  @Default.String("latest")
  String getOffset();

  void setOffset(String offset);
}
