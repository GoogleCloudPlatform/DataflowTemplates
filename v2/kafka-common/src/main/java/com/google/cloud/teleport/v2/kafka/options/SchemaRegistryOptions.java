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
package com.google.cloud.teleport.v2.kafka.options;

import com.google.cloud.teleport.metadata.TemplateParameter;
import com.google.cloud.teleport.v2.kafka.values.KafkaTemplateParamters;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptions;

public interface SchemaRegistryOptions extends PipelineOptions {
  @TemplateParameter.Enum(
      order = 1,
      name = "messageFormat",
      groupName = "Source",
      enumOptions = {
        @TemplateParameter.TemplateEnumOption(
            KafkaTemplateParamters.MessageFormatConstants.AVRO_CONFLUENT_WIRE_FORMAT),
        @TemplateParameter.TemplateEnumOption(
            KafkaTemplateParamters.MessageFormatConstants.AVRO_BINARY_ENCODING),
        @TemplateParameter.TemplateEnumOption(KafkaTemplateParamters.MessageFormatConstants.JSON)
      },
      description = "Message Format",
      helpText =
          "The Kafka message format. Can be AVRO_CONFLUENT_WIRE_FORMAT, AVRO_BINARY_ENCODING or JSON.")
  @Default.String(KafkaTemplateParamters.MessageFormatConstants.AVRO_CONFLUENT_WIRE_FORMAT)
  String getMessageFormat();

  void setMessageFormat(String value);

  @TemplateParameter.Enum(
      order = 2,
      name = "schemaFormat",
      groupName = "Source",
      parentName = "messageFormat",
      parentTriggerValues = {
        KafkaTemplateParamters.MessageFormatConstants.AVRO_CONFLUENT_WIRE_FORMAT
      },
      enumOptions = {
        @TemplateParameter.TemplateEnumOption(KafkaTemplateParamters.SchemaFormat.SCHEMA_REGISTRY),
        @TemplateParameter.TemplateEnumOption(
            KafkaTemplateParamters.SchemaFormat.SINGLE_SCHEMA_FILE),
      },
      description = "Schema Source",
      helpText =
          "The Kafka schema format. Can be provided as SINGLE_SCHEMA_FILE or SCHEMA_REGISTRY. "
              + "If SINGLE_SCHEMA_FILE is specified, all messages should have the schema mentioned in the avro schema file. "
              + "If SCHEMA_REGISTRY is specified, the messages can have either a single schema or multiple schemas.")
  @Default.String(KafkaTemplateParamters.SchemaFormat.SINGLE_SCHEMA_FILE)
  String getSchemaFormat();

  void setSchemaFormat(String value);

  @TemplateParameter.GcsReadFile(
      order = 3,
      groupName = "Source",
      parentName = "schemaFormat",
      parentTriggerValues = {KafkaTemplateParamters.SchemaFormat.SINGLE_SCHEMA_FILE},
      description = "Cloud Storage path to the Avro schema file",
      optional = true,
      helpText = "Cloud Storage path to Avro schema file. For example, gs://MyBucket/file.avsc.")
  @Default.String("")
  String getConfluentAvroSchemaPath();

  void setConfluentAvroSchemaPath(String schemaPath);

  @TemplateParameter.Text(
      order = 4,
      groupName = "Source",
      parentName = "schemaFormat",
      parentTriggerValues = {KafkaTemplateParamters.SchemaFormat.SCHEMA_REGISTRY},
      description = "Schema Registry Connection URL",
      optional = true,
      helpText = "Schema Registry Connection URL for a registry.")
  @Default.String("")
  String getSchemaRegistryConnectionUrl();

  void setSchemaRegistryConnectionUrl(String schemaRegistryConnectionUrl);

  @TemplateParameter.GcsReadFile(
      order = 5,
      groupName = "Source",
      parentName = "messageFormat",
      parentTriggerValues = {KafkaTemplateParamters.MessageFormatConstants.AVRO_BINARY_ENCODING},
      description = "Cloud Storage path to the Avro schema file",
      optional = true,
      helpText = "Cloud Storage path to Avro schema file. For example, gs://MyBucket/file.avsc.")
  @Default.String("")
  String getBinaryAvroSchemaPath();

  void setBinaryAvroSchemaPath(String schemaPath);
}
