/*
 * Copyright (C) 2023 Google LLC
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
import com.google.cloud.teleport.v2.bigtable.options.BigtableCommonOptions;
import com.google.cloud.teleport.v2.templates.bigtablechangestreamstopubsub.model.MessageEncoding;
import com.google.cloud.teleport.v2.templates.bigtablechangestreamstopubsub.model.MessageFormat;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Validation;

/**
 * The {@link BigtableChangeStreamsToPubSubOptions} class provides the custom execution options
 * passed by the executor at the command-line.
 */
public interface BigtableChangeStreamsToPubSubOptions
    extends DataflowPipelineOptions, BigtableCommonOptions.ReadChangeStreamOptions {

  @TemplateParameter.Text(
      order = 1,
      groupName = "Target",
      description = "The output Pub/Sub topic name",
      helpText = "The name of the destination Pub/Sub topic.")
  @Validation.Required
  String getPubSubTopic();

  void setPubSubTopic(String pubSubTopic);

  @TemplateParameter.Enum(
      order = 2,
      enumOptions = {
        @TemplateEnumOption("BINARY"),
        @TemplateEnumOption("JSON"),
      },
      optional = true,
      description = "The encoding of the message written into PubSub",
      helpText =
          "The encoding of the messages to be published to the Pub/Sub topic. When the schema of the "
              + "destination topic is configured, the message encoding is determined by the topic settings. The following values are supported: `BINARY` and `JSON`. Defaults to `JSON`.")
  @Default.Enum("JSON")
  MessageEncoding getMessageEncoding();

  void setMessageEncoding(MessageEncoding messageEncoding);

  @TemplateParameter.Enum(
      order = 3,
      enumOptions = {
        @TemplateEnumOption("AVRO"),
        @TemplateEnumOption("PROTOCOL_BUFFERS"),
        @TemplateEnumOption("JSON"),
      },
      optional = true,
      description = "The format of the message written into PubSub",
      helpText =
          "The encoding of the messages to publish to the Pub/Sub topic. When the schema of the destination topic is configured, the message encoding is determined by the topic settings. "
              + "The following values are supported: `AVRO`, `PROTOCOL_BUFFERS`, and `JSON`. The default value is `JSON`. When the `JSON` format is used, "
              + "the rowKey, column, and value fields of the message are strings, the contents of which are determined by the pipeline options `useBase64Rowkeys`, `useBase64ColumnQualifiers`, `useBase64Values`, and `bigtableChangeStreamCharset`.")
  @Default.Enum("JSON")
  MessageFormat getMessageFormat();

  void setMessageFormat(MessageFormat messageFormat);

  @TemplateParameter.Boolean(
      order = 4,
      optional = true,
      description = "Strip values for SetCell mutation",
      helpText =
          "When set to `true`, the `SET_CELL` mutations are returned without new values set. Defaults to `false`. This parameter is useful when you don't need a new value to be present, also known as cache invalidation, or when values are extremely large and exceed Pub/Sub message size limits.")
  @Default.Boolean(false)
  Boolean getStripValues();

  void setStripValues(Boolean stripValues);

  @TemplateParameter.GcsWriteFolder(
      order = 5,
      optional = true,
      description = "Dead letter queue directory to store any unpublished change record.",
      helpText =
          "The directory for the dead-letter queue. Records that fail to be processed are stored in this directory. Defaults to "
              + "a directory under the Dataflow job temp location. In most cases, you can use the default path.")
  @Default.String("")
  String getDlqDirectory();

  void setDlqDirectory(String dlqDirectory);

  @TemplateParameter.Integer(
      order = 6,
      optional = true,
      description = "Dead letter queue retry minutes",
      helpText = "The number of minutes between dead-letter queue retries. Defaults to `10`.")
  @Default.Integer(10)
  Integer getDlqRetryMinutes();

  void setDlqRetryMinutes(Integer dlqRetryMinutes);

  @TemplateParameter.Integer(
      order = 7,
      optional = true,
      description = "Dead letter maximum retries",
      helpText = "The dead letter maximum retries. Defaults to `5`.")
  @Default.Integer(5)
  Integer getDlqMaxRetries();

  void setDlqMaxRetries(Integer dlqMaxRetries);

  @TemplateParameter.ProjectId(
      order = 9,
      groupName = "Target",
      optional = true,
      description = "PubSub project ID",
      helpText = "The Bigtable project ID. The default is the project of the Dataflow job.")
  @Default.String("")
  String getPubSubProjectId();

  void setPubSubProjectId(String pubSubProjectId);

  @TemplateParameter.Boolean(
      order = 8,
      optional = true,
      parentName = "messageFormat",
      parentTriggerValues = {"JSON"},
      description = "Write Base64-encoded row keys",
      helpText =
          "Used with JSON message encoding. When set to `true`, the `rowKey` field is a Base64-encoded string. Otherwise, the `rowKey`"
              + " is produced by using `bigtableChangeStreamCharset` to decode bytes into a string. Defaults to `false`.")
  @Default.Boolean(false)
  Boolean getUseBase64Rowkeys();

  void setUseBase64Rowkeys(Boolean useBase64Rowkeys);

  @TemplateParameter.Boolean(
      order = 9,
      optional = true,
      parentName = "messageFormat",
      parentTriggerValues = {"JSON"},
      description = "Write Base64-encoded column qualifiers",
      helpText =
          "Used with JSON message encoding. When set to `true`, the `column` field is a Base64-encoded string. Otherwise, the column "
              + "is produced by using `bigtableChangeStreamCharset` to decode bytes into a string. Defaults to `false`.")
  @Default.Boolean(false)
  Boolean getUseBase64ColumnQualifiers();

  void setUseBase64ColumnQualifiers(Boolean useBase64ColumnQualifiers);

  @TemplateParameter.Boolean(
      order = 10,
      optional = true,
      parentName = "messageFormat",
      parentTriggerValues = {"JSON"},
      description = "Write Base64-encoded values",
      helpText =
          "Used with JSON message encoding. When set to `true`, the value field is a Base64-encoded string. Otherwise, the value is"
              + "produced by using `bigtableChangeStreamCharset` to decode bytes into a string. Defaults to `false`.")
  @Default.Boolean(false)
  Boolean getUseBase64Values();

  void setUseBase64Values(Boolean useBase64Values);

  @TemplateParameter.Boolean(
      order = 11,
      optional = true,
      description = "Whether or not to disable retries for the DLQ",
      helpText = "Whether or not to disable retries for the DLQ")
  @Default.Boolean(false)
  boolean getDisableDlqRetries();

  void setDisableDlqRetries(boolean value);
}
