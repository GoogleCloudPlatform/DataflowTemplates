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
import com.google.cloud.teleport.v2.bigtable.options.BigtableCommonOptions;
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
      description = "The output Pub/Sub topic",
      helpText = "The Pub/Sub topic to publish PubSubMessage.")
  @Validation.Required
  String getPubSubTopic();

  void setPubSubTopic(String pubSubTopic);

  @TemplateParameter.Text(
      order = 2,
      optional = true,
      description = "The encoding of the message written into PubSub",
      helpText =
          "The format of the message to be written into PubSub. "
              + "Allowed formats are BINARY and JSON Text. Default value is JSON.")
  @Default.String("JSON")
  String getMessageEncoding();

  void setMessageEncoding(String messageEncoding);

  @TemplateParameter.Text(
      order = 3,
      optional = true,
      description = "The format of the message written into PubSub",
      helpText =
          "The message format chosen for outputting data to PubSub. "
              + "Allowed formats are AVRO, PROTOCOL_BUFFER and JSON Text. Default value is JSON.")
  @Default.String("JSON")
  String getMessageFormat();

  void setMessageFormat(String messageFormat);

  @TemplateParameter.Boolean(
      order = 4,
      optional = true,
      description = "Strip value for SetCell mutation",
      helpText =
          "Strip value for SetCell mutation. If true the SetCell mutation message won’t include the value written.")
  @Default.Boolean(false)
  Boolean getStripValue();

  void setStripValue(Boolean stripValue);

  @TemplateParameter.GcsWriteFolder(
      order = 5,
      optional = true,
      description = "Dead letter queue directory to store any unpublished change record.",
      helpText =
          "The file path to store any unprocessed records with"
              + " the reason they failed to be processed. "
              + "Default is a directory under the Dataflow job's temp location. "
              + "The default value is enough under most conditions.")
  @Default.String("")
  String getDlqDirectory();

  void setDlqDirectory(String value);

  @TemplateParameter.Integer(
      order = 6,
      optional = true,
      description = "Dead letter queue retry minutes",
      helpText = "The number of minutes between dead letter queue retries. Defaults to 10.")
  @Default.Integer(10)
  Integer getDlqRetryMinutes();

  void setDlqRetryMinutes(Integer value);

  @TemplateParameter.Integer(
      order = 7,
      optional = true,
      description = "Dead letter maximum retries",
      helpText = "The number of attempts to process change stream mutations. Defaults to 5.")
  @Default.Integer(5)
  Integer getDlqMaxRetries();

  void setDlqMaxRetries(Integer value);

  @TemplateParameter.Text(
      order = 8,
      optional = true,
      description = "Pub/Sub API",
      helpText =
          "Pub/Sub API used to implement the pipeline. Allowed APIs are pubsubio and native_client."
              + " Default is pubsubio. For a small QPS, native_client can achieve a smaller latency"
              + " than pubsubio. For a large QPS, pubsubio has better and more stable performance.")
  @Default.String("pubsubio")
  String getPubSubAPI();

  void setPubSubAPI(String pubSubAPI);

  @TemplateParameter.ProjectId(
      order = 9,
      optional = true,
      description = "PubSub project ID",
      helpText = "The PubSub Project. Default is the project for the Dataflow job.")
  @Default.String("")
  String getPubSubProjectId();

  void setPubSubProjectId(String value);

  @TemplateParameter.Boolean(
      order = 8,
      optional = true,
      description = "Write Base64-encoded rowkeys",
      helpText =
          "Only supported for the TEXT output file format. When set to true, rowkeys will be written as Base64-encoded strings. Otherwise bigtableChangeStreamCharset charset will be used to decode binary values into String rowkeys"
              + "Defaults to false.")
  @Default.Boolean(false)
  Boolean getUseBase64Rowkey();

  void setUseBase64Rowkey(Boolean useBase64Rowkey);

  @TemplateParameter.Boolean(
      order = 9,
      optional = true,
      description = "Write Base64-encoded column qualifiers",
      helpText =
          "Only supported for the TEXT output file format. When set to true, column qualifiers will be written as Base64-encoded strings. Otherwise bigtableChangeStreamCharset charset will be used to decode binary values into String column qualifiers"
              + "Defaults to false.")
  @Default.Boolean(false)
  Boolean getUseBase64ColumnQualifier();

  void setUseBase64ColumnQualifier(Boolean useBase64ColumnQualifier);

  @TemplateParameter.Boolean(
      order = 10,
      optional = true,
      description = "Write Base64-encoded value",
      helpText =
          "Only supported for the TEXT output file format. When set to true, values will be written as Base64-encoded strings. Otherwise bigtableChangeStreamCharset charset will be used to decode binary values into String values"
              + "Defaults to false.")
  @Default.Boolean(false)
  Boolean getUseBase64Value();

  void setUseBase64Value(Boolean useBase64Value);
}
