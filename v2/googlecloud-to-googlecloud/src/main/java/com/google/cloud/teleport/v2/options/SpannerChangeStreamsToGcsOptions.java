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

import com.google.cloud.spanner.Options.RpcPriority;
import com.google.cloud.teleport.v2.transforms.WriteDataChangeRecordsToGcsAvro;
import com.google.cloud.teleport.v2.transforms.WriteDataChangeRecordsToGcsText;
import com.google.cloud.teleport.v2.utils.WriteToGCSUtility.FileFormat;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation;

/**
 * The {@link SpannerChangeStreamsToGcsOptions} interface provides the custom execution options
 * passed by the executor at the command-line.
 */
public interface SpannerChangeStreamsToGcsOptions
    extends DataflowPipelineOptions,
        WriteDataChangeRecordsToGcsAvro.WriteToGcsAvroOptions,
        WriteDataChangeRecordsToGcsText.WriteToGcsTextOptions {

  @Description(
      "Project to read change streams from. The default for this parameter is the project where the"
          + " Dataflow pipeline is running.")
  @Default.String("")
  String getSpannerProjectId();

  void setSpannerProjectId(String projectId);

  @Description("The Spanner instance to read from.")
  @Validation.Required
  String getSpannerInstanceId();

  void setSpannerInstanceId(String spannerInstanceId);

  @Description("The Spanner database to read from.")
  @Validation.Required
  String getSpannerDatabase();

  void setSpannerDatabase(String spannerDatabase);

  @Description("The Spanner instance to use for the change stream metadata table.")
  @Validation.Required
  String getSpannerMetadataInstanceId();

  void setSpannerMetadataInstanceId(String spannerMetadataInstanceId);

  @Description("The Spanner database to use for the change stream metadata table.")
  @Validation.Required
  String getSpannerMetadataDatabase();

  void setSpannerMetadataDatabase(String spannerMetadataDatabase);

  @Description(
      "The Cloud Spanner change streams Connector metadata table name to use. If not provided, a"
          + " Cloud Spanner change streams Connector metadata table will automatically be created"
          + " during the pipeline flow.")
  String getSpannerMetadataTableName();

  void setSpannerMetadataTableName(String value);

  @Description("The Spanner change stream to read from.")
  @Validation.Required
  String getSpannerChangeStreamName();

  void setSpannerChangeStreamName(String spannerChangeStreamName);

  @Description(
      "The starting DateTime to use for reading change streams"
          + " (https://tools.ietf.org/html/rfc3339). Defaults to now.")
  @Default.String("")
  String getStartTimestamp();

  void setStartTimestamp(String startTimestamp);

  @Description(
      "The ending DateTime to use for reading change streams"
          + " (https://tools.ietf.org/html/rfc3339). The default value is \"max\", which represents"
          + " an infinite time in the future.")
  @Default.String("")
  String getEndTimestamp();

  void setEndTimestamp(String startTimestamp);

  @Description("Spanner host endpoint (only used for testing).")
  @Default.String("https://spanner.googleapis.com")
  String getSpannerHost();

  void setSpannerHost(String value);

  @Description("The format of the output GCS file. Allowed formats are TEXT, AVRO. Default is AVRO")
  @Default.Enum("AVRO")
  FileFormat getOutputFileFormat();

  void setOutputFileFormat(FileFormat outputFileFormat);

  @Description(
      "The window duration in which data will be written. Defaults to 5m. "
          + "Allowed formats are: "
          + "<int>s (for seconds, example: 5s), "
          + "<int>m (for minutes, example: 12m), "
          + "<int>h (for hours, example: 2h).")
  @Default.String("5m")
  String getWindowDuration();

  void setWindowDuration(String windowDuration);

  @Description(
      "Priority for Spanner RPC invocations. Defaults to HIGH. Allowed priorites are LOW, MEDIUM,"
          + " HIGH.")
  @Default.Enum("HIGH")
  RpcPriority getRpcPriority();

  void setRpcPriority(RpcPriority rpcPriority);
}
