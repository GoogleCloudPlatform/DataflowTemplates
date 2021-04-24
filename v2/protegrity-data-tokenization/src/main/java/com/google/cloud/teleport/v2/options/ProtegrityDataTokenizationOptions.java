/*
 * Copyright (C) 2020 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.v2.options;

import com.google.cloud.teleport.v2.options.PubsubCommonOptions.ReadSubscriptionOptions;
import com.google.cloud.teleport.v2.transforms.io.BigTableIO;
import com.google.cloud.teleport.v2.transforms.io.GcsIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

/**
 * The {@link ProtegrityDataTokenizationOptions} interface provides the custom execution options
 * passed by the executor at the command-line.
 */
public interface ProtegrityDataTokenizationOptions extends PipelineOptions,
    GcsIO.GcsPipelineOptions, BigTableIO.BigTableOptions, ReadSubscriptionOptions,
    GcsCommonOptions.ReadOptions, GcsCommonOptions.WriteOptions, CsvOptions {

  @Description("Path to data schema (JSON format) in GCS compatible with BigQuery.")
  String getDataSchemaGcsPath();

  void setDataSchemaGcsPath(String dataSchemaGcsPath);

  @Description("Cloud BigQuery table name to write into.")
  String getBigQueryTableName();

  void setBigQueryTableName(String bigQueryTableName);

  //Protegrity specific parameters
  @Description("URI for the API calls to DSG.")
  String getDsgUri();

  void setDsgUri(String dsgUri);

  @Description("Size of the batch to send to DSG per request.")
  @Default.Integer(10)
  Integer getBatchSize();

  void setBatchSize(Integer batchSize);

  @Description(
      "GCS path to the payload configuration file with an array of fields "
          + "to extract for tokenization."
  )
  String getPayloadConfigGcsPath();

  void setPayloadConfigGcsPath(String payloadConfigGcsPath);

  @Description("Dead-Letter GCS path to store not-tokenized data")
  String getNonTokenizedDeadLetterGcsPath();

  void setNonTokenizedDeadLetterGcsPath(String nonTokenizedDeadLetterGcsPath);
}
