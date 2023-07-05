/*
 * Copyright (C) 2022 Google LLC
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
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryOptions;
import org.apache.beam.sdk.options.Default;

/** The BQ Storage API options for the streaming templates that write to BigQuery. */
public interface BigQueryStorageApiStreamingOptions extends BigQueryOptions {
  @TemplateParameter.Boolean(
      order = 1,
      optional = true,
      description = "Use BigQuery Storage Write API",
      helpText =
          "If enabled (set to true) the pipeline will use Storage Write API when writing the data"
              + " to BigQuery (see"
              + " https://cloud.google.com/blog/products/data-analytics/streaming-data-into-bigquery-using-storage-write-api)."
              + " If this is enabled and at-least-once semantics (useStorageWriteApiAtLeastOnce)"
              + " option is off then \"Number of streams for BigQuery Storage Write API\" and"
              + " \"Triggering frequency in seconds for BigQuery Storage Write API\" must be"
              + " provided.")
  @Default.Boolean(false)
  @Override
  Boolean getUseStorageWriteApi();

  @TemplateParameter.Boolean(
      order = 2,
      optional = true,
      description = "Use at-least-once semantics in BigQuery Storage Write API",
      helpText =
          "This parameter takes effect only if \"Use BigQuery Storage Write API\" is enabled. If"
              + " enabled the at-least-once semantics will be used for Storage Write API, otherwise"
              + " exactly-once semantics will be used.")
  @Default.Boolean(false)
  @Override
  Boolean getUseStorageWriteApiAtLeastOnce();

  @TemplateParameter.Integer(
      order = 3,
      optional = true,
      description = "Number of streams for BigQuery Storage Write API",
      helpText =
          "Number of streams defines the parallelism of the BigQueryIO's Write transform and"
              + " roughly corresponds to the number of Storage Write API's streams which will be"
              + " used by the pipeline. See"
              + " https://cloud.google.com/blog/products/data-analytics/streaming-data-into-bigquery-using-storage-write-api"
              + " for the recommended values.")
  @Override
  @Default.Integer(0)
  Integer getNumStorageWriteApiStreams();

  @TemplateParameter.Integer(
      order = 4,
      optional = true,
      description = "Triggering frequency in seconds for BigQuery Storage Write API",
      helpText =
          "Triggering frequency will determine how soon the data will be visible for querying in"
              + " BigQuery. See"
              + " https://cloud.google.com/blog/products/data-analytics/streaming-data-into-bigquery-using-storage-write-api"
              + " for the recommended values.")
  @Override
  Integer getStorageWriteApiTriggeringFrequencySec();
}
