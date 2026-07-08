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

/** The BQ Storage API options for the batch templates that write to BigQuery. */
public interface BigQueryStorageApiBatchOptions extends BigQueryOptions {
  @TemplateParameter.Boolean(
      order = 1,
      optional = true,
      description = "Use BigQuery Storage Write API",
      helpText =
          "If `true`, the pipeline uses the BigQuery Storage Write API (https://cloud.google.com/bigquery/docs/write-api). The default value is `false`. For more information, see Using the Storage Write API (https://beam.apache.org/documentation/io/built-in/google-bigquery/#storage-write-api).")
  @Default.Boolean(false)
  @Override
  Boolean getUseStorageWriteApi();

  @TemplateParameter.Boolean(
      order = 2,
      optional = true,
      parentName = "useStorageWriteApi",
      parentTriggerValues = {"true"},
      description = "Use at at-least-once semantics in BigQuery Storage Write API",
      helpText =
          "When using the Storage Write API, specifies the write semantics. To use at-least-once semantics (https://beam.apache.org/documentation/io/built-in/google-bigquery/#at-least-once-semantics), set this parameter to `true`. To use exactly-once semantics, set the parameter to `false`. This parameter applies only when `useStorageWriteApi` is `true`. The default value is `false`.")
  @Default.Boolean(false)
  @Override
  Boolean getUseStorageWriteApiAtLeastOnce();
}
