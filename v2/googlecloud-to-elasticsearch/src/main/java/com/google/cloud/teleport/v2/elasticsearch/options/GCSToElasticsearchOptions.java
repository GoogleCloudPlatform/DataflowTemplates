/*
 * Copyright (C) 2021 Google LLC
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
package com.google.cloud.teleport.v2.elasticsearch.options;

import com.google.cloud.teleport.v2.transforms.CsvConverters;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation;

/**
 * The {@link GCSToElasticsearchOptions} class provides the custom execution options passed by the
 * executor at the command-line.
 */
public interface GCSToElasticsearchOptions
    extends CsvConverters.CsvPipelineOptions, ElasticsearchWriteOptions {

  @Description("Deadletter table for failed inserts in form: <project-id>:<dataset>.<table>")
  @Validation.Required
  String getDeadletterTable();

  void setDeadletterTable(String deadletterTable);

  @Description("Input format, default is CSV")
  @Default.String("csv")
  String getInputFormat();

  void setInputFormat(String inputFormat);
}
