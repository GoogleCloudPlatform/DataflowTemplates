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
package com.google.cloud.teleport.v2.auto.dlq;

import com.google.cloud.teleport.metadata.TemplateParameter;
import org.apache.beam.sdk.options.PipelineOptions;

public interface BigQueryDeadletterOptions extends PipelineOptions {

  @TemplateParameter.BigQueryTable(
      groupName = "Target",
      order = 4,
      optional = true,
      description = "Table for messages failed to reach the output table (i.e., Deadletter table)",
      helpText =
          "Messages failed to reach the output table for all kind of reasons (e.g., mismatched"
              + " schema, malformed json) are written to this table. It should be in the format"
              + " of \"your-project-id:your-dataset.your-table-name\". If it doesn't exist, it"
              + " will be created during pipeline execution. If not specified,"
              + " \"{outputTableSpec}_error_records\" is used instead.")
  String getOutputDeadletterTable();

  void setOutputDeadletterTable(String value);
}
