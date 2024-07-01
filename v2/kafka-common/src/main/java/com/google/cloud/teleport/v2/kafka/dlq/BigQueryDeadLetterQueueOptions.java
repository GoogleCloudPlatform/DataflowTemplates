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
package com.google.cloud.teleport.v2.kafka.dlq;

import com.google.cloud.teleport.metadata.TemplateParameter;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptions;

public interface BigQueryDeadLetterQueueOptions extends PipelineOptions {
  String GROUP_NAME = "Dead Letter Queue";

  // Make this non-optional. Otherwise, the UI will show this as Optional Dead Letter Queue
  // Parameters
  // instead of Dead Letter Queue.
  @TemplateParameter.Boolean(
      name = "useBigQueryDLQ",
      groupName = GROUP_NAME,
      description = "Write errors to BigQuery",
      helpText =
          "If true, failed messages will be written to BigQuery with extra error information.")
  @Default.Boolean(false)
  Boolean getUseBigQueryDLQ();

  void setUseBigQueryDLQ(Boolean value);

  @TemplateParameter.Text(
      groupName = GROUP_NAME,
      parentName = "useBigQueryDLQ",
      parentTriggerValues = {"true"},
      optional = true,
      description = "Fully Qualified BigQuery table name for Dead Letter Queue.",
      helpText =
          "Fully Qualified BigQuery table name for failed messages. Messages failed to reach the "
              + "output table for different reasons "
              + "(e.g., mismatched schema, malformed json) are written to this table."
              + "The table will be created by the template.",
      example = "your-project-id:your-dataset.your-table-name")
  String getOutputDeadletterTable();

  void setOutputDeadletterTable(String outputDeadletterTable);
}
