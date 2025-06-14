/*
 * Copyright (C) 2025 Google LLC
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
package com.google.cloud.teleport.v2.templates.options;

import com.google.cloud.teleport.metadata.TemplateParameter;
import com.google.cloud.teleport.v2.bigtable.options.BigtableCommonOptions;
import com.google.cloud.teleport.v2.kafka.options.KafkaWriteOptions;
import com.google.cloud.teleport.v2.kafka.options.SchemaRegistryOptions;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Default;

public interface BigtableChangeStreamsToKafkaOptions
    extends DataflowPipelineOptions,
        BigtableCommonOptions.ReadChangeStreamOptions,
        KafkaWriteOptions,
        SchemaRegistryOptions {
  @TemplateParameter.GcsWriteFolder(
      order = 1,
      optional = true,
      description = "Dead letter queue directory to store any unpublished change record.",
      helpText =
          "The directory for the dead-letter queue. Records that fail to be processed are stored in this directory. Defaults to "
              + "a directory under the Dataflow job temp location. In most cases, you can use the default path.")
  @Default.String("")
  String getDlqDirectory();

  void setDlqDirectory(String dlqDirectory);

  @TemplateParameter.Integer(
      order = 2,
      optional = true,
      description = "Dead letter queue retry minutes",
      helpText = "The number of minutes between dead-letter queue retries. Defaults to `10`.")
  @Default.Integer(10)
  Integer getDlqRetryMinutes();

  void setDlqRetryMinutes(Integer dlqRetryMinutes);

  @TemplateParameter.Integer(
      order = 3,
      optional = true,
      description = "Dead letter maximum retries",
      helpText = "The dead letter maximum retries. Defaults to `5`.")
  @Default.Integer(5)
  Integer getDlqMaxRetries();

  void setDlqMaxRetries(Integer dlqMaxRetries);

  @TemplateParameter.Boolean(
      order = 4,
      optional = true,
      description = "Whether or not to disable retries for the DLQ",
      helpText = "Whether or not to disable retries for the DLQ")
  @Default.Boolean(false)
  boolean getDisableDlqRetries();

  void setDisableDlqRetries(boolean value);
}
