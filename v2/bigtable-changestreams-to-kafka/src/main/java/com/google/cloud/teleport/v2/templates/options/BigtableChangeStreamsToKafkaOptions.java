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
}
