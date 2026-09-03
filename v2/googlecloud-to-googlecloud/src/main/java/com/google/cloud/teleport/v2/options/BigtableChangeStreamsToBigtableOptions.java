/*
 * Copyright (C) 2026 Google LLC
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

/**
 * The {@link BigtableChangeStreamsToBigtableOptions} class provides the custom execution options
 * passed by the executor at the command-line.
 */
public interface BigtableChangeStreamsToBigtableOptions
    extends DataflowPipelineOptions,
        BigtableCommonOptions.ReadChangeStreamOptions,
        BigtableCommonOptions.WriteOptions {

  @TemplateParameter.Boolean(
      order = 1,
      optional = true,
      description = "Bidirectional replication",
      helpText =
          "Whether bidirectional replication between bigtable instances is enabled, adds additional logic to filter out replicated mutations")
  @Default.Boolean(false)
  Boolean getBidirectionalReplicationEnabled();

  void setBidirectionalReplicationEnabled(Boolean bidirectionalReplicationEnabled);

  @TemplateParameter.Text(
      order = 2,
      optional = true,
      description = "Source CBT qualifier",
      helpText = "Bidirectional replication source CBT qualifier to append")
  @Default.String("BIDIRECTIONAL_REPL_SOURCE_CBT")
  String getCbtQualifier();

  void setCbtQualifier(String cbtQualifier);

  @TemplateParameter.Text(
      order = 3,
      optional = true,
      description = "Filter CBT qualifier",
      helpText = "Bidirectional replication filter CBT qualifier to check and ignore")
  @Default.String("BIDIRECTIONAL_REPL_SOURCE_CBT")
  String getCbtFilterQualifier();

  void setCbtFilterQualifier(String cbtFilterQualifier);

  @TemplateParameter.Boolean(
      order = 4,
      optional = true,
      description = "Dry run",
      helpText = "When dry run is enabled, pipeline will not write to Bigtable")
  @Default.Boolean(false)
  Boolean getDryRunEnabled();

  void setDryRunEnabled(Boolean dryRunEnabled);

  @TemplateParameter.Boolean(
      order = 5,
      optional = true,
      description = "Filter GC mutations",
      helpText = "Filters out garbage collection Delete mutations from CBT")
  @Default.Boolean(false)
  Boolean getFilterGCMutations();

  void setFilterGCMutations(Boolean filterGCMutations);

  @TemplateParameter.Boolean(
      order = 6,
      optional = true,
      description = "Redistribute change stream mutations by row key",
      helpText =
          "When set to true, redistributes change stream mutations by their row key to balance load across workers.")
  @Default.Boolean(false)
  Boolean getAddRedistribute();

  void setAddRedistribute(Boolean addRedistribute);
}
