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
package com.google.cloud.teleport.metadata.options;

import com.google.cloud.teleport.metadata.TemplateParameter;

/** Template options that apply to all pipelines. */
public class DefaultTemplateOptions {

  @TemplateParameter.Text(
      order = 100,
      optional = true,
      description = "Autoscaling Algorithm",
      helpText = "Autoscaling Algorithm")
  private String autoscalingAlgorithm;

  @TemplateParameter.Integer(
      order = 101,
      optional = true,
      description = "Number of Workers",
      helpText = "Number of Workers")
  private Integer numWorkers;

  @TemplateParameter.Integer(
      order = 102,
      optional = true,
      description = "Maximum Number of Workers",
      helpText = "Maximum Number of Workers")
  private Integer maxNumWorkers;

  @TemplateParameter.Integer(
      order = 103,
      optional = true,
      description = "Number of Worker Harness Threads",
      helpText = "Number of Worker Harness Threads")
  private Integer numberOfWorkerHarnessThreads;

  @TemplateParameter.Boolean(
      order = 104,
      optional = true,
      description = "Dump Heap on OOM",
      helpText = "If Heap should be dumped to Cloud Storage upon Out-of-Memory issues")
  private Boolean dumpHeapOnOOM;

  @TemplateParameter.GcsWriteFolder(
      order = 105,
      optional = true,
      description = "Save Heap Dumps Cloud Storage Path",
      helpText = "Cloud Storage to save dumps in case of Out-of-Memory error")
  private String saveHeapDumpsToGcsPath;

  @TemplateParameter.Text(
      order = 106,
      optional = true,
      description = "Worker Machine Type",
      helpText = "Worker Machine Type")
  private String workerMachineType;

  @TemplateParameter.Integer(
      order = 107,
      optional = true,
      description = "Max Streaming Rows to Batch",
      helpText = "Max Streaming Rows to Batch")
  private Integer maxStreamingRowsToBatch;

  @TemplateParameter.Text(
      order = 108,
      optional = true,
      description = "Default Worker Log Level",
      helpText = "Default Worker Log Level")
  private String defaultWorkerLogLevel;

  @TemplateParameter.Integer(
      order = 109,
      optional = true,
      description = "Max Streaming Batch Size",
      helpText = "Max Streaming Batch Size")
  private Integer maxStreamingBatchSize;
}
