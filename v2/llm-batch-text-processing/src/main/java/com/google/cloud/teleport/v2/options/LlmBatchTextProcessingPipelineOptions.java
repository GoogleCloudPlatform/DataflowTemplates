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
package com.google.cloud.teleport.v2.options;

import com.google.cloud.teleport.metadata.TemplateParameter;
import org.apache.beam.sdk.options.PipelineOptions;

public interface LlmBatchTextProcessingPipelineOptions extends PipelineOptions {

  @TemplateParameter.Text(
      order = 1,
      optional = false,
      description = "Api Key",
      helpText = "Open AI API key")
  String getApiKey();

  void setApiKey(String key);

  @TemplateParameter.Text(
      order = 2,
      optional = false,
      description = "Instruction Prompt",
      helpText = "instruction prompt to model on how to process input data")
  String getPrompt();

  void setPrompt(String prompt);

  @TemplateParameter.Text(
      order = 3,
      optional = false,
      description = "Model Name",
      helpText = "OpenAI model name")
  String getModelName();

  void setModelName(String name);

  @TemplateParameter.GcsReadFile(
      order = 4,
      optional = false,
      description = "input data file",
      helpText = "Input data to be processed by LLM")
  String getInputDataFile();

  void setInputDataFile(String file);

  @TemplateParameter.GcsWriteFile(
      order = 5,
      optional = false,
      description = "Output file",
      helpText = "Output file to store the LLM processed data")
  String getLlmOutputFile();

  void setLlmOutputFile(String value);
}
