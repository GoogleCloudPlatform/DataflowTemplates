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
package com.google.cloud.teleport.v2.templates;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.v2.options.LlmBatchTextProcessingPipelineOptions;
import com.google.cloud.teleport.v2.transforms.LlmTransform;
import com.langchainbeam.LangchainModelHandler;
import com.langchainbeam.model.LangchainBeamOutput;
import com.langchainbeam.model.openai.OpenAiModelOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

/**
 * A simple Apache Beam pipeline to batch process text input using an LLM model
 *
 * <p>This pipeline:
 *
 * <ul>
 *   <li>Reads text input from a file (e.g., GCS)
 *   <li>Processes each line using the configured LLM
 *   <li>Writes model output to a target file in GCS
 * </ul>
 */
@Template(
    category = TemplateCategory.BATCH,
    description = {"Simple template to batch process data using LLM"},
    displayName = "LLM Text processing",
    name = "LlmBatchTextProcessing",
    optionsClass = LlmBatchTextProcessingPipelineOptions.class,
    flexContainerName = "llm-batch-text-processing")
public final class LlmBatchTextProcessing {

  public static void main(String[] args) {

    LlmBatchTextProcessingPipelineOptions options =
        PipelineOptionsFactory.fromArgs(args)
            .withValidation()
            .as(LlmBatchTextProcessingPipelineOptions.class);
    run(options);
  }

  /** Defines and runs the pipeline using provided {@link LlmBatchTextProcessingPipelineOptions}. */
  public static PipelineResult run(LlmBatchTextProcessingPipelineOptions options) {

    checkNotNull(options, "options argument to run method cannot be null.");

    Pipeline pipeline = Pipeline.create(options);

    OpenAiModelOptions modelOptions =
        OpenAiModelOptions.builder()
            .modelName(options.getModelName())
            .apiKey(options.getApiKey())
            .build();

    LangchainModelHandler handler = new LangchainModelHandler(modelOptions, options.getPrompt());

    PCollection<String> inputData =
        pipeline.apply("Read data", TextIO.read().from(options.getInputDataFile()));

    PCollection<LangchainBeamOutput> modelOutput = inputData.apply(LlmTransform.generate(handler));

    modelOutput
        .apply(
            "FormatOutput",
            MapElements.into(TypeDescriptors.strings())
                .via(
                    out ->
                        String.format(
                            "Model Output: %s, Input Element: %s",
                            out.getOutput(), out.getInputElement())))
        .apply(
            "WriteToGCS",
            TextIO.write().to(options.getLlmOutputFile()).withSuffix(".txt").withoutSharding());

    return pipeline.run();
  }
}
