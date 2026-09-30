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
package com.google.cloud.teleport.templates.yaml;

import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.metadata.TemplateParameter;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Validation;

@Template(
    name = "Word_Count_Yaml",
    category = TemplateCategory.BATCH,
    type = Template.TemplateType.YAML,
    displayName = "Word Count (YAML)",
    description =
        "The Word Count template is a batch pipeline that reads text from Cloud Storage, splits lines into words, counts word occurrences, formats the results, and writes the output back to Cloud Storage.",
    flexContainerName = "pipeline-yaml",
    yamlTemplateFile = "WordCount.yaml",
    filesToCopy = {"main.py", "requirements.txt", "options/text_options.yaml"},
    documentation = "",
    contactInformation = "https://cloud.google.com/support",
    requirements = {
      "The input text file(s) on Cloud Storage must exist and be accessible.",
      "The output Cloud Storage directory must be accessible."
    },
    streaming = false,
    hidden = false)
public interface WordCountYaml {

  @TemplateParameter.Text(
      order = 1,
      name = "inputPath",
      optional = false,
      description = "Input file path",
      helpText = "The Cloud Storage path or file pattern to read text from.",
      example = "gs://dataflow-samples/shakespeare/kinglear.txt")
  @Validation.Required
  @Default.String("gs://dataflow-samples/shakespeare/kinglear.txt")
  String getInputPath();

  @TemplateParameter.Text(
      order = 2,
      name = "outputPath",
      optional = false,
      description = "Output file path",
      helpText = "The Cloud Storage path prefix to write output text files to.",
      example = "gs://your-bucket/wordcount/output")
  @Validation.Required
  String getOutputPath();
}
