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
package com.google.cloud.teleport.templates.python;

import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.Template.TemplateType;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.metadata.TemplateParameter;

/** Placeholder template class for WordCount in Python. */
@Template(
    name = "Word_Count_Python",
    category = TemplateCategory.GET_STARTED,
    type = TemplateType.PYTHON,
    displayName = "Word Count (Python)",
    description =
        "Batch pipeline. Reads text from Cloud Storage, tokenizes text lines into individual words,"
            + " and performs frequency count on each of the words.",
    flexContainerName = "word-count-python",
    contactInformation = "https://cloud.google.com/support",
    hidden = true)
public interface WordCountPython {

  @TemplateParameter.GcsReadFile(
      order = 1,
      name = "input",
      description = "Input file(s) in Cloud Storage",
      helpText =
          "The input file pattern Dataflow reads from. Use the example file "
              + "(gs://dataflow-samples/shakespeare/kinglear.txt) or enter the path to your own "
              + "using the same format: gs://your-bucket/your-file.txt")
  String getInput();

  @TemplateParameter.GcsWriteFolder(
      order = 2,
      description = "Output Cloud Storage file prefix",
      helpText = "Path and filename prefix for writing output files. Ex: gs://your-bucket/counts")
  String getOutput();
}
