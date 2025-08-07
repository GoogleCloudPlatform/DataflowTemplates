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
package com.google.cloud.teleport.templates.python;

import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.metadata.TemplateParameter;

/** template class for YAMLTemplate in Python. */
@Template(
    name = "ArrayRecord_Converter",
    category = TemplateCategory.STREAMING,
    type = Template.TemplateType.PYTHON,
    displayName = "ArrayRecord Converter Job",
    description =
        "The ArrayRecord_Converter Template is used to convert bulk text/image dataset in GCS into ArrayRecord Datasets in GCS"
            + "An input GCS path can be passed in and resulting data will be uploaded to the `output_path`",
    flexContainerName = "arrayrecord-converter",
    contactInformation = "https://cloud.google.com/support",
    )
public interface ArrayRecordConverter {
  @TemplateParameter.GcsReadFile(
      order = 1,
      name = "input_path",
      optional = false,
      description = "Input GCS path to match all files(e.g., gcs://example/*.txt)",
      helpText = "An input path in the form of a GCS path.")
  String getInputPath();

  @TemplateParameter.Text(
      order = 2,
      name = "input_format",
      optional = false,
      description = "Input format of the data, can be either text or image",
      helpText = "The format of the input, which can be either text or image. This job does not support other values.")
  String getInputFormat();

  @TemplateParameter.GcsWriteFolder(
      order = 3,
      name = "output_path",
      optional = false,
      description = "Output GCS path where files are generated",
      helpText =
          "Output path in the form of a GCS path where files will be uploaded.")
  String getOutputPath();
}
