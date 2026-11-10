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
    name = "GCS_Text_to_BigQuery_Yaml",
    category = TemplateCategory.BATCH,
    type = Template.TemplateType.YAML,
    displayName = "Text files on Cloud Storage to BigQuery (YAML)",
    description =
        "The Text files on Cloud Storage to BigQuery template is a batch pipeline that reads text files from Cloud Storage and writes each line to a BigQuery table.",
    flexContainerName = "pipeline-yaml",
    yamlTemplateFile = "GcsTextToBigQuery.yaml",
    filesToCopy = {
      "main.py",
      "requirements.txt",
      "options/text_options.yaml",
      "options/bigquery_options.yaml"
    },
    documentation = "",
    contactInformation = "https://cloud.google.com/support",
    requirements = {
      "The input text files must exist on Cloud Storage.",
      "The target BigQuery dataset and table must exist or be creatable."
    },
    streaming = false,
    hidden = false)
public interface GcsTextToBigQueryYaml {

  @TemplateParameter.Text(
      order = 1,
      name = "inputPath",
      optional = false,
      description = "Cloud Storage text path",
      helpText = "The Cloud Storage path or file pattern to the text file(s) to read.",
      example = "gs://my-bucket/path/*.txt")
  @Validation.Required
  String getInputPath();

  @TemplateParameter.Text(
      order = 2,
      name = "table",
      optional = false,
      description = "BigQuery table",
      helpText =
          "BigQuery table location to write the output to or read from. The name  should be in the format <project>:<dataset>.<table_name>. For write,  the table's schema must match input objects.",
      example = "my-project:my_dataset.my_table")
  @Validation.Required
  String getTable();

  @TemplateParameter.Text(
      order = 3,
      name = "createDisposition",
      optional = true,
      description = "How to create",
      helpText =
          "Specifies whether a table should be created if it does not exist.  Valid inputs are 'CREATE_NEVER' and 'CREATE_IF_NEEDED'.",
      example = "CREATE_NEVER")
  @Default.String("CREATE_NEVER")
  String getCreateDisposition();

  @TemplateParameter.Text(
      order = 4,
      name = "writeDisposition",
      optional = true,
      description = "How to write",
      helpText =
          "How to specify if a write should append to an existing table, replace the table, or verify that the table is empty. Note that the dataset being written to must already exist. Unbounded collections can only be written using 'WRITE_EMPTY' or 'WRITE_APPEND'.",
      example = "WRITE_APPEND")
  @Default.String("WRITE_APPEND")
  String getWriteDisposition();
}
