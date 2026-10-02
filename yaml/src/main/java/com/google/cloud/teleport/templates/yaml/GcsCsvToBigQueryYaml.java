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
    name = "GCS_CSV_to_BigQuery_Yaml",
    category = TemplateCategory.BATCH,
    type = Template.TemplateType.YAML,
    displayName = "CSV files on Cloud Storage to BigQuery (YAML)",
    description =
        "The CSV files on Cloud Storage to BigQuery template is a batch pipeline that reads CSV files from Cloud Storage and writes them to a BigQuery table.",
    flexContainerName = "pipeline-yaml",
    yamlTemplateFile = "GcsCsvToBigQuery.yaml",
    filesToCopy = {
      "main.py",
      "requirements.txt",
      "options/csv_options.yaml",
      "options/bigquery_options.yaml"
    },
    documentation = "",
    contactInformation = "https://cloud.google.com/support",
    requirements = {
      "The input CSV files must exist on Cloud Storage and have a header row.",
      "The target BigQuery dataset and table must exist or be creatable."
    },
    streaming = false,
    hidden = false)
public interface GcsCsvToBigQueryYaml {

  @TemplateParameter.Text(
      order = 1,
      name = "csvPath",
      optional = false,
      description = "Cloud Storage CSV path",
      helpText =
          "The Cloud Storage path or file pattern to the CSV file(s) to read. For example: gs://my-bucket/path/*.csv",
      example = "")
  @Validation.Required
  String getCsvPath();

  @TemplateParameter.Text(
      order = 2,
      name = "delimiter",
      optional = true,
      description = "CSV delimiter character",
      helpText =
          "A single character string used to separate fields, e.g. ',' or '\t'. Defaults to ','.",
      example = "")
  String getDelimiter();

  @TemplateParameter.Text(
      order = 3,
      name = "comment",
      optional = true,
      description = "CSV comment character",
      helpText =
          "A single character string indicating that the remainder of the line should not be parsed, e.g. '#'.",
      example = "#")
  String getComment();

  @TemplateParameter.Text(
      order = 4,
      name = "filenameColumn",
      optional = true,
      description = "Column name for source filename",
      helpText =
          "If not None, the name of the column to add to each record, containing the filename of the source file.",
      example = "source_file")
  String getFilenameColumn();

  @TemplateParameter.Text(
      order = 5,
      name = "table",
      optional = false,
      description = "BigQuery table",
      helpText =
          "BigQuery table location to write the output to or read from. The name  should be in the format <project>:<dataset>.<table_name>. For write,  the table's schema must match input objects.",
      example = "")
  @Validation.Required
  String getTable();

  @TemplateParameter.Text(
      order = 6,
      name = "createDisposition",
      optional = true,
      description = "How to create",
      helpText =
          "Specifies whether a table should be created if it does not exist.  Valid inputs are 'Never' and 'IfNeeded'.",
      example = "")
  @Default.String("CREATE_IF_NEEDED")
  String getCreateDisposition();

  @TemplateParameter.Text(
      order = 7,
      name = "writeDisposition",
      optional = true,
      description = "How to write",
      helpText =
          "How to specify if a write should append to an existing table, replace the table, or verify that the table is empty. Note that the my_dataset being written to must already exist. Unbounded collections can only be written using 'WRITE_EMPTY' or 'WRITE_APPEND'.",
      example = "")
  @Default.String("WRITE_APPEND")
  String getWriteDisposition();

  @TemplateParameter.Integer(
      order = 8,
      name = "numStreams",
      optional = true,
      description = "Number of streams for BigQuery Storage Write API",
      helpText =
          "Number of streams defines the parallelism of the BigQueryIO’s Write  transform and roughly corresponds to the number of Storage Write API’s  streams which will be used by the pipeline. See https://cloud.google.com/blog/products/data-analytics/streaming-data-into-bigquery-using-storage-write-api for the recommended values. The default value is 1.",
      example = "")
  @Default.Integer(1)
  Integer getNumStreams();
}
