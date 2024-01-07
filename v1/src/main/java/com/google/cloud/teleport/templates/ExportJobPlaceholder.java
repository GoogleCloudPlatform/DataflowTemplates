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
package com.google.cloud.teleport.templates;

import com.google.cloud.bigtable.beam.sequencefiles.ExportJob;
import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.metadata.TemplateCreationParameter;
import com.google.cloud.teleport.metadata.TemplateParameter;
import com.google.cloud.teleport.templates.ExportJobPlaceholder.ExportJobPlaceholderOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Default.Integer;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.ValueProvider;

/**
 * Placeholder template class for {@link ExportJob}.
 *
 * <p>Check out <a
 * href="https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/v1/README_Cloud_Bigtable_to_GCS_SequenceFile.md">README</a>
 * for instructions on how to use or modify this template.
 */
@Template(
    name = "Cloud_Bigtable_to_GCS_SequenceFile",
    category = TemplateCategory.BATCH,
    displayName = "Cloud Bigtable to SequenceFile Files on Cloud Storage",
    description =
        "The Bigtable to Cloud Storage SequenceFile template is a pipeline that reads data from a Bigtable table and "
            + "writes the data to a Cloud Storage bucket in SequenceFile format. "
            + "You can use the template to copy data from Bigtable to Cloud Storage.",
    placeholderClass = ExportJob.class,
    optionsClass = ExportJobPlaceholderOptions.class,
    documentation =
        "https://cloud.google.com/dataflow/docs/guides/templates/provided/bigtable-to-sequencefile",
    contactInformation = "https://cloud.google.com/support",
    requirements = {
      "The Bigtable table must exist.",
      "The output Cloud Storage bucket must exist before running the pipeline."
    })
public class ExportJobPlaceholder {

  /** Options class for the ExportJob pipeline. */
  protected interface ExportJobPlaceholderOptions {

    @TemplateParameter.ProjectId(
        order = 1,
        description = "Project ID",
        helpText =
            "The ID of the Google Cloud project of the Cloud Bigtable instance that you want to read data from. Defaults to job project.")
    ValueProvider<String> getBigtableProject();

    @TemplateParameter.Text(
        order = 2,
        regexes = {"[a-z][a-z0-9\\-]+[a-z0-9]"},
        description = "Instance ID",
        helpText = "The ID of the Cloud Bigtable instance that contains the table")
    ValueProvider<String> getBigtableInstanceId();

    @TemplateParameter.Text(
        order = 3,
        regexes = {"[_a-zA-Z0-9][-_.a-zA-Z0-9]*"},
        description = "Table ID",
        helpText = "The ID of the Cloud Bigtable table to export")
    ValueProvider<String> getBigtableTableId();

    @TemplateParameter.Text(
        order = 4,
        optional = true,
        regexes = {"[_a-zA-Z0-9][-_.a-zA-Z0-9]*"},
        description = "Application profile ID",
        helpText = "The ID of the Cloud Bigtable application profile to be used for the export")
    ValueProvider<String> getBigtableAppProfileId();

    @TemplateParameter.Text(
        order = 5,
        optional = true,
        description = "Bigtable Start Row",
        helpText = "The row where to start the export from, defaults to the first row.")
    @Default.String("")
    ValueProvider<String> getBigtableStartRow();

    @TemplateParameter.Text(
        order = 6,
        optional = true,
        description = "Bigtable Stop Row",
        helpText = "The row where to stop the export, defaults to the last row.")
    @Default.String("")
    ValueProvider<String> getBigtableStopRow();

    @TemplateParameter.Integer(
        order = 7,
        optional = true,
        description = "Bigtable Max Versions",
        helpText = "Maximum number of cell versions.")
    @Integer(2147483647)
    ValueProvider<Integer> getBigtableMaxVersions();

    @TemplateParameter.Text(
        order = 8,
        optional = true,
        description = "Bigtable Filter",
        helpText = "Filter string. See: http://hbase.apache.org/book.html#thrift.")
    @Default.String("")
    ValueProvider<String> getBigtableFilter();

    @TemplateParameter.GcsWriteFolder(
        order = 9,
        description = "Destination path",
        helpText = "Cloud Storage path where data should be written.",
        example = "gs://your-bucket/your-path/")
    ValueProvider<String> getDestinationPath();

    @TemplateParameter.Text(
        order = 10,
        description = "SequenceFile prefix",
        helpText = "The prefix for each shard in destinationPath.",
        example = "output-")
    @Default.String("part")
    ValueProvider<String> getFilenamePrefix();

    @TemplateCreationParameter(value = "false")
    @Description("Wait for pipeline to finish.")
    @Default.Boolean(false)
    boolean getWait();
  }
}
