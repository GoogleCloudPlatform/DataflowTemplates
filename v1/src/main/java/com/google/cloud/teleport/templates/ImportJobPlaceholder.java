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

import com.google.cloud.bigtable.beam.sequencefiles.ImportJob;
import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.metadata.TemplateCreationParameter;
import com.google.cloud.teleport.metadata.TemplateParameter;
import com.google.cloud.teleport.templates.ImportJobPlaceholder.ImportJobPlaceholderOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Default.Integer;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.ValueProvider;

/**
 * Placeholder template class for {@link ImportJob}.
 *
 * <p>Check out <a
 * href="https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/v1/README_GCS_SequenceFile_to_Cloud_Bigtable.md">README</a>
 * for instructions on how to use or modify this template.
 */
@Template(
    name = "GCS_SequenceFile_to_Cloud_Bigtable",
    category = TemplateCategory.BATCH,
    displayName = "SequenceFile Files on Cloud Storage to Cloud Bigtable",
    description =
        "The Cloud Storage SequenceFile to Bigtable template is a pipeline that reads data from SequenceFiles in a "
            + "Cloud Storage bucket and writes the data to a Bigtable table. "
            + "You can use the template to copy data from Cloud Storage to Bigtable.",
    placeholderClass = ImportJob.class,
    optionsClass = ImportJobPlaceholderOptions.class,
    documentation =
        "https://cloud.google.com/dataflow/docs/guides/templates/provided/sequencefile-to-bigtable",
    contactInformation = "https://cloud.google.com/support",
    requirements = {
      "The Bigtable table must exist.",
      "The input SequenceFiles must exist in a Cloud Storage bucket before running the pipeline.",
      "The input SequenceFiles must have been exported from Bigtable or HBase."
    })
public class ImportJobPlaceholder {

  /** Options class for the ImportJob pipeline. */
  protected interface ImportJobPlaceholderOptions {

    @TemplateParameter.ProjectId(
        order = 1,
        groupName = "Target",
        description = "Project ID",
        helpText =
            "The ID of the Google Cloud project of the Cloud Bigtable instance that you want to write data to. Defaults to --project.")
    ValueProvider<String> getBigtableProject();

    @TemplateParameter.Text(
        order = 2,
        groupName = "Target",
        regexes = {"[a-z][a-z0-9\\-]+[a-z0-9]"},
        description = "Instance ID",
        helpText = "The ID of the Cloud Bigtable instance that contains the table")
    ValueProvider<String> getBigtableInstanceId();

    @TemplateParameter.Text(
        order = 3,
        groupName = "Target",
        regexes = {"[_a-zA-Z0-9][-_.a-zA-Z0-9]*"},
        description = "Table ID",
        helpText = "The ID of the Cloud Bigtable table to import")
    ValueProvider<String> getBigtableTableId();

    @TemplateParameter.Text(
        order = 4,
        groupName = "Target",
        optional = true,
        regexes = {"[_a-zA-Z0-9][-_.a-zA-Z0-9]*"},
        description = "Application profile ID",
        helpText = "The ID of the Cloud Bigtable application profile to be used for the import")
    ValueProvider<String> getBigtableAppProfileId();

    @TemplateParameter.GcsReadFile(
        order = 5,
        groupName = "Source",
        description = "Source path pattern",
        helpText = "Cloud Storage path pattern where data is located.",
        example = "gs://your-bucket/your-path/prefix*")
    ValueProvider<String> getSourcePattern();

    @TemplateParameter.Integer(
        order = 6,
        description = "Mutation Throttle Latency",
        optional = true,
        helpText =
            "Optional Set mutation latency throttling (enables the feature). Value in milliseconds.")
    @Integer(0)
    ValueProvider<Integer> getMutationThrottleLatencyMs();

    @TemplateCreationParameter(value = "false")
    @Description("Wait for pipeline to finish.")
    @Default.Boolean(false)
    boolean getWait();
  }
}
