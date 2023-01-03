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

/** Placeholder template class for {@link ImportJob}. */
@Template(
    name = "GCS_SequenceFile_to_Cloud_Bigtable",
    category = TemplateCategory.BATCH,
    displayName = "SequenceFile Files on Cloud Storage to Cloud Bigtable",
    description =
        "A pipeline which reads data from SequenceFile in Cloud Storage and writes it to Cloud Bigtable table.",
    placeholderClass = ImportJob.class,
    optionsClass = ImportJobPlaceholderOptions.class,
    contactInformation = "https://cloud.google.com/support")
public class ImportJobPlaceholder {

  /** Options class for the ImportJob pipeline. */
  protected interface ImportJobPlaceholderOptions {

    @TemplateParameter.ProjectId(
        order = 1,
        description = "Project ID",
        helpText =
            "The ID of the Google Cloud project of the Cloud Bigtable instance that you want to write data to. Defaults to --project.")
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
        helpText = "The ID of the Cloud Bigtable table to import")
    ValueProvider<String> getBigtableTableId();

    @TemplateParameter.Text(
        order = 4,
        optional = true,
        regexes = {"[_a-zA-Z0-9][-_.a-zA-Z0-9]*"},
        description = "Application profile ID",
        helpText = "The ID of the Cloud Bigtable application profile to be used for the import")
    ValueProvider<String> getBigtableAppProfileId();

    @TemplateParameter.GcsReadFile(
        order = 5,
        description = "Source path pattern",
        helpText = "Cloud Storage path pattern where data is located.",
        example = "gs://your-bucket/your-path/prefix*")
    ValueProvider<String> getSourcePattern();

    @TemplateParameter.Integer(
        order = 6,
        description = "Mutation Throttle Latency",
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
