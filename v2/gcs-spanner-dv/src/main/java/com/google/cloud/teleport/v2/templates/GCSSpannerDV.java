/*
 * Copyright (C) 2026 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.v2.templates;

import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.metadata.TemplateParameter;
import com.google.cloud.teleport.v2.common.UncaughtExceptionLogger;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Template(
    name = "GCS_Spanner_DV",
    category = TemplateCategory.BATCH,
    displayName = "GCS Spanner Data Validation",
    description = "Batch pipeline that reads data from GCS and Spanner compares them to validate migration correctness.",
    optionsClass = GCSSpannerDV.Options.class,
    flexContainerName = "gcs-spanner-dv",
    documentation =
        "https://cloud.google.com/dataflow/docs/guides/templates/provided/gcs-spanner-dv",
    contactInformation = "https://cloud.google.com/support",
    preview = true,
    requirements = {
      "The GCS directory for AVRO files must exist before pipeline execution.",
      "The Spanner tables must exist before pipeline execution.",
      "The Spanner tables must have a compatible schema."
    }
  )
public class GCSSpannerDV {

  private static final Logger LOG = LoggerFactory.getLogger(GCSSpannerDV.class);

  public interface Options extends PipelineOptions {
    @TemplateParameter.GcsReadFolder(
        order = 1,
        optional = true,
        description = "GCS directory for AVRO files",
        helpText = "This directory is used to read the AVRO files of the records read from source.",
        example = "gs://your-bucket/your-path"
    )
    String getGcsInputDirectory();

    void setGcsInputDirectory(String value);
  }

  public static void main(String[] args) {
    UncaughtExceptionLogger.register();

    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    run(options);
  }

  public static PipelineResult run(Options options) {
    Pipeline pipeline = Pipeline.create(options);
    return pipeline.run();
  }
}
