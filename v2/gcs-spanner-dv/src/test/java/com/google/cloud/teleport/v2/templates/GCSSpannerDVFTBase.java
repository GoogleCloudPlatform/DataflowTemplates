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
package com.google.cloud.teleport.v2.templates;

import com.google.cloud.teleport.v2.spanner.migrations.transformation.CustomTransformation;
import com.google.common.io.Resources;
import java.io.IOException;
import java.util.Map;
import org.apache.beam.it.common.PipelineLauncher.LaunchInfo;
import org.apache.beam.it.common.utils.PipelineUtils;
import org.apache.beam.it.gcp.dataflow.FlexTemplateDataflowJobResourceManager;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;

/**
 * Base class for gcs-spanner-dv failure injection integration tests.
 *
 * <p><strong>Why is this a separate class from {@link GCSSpannerDVITBase}?</strong>
 *
 * <p>While {@code GCSSpannerDVITBase} is runner-agnostic and relies on the generic {@code
 * PipelineLauncher} (allowing tests to run locally via DirectRunner), failure injection testing
 * explicitly requires building a custom Docker image with the {@code failureInjectionTest} Maven
 * profile.
 *
 * <p>Therefore, tests extending this class are strictly coupled to Dataflow Flex Templates and
 * bypass the generic launcher in favor of {@link FlexTemplateDataflowJobResourceManager}.
 */
public abstract class GCSSpannerDVFTBase extends GCSSpannerDVITBase {

  /**
   * Launches the Dataflow job with failure injection testing capabilities using
   * FlexTemplateDataflowJobResourceManager.
   */
  protected LaunchInfo launchFTDataflowJob(
      String testId,
      String projectId,
      SpannerResourceManager spannerResourceManager,
      String bigQueryDataset,
      String gcsInputDirectory,
      String sessionFileResourceName,
      String schemaOverridesFileResourceName,
      String tableOverrides,
      String columnOverrides,
      CustomTransformation customTransformation,
      String failureInjectionParameter,
      Map<String, String> jobParameters)
      throws IOException {

    FlexTemplateDataflowJobResourceManager.Builder flexTemplateBuilder =
        FlexTemplateDataflowJobResourceManager.builder(testId)
            .withTemplateName("GCS_Spanner_Data_Validator")
            .withTemplateModulePath("v2/gcs-spanner-dv")
            .withAdditionalMavenProfile("failureInjectionTest")
            .addEnvironmentVariable(
                "additionalExperiments", java.util.Collections.singletonList("disable_runner_v2"));

    if (failureInjectionParameter != null && !failureInjectionParameter.isEmpty()) {
      flexTemplateBuilder.addParameter("failureInjectionParameter", failureInjectionParameter);
    }

    flexTemplateBuilder.addParameter("projectId", projectId);
    flexTemplateBuilder.addParameter("instanceId", spannerResourceManager.getInstanceId());
    flexTemplateBuilder.addParameter("databaseId", spannerResourceManager.getDatabaseId());
    flexTemplateBuilder.addParameter("bigQueryDataset", bigQueryDataset);
    flexTemplateBuilder.addParameter("gcsInputDirectory", gcsInputDirectory);

    if (sessionFileResourceName != null) {
      gcsClient.uploadArtifact(
          "session.json", Resources.getResource(sessionFileResourceName).getPath());
      flexTemplateBuilder.addParameter("sessionFilePath", getGcsPath("session.json"));
    }

    if (schemaOverridesFileResourceName != null) {
      gcsClient.uploadArtifact(
          "schema_overrides.json",
          Resources.getResource(schemaOverridesFileResourceName).getPath());
      flexTemplateBuilder.addParameter(
          "schemaOverridesFilePath", getGcsPath("schema_overrides.json"));
    }

    if (tableOverrides != null) {
      flexTemplateBuilder.addParameter("tableOverrides", tableOverrides);
    }

    if (columnOverrides != null) {
      flexTemplateBuilder.addParameter("columnOverrides", columnOverrides);
    }

    if (customTransformation != null) {
      flexTemplateBuilder.addParameter(
          "transformationJarPath", getGcsPath(customTransformation.jarPath()));
      flexTemplateBuilder.addParameter("transformationClassName", customTransformation.classPath());
      if (customTransformation.customParameters() != null) {
        flexTemplateBuilder.addParameter(
            "transformationCustomParameters", customTransformation.customParameters());
      }
    }

    String runId = PipelineUtils.createJobName(testId);
    flexTemplateBuilder.addParameter("runId", runId);
    flexTemplateBuilder.addParameter("workerMachineType", "n2-standard-4");

    if (jobParameters != null) {
      jobParameters.forEach(flexTemplateBuilder::addParameter);
    }

    return flexTemplateBuilder.build().launchJob();
  }
}
