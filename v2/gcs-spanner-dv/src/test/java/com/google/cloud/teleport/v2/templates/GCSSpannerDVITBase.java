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

import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;

import com.google.cloud.teleport.v2.spanner.migrations.transformation.CustomTransformation;
import com.google.common.io.Resources;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.it.common.PipelineLauncher.LaunchConfig;
import org.apache.beam.it.common.PipelineLauncher.LaunchInfo;
import org.apache.beam.it.common.utils.PipelineUtils;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.TemplateTestBase;
import org.apache.beam.it.gcp.bigquery.BigQueryResourceManager;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.junit.After;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for gcs-spanner-dv integration tests. It provides helper functions related to
 * environment setup and database initialization.
 */
public abstract class GCSSpannerDVITBase extends TemplateTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(GCSSpannerDVITBase.class);

  protected SpannerResourceManager spannerResourceManager;
  protected BigQueryResourceManager bigQueryResourceManager;

  public SpannerResourceManager setUpSpannerResourceManager() {
    spannerResourceManager = SpannerResourceManager.builder(testName, PROJECT, REGION)
        .maybeUseStaticInstance()
        .build();
    return spannerResourceManager;
  }

  public BigQueryResourceManager setUpBigQueryResourceManager() {
    bigQueryResourceManager = BigQueryResourceManager.builder(testName, PROJECT, credentials).build();
    return bigQueryResourceManager;
  }

  @After
  public void tearDownBase() {
    if (spannerResourceManager != null) {
      spannerResourceManager.cleanupAll();
    }
    if (bigQueryResourceManager != null) {
      bigQueryResourceManager.cleanupAll();
    }
  }

  /**
   * Helper function for creating Spanner DDL. Reads the sql file from resources directory and
   * applies the DDL to Spanner instance.
   *
   * @param spannerResourceManager Initialized SpannerResourceManager instance
   * @param resourceName SQL file name with path relative to resources directory
   */
  public static void createSpannerDDL(
      SpannerResourceManager spannerResourceManager, String resourceName) throws IOException {
    String ddl =
        String.join(
            "\n", Resources.readLines(Resources.getResource(resourceName), StandardCharsets.UTF_8));
    List<String> ddls = Arrays.stream(ddl.split(";"))
        .map(String::trim)
        .filter(s -> !s.isEmpty())
        .toList();
    spannerResourceManager.executeDdlStatements(ddls);
  }

  /**
   * Helper function for executing Spanner DML. Reads the sql file from resources directory and
   * applies the DML to Spanner instance.
   *
   * @param spannerResourceManager Initialized SpannerResourceManager instance
   * @param resourceName SQL file name with path relative to resources directory
   */
  public static void executeSpannerDML(
      SpannerResourceManager spannerResourceManager, String resourceName) throws IOException {
    String dml =
        String.join(
            "\n", Resources.readLines(Resources.getResource(resourceName), StandardCharsets.UTF_8));
    List<String> dmls = Arrays.stream(dml.split(";"))
        .map(String::trim)
        .filter(s -> !s.isEmpty())
        .toList();
    spannerResourceManager.executeDMLStatements(dmls);
  }

  /**
   * Helper function for uploading mock Avro files to GCS.
   *
   * @param gcsPrefix GCS prefix
   * @param resourceFileName Mock Avro file in resources directory
   * @throws IOException If an I/O error occurs
   */
  protected void uploadMockAvroFiles(String gcsPrefix, String resourceFileName) throws IOException {
    String destinationPath = gcsPrefix + "/" + resourceFileName;
    byte[] bytes = Resources.toByteArray(Resources.getResource(resourceFileName));
    java.nio.file.Path tempFile = java.nio.file.Files.createTempFile("mock", ".avro");
    java.nio.file.Files.write(tempFile, bytes);
    gcsClient.copyFileToGcs(tempFile.toAbsolutePath(), destinationPath);
    java.nio.file.Files.delete(tempFile);
  }

  /**
   * Launches the Dataflow job.
   *
   * @param options Pipeline launcher options
   * @param testId Test identifier
   * @param projectId Project ID
   * @param spannerResourceManager Spanner resource manager
   * @param bigQueryDataset BigQuery dataset
   * @param gcsInputDirectory GCS input directory
   * @param sessionFileResourceName Session file name with path relative to resources directory
   * @param schemaOverridesFileResourceName Schema overrides file name with path relative to
   *     resources directory
   * @param tableOverrides Table overrides mapping string
   * @param columnOverrides Column overrides mapping string
   * @param customTransformation Custom transformation logic
   * @param jobParameters Additional job parameters
   * @return LaunchInfo containing job information
   * @throws IOException If an I/O error occurs
   */
  protected LaunchInfo launchDataflowJob(
      LaunchConfig.Builder options,
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
      Map<String, String> jobParameters)
      throws IOException {

    Map<String, String> params = new HashMap<>();
    params.put("projectId", projectId);
    params.put("instanceId", spannerResourceManager.getInstanceId());
    params.put("databaseId", spannerResourceManager.getDatabaseId());
    params.put("bigQueryDataset", bigQueryDataset);
    params.put("gcsInputDirectory", gcsInputDirectory);

    if (sessionFileResourceName != null) {
      LOG.info("uploading session file from resource: {}", sessionFileResourceName);
      gcsClient.uploadArtifact(
          "session.json", Resources.getResource(sessionFileResourceName).getPath());
      params.put("sessionFilePath", getGcsPath("session.json"));
    }

    if (schemaOverridesFileResourceName != null) {
      LOG.info(
          "uploading schema overrides file from resource: {}", schemaOverridesFileResourceName);
      gcsClient.uploadArtifact(
          "schema_overrides.json",
          Resources.getResource(schemaOverridesFileResourceName).getPath());
      params.put("schemaOverridesFilePath", getGcsPath("schema_overrides.json"));
    }

    if (tableOverrides != null) {
      params.put("tableOverrides", tableOverrides);
    }

    if (columnOverrides != null) {
      params.put("columnOverrides", columnOverrides);
    }

    if (customTransformation != null) {
      LOG.info("Custom transformation provided: {}", customTransformation.classPath());
      params.put("transformationJarPath", getGcsPath(customTransformation.jarPath()));
      params.put("transformationClassName", customTransformation.classPath());
      if (customTransformation.customParameters() != null) {
        params.put("transformationCustomParameters", customTransformation.customParameters());
      }
    }

    // Generate a runId for the validation run
    String runId = PipelineUtils.createJobName(testId);
    params.put("runId", runId);
    params.put("workerMachineType", "n2-standard-4");

    // overridden parameters
    if (jobParameters != null) {
      params.putAll(jobParameters);
    }

    options.setParameters(params);
    options.addEnvironment("ipConfiguration", "WORKER_IP_PRIVATE");

    LaunchInfo jobInfo = launchTemplate(options);
    assertThatPipeline(jobInfo).isRunning();

    return jobInfo;
  }
}
