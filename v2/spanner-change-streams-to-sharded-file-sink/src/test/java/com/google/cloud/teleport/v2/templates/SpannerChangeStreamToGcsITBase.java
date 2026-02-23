/*
 * Copyright (C) 2024 Google LLC
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

import com.google.common.io.Resources;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.utils.IORedirectUtil;
import org.apache.beam.it.common.utils.PipelineUtils;
import org.apache.beam.it.gcp.TemplateTestBase;
import org.apache.beam.it.gcp.artifacts.utils.ArtifactUtils;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.gcp.storage.GcsResourceManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for SpannerChangeStreamToGcs integration tests. It provides helper functions related
 * to environment setup.
 */
public class SpannerChangeStreamToGcsITBase extends TemplateTestBase {

  public static String spannerDatabaseName = "";
  public static String spannerMetadataDatabaseName = "";
  private static final Logger LOG = LoggerFactory.getLogger(SpannerChangeStreamToGcsITBase.class);

  public GcsResourceManager createGcsResourceManager(String identifierSuffix) {
    return GcsResourceManager.builder(artifactBucketName, identifierSuffix, credentials)
        .build(); // DB name is appended with prefix to avoid clashes
  }

  public SpannerResourceManager createSpannerResourceManager() {
    return SpannerResourceManager.builder("rr-main-" + testName, PROJECT, REGION)
        .maybeUseStaticInstance()
        .build();
  }

  public void createSpannerDatabase(
      SpannerResourceManager spannerResourceManager, String spannerDdl) throws IOException {
    String ddl =
        String.join(
            " ", Resources.readLines(Resources.getResource(spannerDdl), StandardCharsets.UTF_8));
    ddl = ddl.trim();
    String[] ddls = ddl.split(";");
    for (String d : ddls) {
      if (!d.isBlank()) {
        spannerResourceManager.executeDdlStatement(d);
      }
    }
    spannerDatabaseName = spannerResourceManager.getDatabaseId();
  }

  public void uploadSessionFileToGcs(
      GcsResourceManager gcsResourceManager, String sessionFileResourceName) throws IOException {
    gcsResourceManager.uploadArtifact(
        "input/session.json", Resources.getResource(sessionFileResourceName).getPath());
  }

  public SpannerResourceManager createSpannerMetadataResourceManager() {
    return SpannerResourceManager.builder("rr-meta-" + testName, PROJECT, REGION)
        .maybeUseStaticInstance()
        .build();
  }

  public void createSpannerMetadataDatabase(SpannerResourceManager spannerMetadataResourceManager) {
    String dummy = "CREATE TABLE IF NOT EXISTS t1(id INT64 ) primary key(id)";
    spannerMetadataResourceManager.executeDdlStatement(dummy);
    // needed to create separate metadata database
    spannerMetadataDatabaseName = spannerMetadataResourceManager.getDatabaseId();
  }

  public void createAndUploadJarToGcs(GcsResourceManager gcsResourceManager)
      throws IOException, InterruptedException {
    String[] shellCommand = {"/bin/bash", "-c", "cd ../spanner-custom-shard"};

    Process exec = Runtime.getRuntime().exec(shellCommand);

    IORedirectUtil.redirectLinesLog(exec.getInputStream(), LOG);
    IORedirectUtil.redirectLinesLog(exec.getErrorStream(), LOG);

    if (exec.waitFor() != 0) {
      throw new RuntimeException("Error staging template, check Maven logs.");
    }
    gcsResourceManager.uploadArtifact(
        "input/customShard.jar",
        "../spanner-custom-shard/target/spanner-custom-shard-1.0-SNAPSHOT.jar");
  }

  public String getGcsFullPath(
      GcsResourceManager gcsResourceManager, String artifactId, String identifierSuffix) {
    return ArtifactUtils.getFullGcsPath(
        artifactBucketName, identifierSuffix, gcsResourceManager.runId(), artifactId);
  }

  public void prepareLaunchParameters(
      GcsResourceManager gcsResourceManager,
      SpannerResourceManager spannerResourceManager,
      SpannerResourceManager spannerMetadataResourceManager,
      String spannerDdl,
      String sessionFileResourceName)
      throws IOException {
    createSpannerDatabase(spannerResourceManager, spannerDdl);
    if (sessionFileResourceName != null) {
      uploadSessionFileToGcs(gcsResourceManager, sessionFileResourceName);
    }
    createSpannerMetadataDatabase(spannerMetadataResourceManager);
  }

  public PipelineLauncher.LaunchInfo launchReaderDataflowJob(
      GcsResourceManager gcsResourceManager,
      SpannerResourceManager spannerResourceManager,
      SpannerResourceManager spannerMetadataResourceManager,
      String identifierSuffix,
      String shardingCustomJarPath,
      String shardingCustomClassName,
      boolean isMultiShard)
      throws IOException {
    // default parameters
    Map<String, String> params =
        new HashMap<>() {
          {
            put("instanceId", spannerResourceManager.getInstanceId());
            put("databaseId", spannerResourceManager.getDatabaseId());
            put("spannerProjectId", PROJECT);
            put("metadataDatabase", spannerMetadataResourceManager.getDatabaseId());
            put("metadataInstance", spannerMetadataResourceManager.getInstanceId());
            put(
                "sourceShardsFilePath",
                getGcsFullPath(gcsResourceManager, "input/shard.json", identifierSuffix));
            put("changeStreamName", "allstream");
            put("runIdentifier", "run1");
            put(
                "gcsOutputDirectory",
                getGcsFullPath(gcsResourceManager, "output", identifierSuffix));
          }
        };

    if (shardingCustomJarPath != null) {
      params.put(
          "shardingCustomJarPath",
          getGcsFullPath(gcsResourceManager, shardingCustomJarPath, identifierSuffix));
    }
    if (shardingCustomClassName != null) {
      params.put("shardingCustomClassName", shardingCustomClassName);
    }
    if (isMultiShard) {
      params.put(
          "sessionFilePath",
          getGcsFullPath(gcsResourceManager, "input/session.json", identifierSuffix));
    }

    // Construct template
    String jobName = PipelineUtils.createJobName("rr-it");
    // /-DunifiedWorker=true when using runner v2
    PipelineLauncher.LaunchConfig.Builder options =
        PipelineLauncher.LaunchConfig.builder(jobName, specPath);
    options.setParameters(params);
    options.addEnvironment("additionalExperiments", Collections.singletonList("use_runner_v2"));
    // Run
    PipelineLauncher.LaunchInfo jobInfo = launchTemplate(options);
    assertThatPipeline(jobInfo).isRunning();
    return jobInfo;
  }
}
