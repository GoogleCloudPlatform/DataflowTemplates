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

import com.google.cloud.teleport.v2.spanner.migrations.shard.Shard;
import com.google.common.io.Resources;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.it.common.PipelineLauncher.LaunchConfig;
import org.apache.beam.it.common.PipelineLauncher.LaunchInfo;
import org.apache.beam.it.common.utils.PipelineUtils;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.TemplateTestBase;
import org.apache.beam.it.gcp.dataflow.FlexTemplateDataflowJobResourceManager;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.gcp.storage.GcsResourceManager;
import org.apache.beam.it.jdbc.CustomMySQLResourceManager;
import org.apache.beam.it.jdbc.JDBCResourceManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GCSToSourceDbITBase extends TemplateTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(GCSToSourceDbITBase.class);

  public static void cleanupResourceManagers(
      SpannerResourceManager spannerResourceManager,
      SpannerResourceManager spannerMetadataResourceManager,
      GcsResourceManager gcsResourceManager,
      FlexTemplateDataflowJobResourceManager flexTemplateDataflowJobResourceManager,
      List<? extends JDBCResourceManager> jdbcResourceManagers) {
    ResourceManagerUtils.cleanResources(
        spannerResourceManager,
        spannerMetadataResourceManager,
        gcsResourceManager,
        flexTemplateDataflowJobResourceManager);
    for (JDBCResourceManager jdbcResourceManager : jdbcResourceManagers) {
      ResourceManagerUtils.cleanResources(jdbcResourceManager);
    }
  }

  public SpannerResourceManager createSpannerDatabase(String spannerDdlResourceFile)
      throws IOException {
    SpannerResourceManager spannerResourceManager =
        SpannerResourceManager.builder("rr-main-" + testName, PROJECT, REGION)
            .maybeUseStaticInstance()
            .build();
    String ddl =
        String.join(
            " ",
            Resources.readLines(
                Resources.getResource(spannerDdlResourceFile), StandardCharsets.UTF_8));
    ddl = ddl.trim();
    String[] ddls = ddl.split(";");
    for (String d : ddls) {
      if (!d.isBlank()) {
        spannerResourceManager.executeDdlStatement(d);
      }
    }
    return spannerResourceManager;
  }

  public SpannerResourceManager createSpannerMetadataDatabase() throws IOException {
    SpannerResourceManager spannerMetadataResourceManager =
        SpannerResourceManager.builder("rr-meta-" + testName, PROJECT, REGION)
            .maybeUseStaticInstance()
            .build();
    String dummy = "create table t1(id INT64 ) primary key(id)";
    spannerMetadataResourceManager.executeDdlStatement(dummy);
    return spannerMetadataResourceManager;
  }

  public void createAndUploadShardConfigToGcs(
      GcsResourceManager gcsResourceManager, List<CustomMySQLResourceManager> jdbcResourceManagers)
      throws IOException {
    JsonArray ja = new JsonArray();
    for (int i = 0; i < 1; ++i) {
      Shard shard = new Shard();
      shard.setLogicalShardId("Shard" + (i + 1));
      shard.setUser(jdbcResourceManagers.get(i).getUsername());
      shard.setHost(jdbcResourceManagers.get(i).getHost());
      shard.setPassword(jdbcResourceManagers.get(i).getPassword());
      shard.setPort(String.valueOf(jdbcResourceManagers.get(i).getPort()));
      shard.setDbName(jdbcResourceManagers.get(i).getDatabaseName());
      JsonObject jsObj = (JsonObject) new Gson().toJsonTree(shard).getAsJsonObject();
      jsObj.remove("secretManagerUri"); // remove field secretManagerUri
      ja.add(jsObj);
    }
    String shardFileContents = ja.toString();
    LOG.info("Shard file contents: {}", shardFileContents);
    gcsResourceManager.createArtifact("input/shard.json", shardFileContents);
  }

  public GcsResourceManager createGcsResourceManager() {
    return GcsResourceManager.builder(artifactBucketName, getClass().getSimpleName(), credentials)
        .build();
  }

  public LaunchInfo launchReaderDataflowJob(
      GcsResourceManager gcsResourceManager,
      SpannerResourceManager spannerResourceManager,
      SpannerResourceManager spannerMetadataResourceManager)
      throws IOException {
    // default parameters
    FlexTemplateDataflowJobResourceManager flexTemplateDataflowJobResourceManager =
        FlexTemplateDataflowJobResourceManager.builder(getClass().getSimpleName())
            .withTemplateName("Spanner_Change_Streams_to_Sharded_File_Sink")
            .withTemplateModulePath("v2/spanner-change-streams-to-sharded-file-sink")
            .addParameter("sessionFilePath", getGcsPath("input/session.json", gcsResourceManager))
            .addParameter("instanceId", spannerResourceManager.getInstanceId())
            .addParameter("databaseId", spannerResourceManager.getDatabaseId())
            .addParameter("spannerProjectId", PROJECT)
            .addParameter("metadataDatabase", spannerMetadataResourceManager.getDatabaseId())
            .addParameter("metadataInstance", spannerMetadataResourceManager.getInstanceId())
            .addParameter(
                "sourceShardsFilePath", getGcsPath("input/shard.json", gcsResourceManager))
            .addParameter("changeStreamName", "allstream")
            .addParameter("runIdentifier", "run1")
            .addParameter("gcsOutputDirectory", getGcsPath("output", gcsResourceManager))
            .addEnvironmentVariable(
                "additionalExperiments", Collections.singletonList("use_runner_v2"))
            .build();
    // Run
    return flexTemplateDataflowJobResourceManager.launchJob();
  }

  public LaunchInfo launchWriterDataflowJob(
      GcsResourceManager gcsResourceManager, SpannerResourceManager spannerMetadataResourceManager)
      throws IOException {
    return launchWriterDataflowJob(gcsResourceManager, spannerMetadataResourceManager, null);
  }

  public LaunchInfo launchWriterDataflowJob(
      GcsResourceManager gcsResourceManager,
      SpannerResourceManager spannerMetadataResourceManager,
      Map<String, String> paramOverrides)
      throws IOException {
    Map<String, String> params =
        new HashMap<>() {
          {
            put("sessionFilePath", getGcsPath("input/session.json", gcsResourceManager));
            put("spannerProjectId", PROJECT);
            put("metadataDatabase", spannerMetadataResourceManager.getDatabaseId());
            put("metadataInstance", spannerMetadataResourceManager.getInstanceId());
            put("sourceShardsFilePath", getGcsPath("input/shard.json", gcsResourceManager));
            put("runIdentifier", "run1");
            put("GCSInputDirectoryPath", getGcsPath("output", gcsResourceManager));
          }
        };

    if (paramOverrides != null) {
      paramOverrides.forEach((key, value) -> params.put(key, value));
    }
    String jobName = PipelineUtils.createJobName(testName);
    LaunchConfig.Builder options = LaunchConfig.builder(jobName, specPath);
    options.setParameters(params);
    // Run
    LaunchInfo writerJobInfo = launchTemplate(options, false);
    return writerJobInfo;
  }
}
