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
import com.google.common.base.MoreObjects;
import com.google.common.io.Resources;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineLauncher.LaunchConfig;
import org.apache.beam.it.common.PipelineLauncher.LaunchInfo;
import org.apache.beam.it.common.TestProperties;
import org.apache.beam.it.common.utils.PipelineUtils;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.TemplateLoadTestBase;
import org.apache.beam.it.gcp.artifacts.utils.ArtifactUtils;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.gcp.storage.GcsResourceManager;
import org.apache.beam.it.jdbc.JDBCResourceManager;
import org.apache.beam.it.jdbc.MySQLResourceManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for Spanner to sourcedb Load tests. It provides helper functions related to
 * environment setup and assertConditions.
 */
public class SpannerToJdbcLTBase extends TemplateLoadTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(SpannerToJdbcLTBase.class);
  private static final String READER_SPEC_PATH =
      MoreObjects.firstNonNull(
          TestProperties.specPath(),
          "gs://dataflow-templates/latest/flex/Spanner_Change_Streams_to_Sharded_File_Sink");
  private static final String WRITER_SPEC_PATH =
      MoreObjects.firstNonNull(
          TestProperties.specPath(), "gs://dataflow-templates/latest/flex/GCS_to_Sourcedb");
  public SpannerResourceManager spannerResourceManager;
  public SpannerResourceManager spannerMetadataResourceManager;
  public List<JDBCResourceManager> jdbcResourceManagers;
  public GcsResourceManager gcsResourceManager;

  public void setupResourceManagers(
      String spannerDdlResource, String sessionFileResource, String artifactBucket)
      throws IOException {
    spannerResourceManager = createSpannerDatabase(spannerDdlResource);
    spannerMetadataResourceManager = createSpannerMetadataDatabase();

    gcsResourceManager =
        GcsResourceManager.builder(artifactBucket, getClass().getSimpleName(), CREDENTIALS).build();

    gcsResourceManager.uploadArtifact(
        "input/session.json", Resources.getResource(sessionFileResource).getPath());
  }

  public void setupMySQLResourceManager(int numShards) throws IOException {
    jdbcResourceManagers = new ArrayList<>();
    for (int i = 0; i < numShards; ++i) {
      jdbcResourceManagers.add(MySQLResourceManager.builder(testName).build());
    }

    createAndUploadShardConfigToGcs(gcsResourceManager, jdbcResourceManagers);
  }

  public void cleanupResourceManagers() {
    ResourceManagerUtils.cleanResources(
        spannerResourceManager, spannerMetadataResourceManager, gcsResourceManager);
    for (JDBCResourceManager jdbcResourceManager : jdbcResourceManagers) {
      ResourceManagerUtils.cleanResources(jdbcResourceManager);
    }
  }

  public SpannerResourceManager createSpannerDatabase(String spannerDdlResourceFile)
      throws IOException {
    SpannerResourceManager spannerResourceManager =
        SpannerResourceManager.builder("rr-main-" + testName, project, region)
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
        SpannerResourceManager.builder("rr-meta-" + testName, project, region)
            .maybeUseStaticInstance()
            .build();
    String dummy = "CREATE TABLE IF NOT EXISTS t1(id INT64 ) primary key(id)";
    spannerMetadataResourceManager.executeDdlStatement(dummy);
    return spannerMetadataResourceManager;
  }

  public void createAndUploadShardConfigToGcs(
      GcsResourceManager gcsResourceManager, List<JDBCResourceManager> jdbcResourceManagers)
      throws IOException {
    JsonArray ja = new JsonArray();
    for (int i = 0; i < 1; ++i) {
      if (jdbcResourceManagers.get(i) instanceof MySQLResourceManager) {
        MySQLResourceManager resourceManager = (MySQLResourceManager) jdbcResourceManagers.get(i);
        Shard shard = new Shard();
        shard.setLogicalShardId("Shard" + (i + 1));
        shard.setUser(jdbcResourceManagers.get(i).getUsername());
        shard.setHost(resourceManager.getHost());
        shard.setPassword(jdbcResourceManagers.get(i).getPassword());
        shard.setPort(String.valueOf(resourceManager.getPort()));
        shard.setDbName(jdbcResourceManagers.get(i).getDatabaseName());
        JsonObject jsObj = (JsonObject) new Gson().toJsonTree(shard).getAsJsonObject();
        jsObj.remove("secretManagerUri"); // remove field secretManagerUri
        ja.add(jsObj);
      } else {
        throw new UnsupportedOperationException(
            jdbcResourceManagers.get(i).getClass().getSimpleName() + " is not supported");
      }
    }
    String shardFileContents = ja.toString();
    LOG.info("Shard file contents: {}", shardFileContents);
    gcsResourceManager.createArtifact("input/shard.json", shardFileContents);
  }

  public LaunchInfo launchReaderDataflowJob(
      GcsResourceManager gcsResourceManager,
      SpannerResourceManager spannerResourceManager,
      SpannerResourceManager spannerMetadataResourceManager,
      String artifactBucket,
      int numWorkers,
      int maxWorkers)
      throws IOException {
    Map<String, String> params =
        new HashMap<>() {
          {
            put(
                "sessionFilePath",
                getGcsPath(artifactBucket, "input/session.json", gcsResourceManager));
            put("instanceId", spannerResourceManager.getInstanceId());
            put("databaseId", spannerResourceManager.getDatabaseId());
            put("spannerProjectId", project);
            put("metadataDatabase", spannerMetadataResourceManager.getDatabaseId());
            put("metadataInstance", spannerMetadataResourceManager.getInstanceId());
            put(
                "sourceShardsFilePath",
                getGcsPath(artifactBucket, "input/shard.json", gcsResourceManager));
            put("changeStreamName", "allstream");
            put("runIdentifier", "run1");
            put("gcsOutputDirectory", getGcsPath(artifactBucket, "output", gcsResourceManager));
          }
        };

    LaunchConfig.Builder options =
        LaunchConfig.builder(getClass().getSimpleName(), READER_SPEC_PATH);
    options
        .addEnvironment("maxWorkers", maxWorkers)
        .addEnvironment("numWorkers", numWorkers)
        .addEnvironment("additionalExperiments", Collections.singletonList("use_runner_v2"));

    options.setParameters(params);
    PipelineLauncher.LaunchInfo jobInfo = pipelineLauncher.launch(project, region, options.build());

    return jobInfo;
  }

  public LaunchInfo launchWriterDataflowJob(
      GcsResourceManager gcsResourceManager,
      SpannerResourceManager spannerMetadataResourceManager,
      String artifactBucket,
      int numWorkers,
      int maxWorkers)
      throws IOException {
    Map<String, String> params =
        new HashMap<>() {
          {
            put(
                "sessionFilePath",
                getGcsPath(artifactBucket, "input/session.json", gcsResourceManager));
            put("spannerProjectId", project);
            put("metadataDatabase", spannerMetadataResourceManager.getDatabaseId());
            put("metadataInstance", spannerMetadataResourceManager.getInstanceId());
            put(
                "sourceShardsFilePath",
                getGcsPath(artifactBucket, "input/shard.json", gcsResourceManager));
            put("runIdentifier", "run1");
            put("GCSInputDirectoryPath", getGcsPath(artifactBucket, "output", gcsResourceManager));
          }
        };
    String jobName = PipelineUtils.createJobName(testName);
    LaunchConfig.Builder options = LaunchConfig.builder(jobName, WRITER_SPEC_PATH);
    options.addEnvironment("maxWorkers", maxWorkers).addEnvironment("numWorkers", numWorkers);
    options.setParameters(params);
    // Run
    LaunchInfo writerJobInfo = pipelineLauncher.launch(project, region, options.build());
    return writerJobInfo;
  }

  public String getGcsPath(
      String bucket, String artifactId, GcsResourceManager gcsResourceManager) {
    return ArtifactUtils.getFullGcsPath(
        bucket, getClass().getSimpleName(), gcsResourceManager.runId(), artifactId);
  }
}
