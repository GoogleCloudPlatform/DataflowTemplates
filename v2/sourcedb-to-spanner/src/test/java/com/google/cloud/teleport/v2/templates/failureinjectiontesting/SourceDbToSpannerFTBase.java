/*
 * Copyright (C) 2025 Google LLC
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
package com.google.cloud.teleport.v2.templates.failureinjectiontesting;

import static java.util.Arrays.stream;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.conditions.ConditionCheck;
import org.apache.beam.it.gcp.TemplateTestBase;
import org.apache.beam.it.gcp.cloudsql.CloudSqlResourceManager;
import org.apache.beam.it.gcp.dataflow.FlexTemplateDataflowJobResourceManager;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.gcp.storage.GcsResourceManager;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class SourceDbToSpannerFTBase extends TemplateTestBase {

  public static final String SEVERE_ERRORS_COUNTER_NAME = "mutation_groups_write_fail";
  private static final Logger LOG = LoggerFactory.getLogger(SourceDbToSpannerFTBase.class);

  protected SpannerResourceManager createSpannerDatabase(String spannerSchemaFile)
      throws IOException {
    SpannerResourceManager spannerResourceManager =
        SpannerResourceManager.builder("ft-" + testName, PROJECT, REGION)
            .maybeUseStaticInstance()
            .build();

    String ddl;
    try (InputStream inputStream =
        Thread.currentThread().getContextClassLoader().getResourceAsStream(spannerSchemaFile)) {
      if (inputStream == null) {
        throw new FileNotFoundException("Resource file not found: " + spannerSchemaFile);
      }
      try (BufferedReader reader =
          new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
        ddl = reader.lines().collect(Collectors.joining("\n"));
      }
    }

    if (ddl.isBlank()) {
      throw new IllegalStateException("DDL file is empty: " + spannerSchemaFile);
    }

    List<String> ddls =
        stream(ddl.trim().split(";")).filter(s -> !s.isBlank()).collect(Collectors.toList());
    spannerResourceManager.executeDdlStatements(ddls);
    return spannerResourceManager;
  }

  public String getGcsPath(String... pathParts) {
    checkArgument(pathParts.length != 0, "Must provide at least one path part");
    checkArgument(
        stream(pathParts).noneMatch(Strings::isNullOrEmpty), "No path part can be null or empty");

    return String.format("gs://%s", String.join("/", pathParts));
  }

  protected PipelineLauncher.LaunchInfo launchBulkDataflowJob(
      String jobName,
      SpannerResourceManager spannerResourceManager,
      GcsResourceManager gcsResourceManager,
      CloudSqlResourceManager cloudSqlResourceManager)
      throws IOException {
    // launch dataflow template
    FlexTemplateDataflowJobResourceManager flexTemplateDataflowJobResourceManager =
        FlexTemplateDataflowJobResourceManager.builder(jobName)
            .withTemplateName("Sourcedb_to_Spanner_Flex")
            .withTemplateModulePath("v2/sourcedb-to-spanner")
            .addParameter("instanceId", spannerResourceManager.getInstanceId())
            .addParameter("databaseId", spannerResourceManager.getDatabaseId())
            .addParameter("projectId", PROJECT)
            .addParameter("outputDirectory", "gs://" + artifactBucketName)
            // .addParameter("sessionFilePath", getGcsPath("input/session.json",
            // gcsResourceManager))
            .addParameter("sourceConfigURL", cloudSqlResourceManager.getUri())
            .addParameter("username", cloudSqlResourceManager.getUsername())
            .addParameter("password", cloudSqlResourceManager.getPassword())
            .addParameter("jdbcDriverClassName", "com.mysql.jdbc.Driver")
            .addEnvironmentVariable(
                "additionalExperiments", Collections.singletonList("disable_runner_v2"))
            .build();

    // Run
    PipelineLauncher.LaunchInfo jobInfo = flexTemplateDataflowJobResourceManager.launchJob();
    return jobInfo;
  }

  protected PipelineLauncher.LaunchInfo launchShardedBulkDataflowJob(
      String jobName,
      SpannerResourceManager spannerResourceManager,
      GcsResourceManager gcsResourceManager)
      throws IOException {

    FlexTemplateDataflowJobResourceManager flexTemplateDataflowJobResourceManager =
        FlexTemplateDataflowJobResourceManager.builder(jobName)
            .withTemplateName("Sourcedb_to_Spanner_Flex")
            .withTemplateModulePath("v2/sourcedb-to-spanner")
            .addParameter("instanceId", spannerResourceManager.getInstanceId())
            .addParameter("databaseId", spannerResourceManager.getDatabaseId())
            .addParameter("projectId", PROJECT)
            .addParameter("outputDirectory", "gs://" + artifactBucketName)
            .addParameter("sessionFilePath", getGcsPath("input/session.json", gcsResourceManager))
            .addParameter(
                "sourceConfigURL", getGcsPath("input/shard-bulk.json", gcsResourceManager))
            .addEnvironmentVariable(
                "additionalExperiments", Collections.singletonList("disable_runner_v2"))
            .build();

    PipelineLauncher.LaunchInfo jobInfo = flexTemplateDataflowJobResourceManager.launchJob();
    return jobInfo;
  }

  protected void createAndUploadBulkShardConfigToGcs(
      ArrayList<DataShard> dataShardsList, GcsResourceManager gcsResourceManager) {
    JSONObject bulkConfig = new JSONObject();
    bulkConfig.put("configType", "dataflow");

    JSONObject shardConfigBulk = new JSONObject();

    JSONObject schemaSourceJson = new JSONObject();
    schemaSourceJson.put("dataShardId", "");
    schemaSourceJson.put("host", "");
    schemaSourceJson.put("user", "");
    schemaSourceJson.put("password", "");
    schemaSourceJson.put("port", "");
    schemaSourceJson.put("dbName", "");
    shardConfigBulk.put("schemaSource", schemaSourceJson);

    JSONArray dataShardsArray = new JSONArray();
    if (dataShardsList != null) {
      for (DataShard shardData : dataShardsList) {
        JSONObject shardJson = new JSONObject();

        shardJson.put("dataShardId", shardData.dataShardId);
        shardJson.put("host", shardData.host);
        shardJson.put("user", shardData.user);
        shardJson.put("password", shardData.password);
        shardJson.put("port", shardData.port);
        shardJson.put("dbName", shardData.dbName);
        shardJson.put("namespace", shardData.namespace);
        shardJson.put("connectionProperties", shardData.connectionProperties);

        JSONArray databasesArray = new JSONArray();

        for (Database dbData : shardData.databases) {
          JSONObject dbJson = new JSONObject();
          dbJson.put("dbName", dbData.dbName);
          dbJson.put("databaseId", dbData.databaseId);
          dbJson.put("refDataShardId", dbData.refDataShardId);
          databasesArray.put(dbJson);
        }
        shardJson.put("databases", databasesArray);
        dataShardsArray.put(shardJson);
      }
    }
    shardConfigBulk.put("dataShards", dataShardsArray);

    bulkConfig.put("shardConfigurationBulk", shardConfigBulk);
    String shardFileContents = bulkConfig.toString();
    LOG.info("Shard file contents: {}", shardFileContents);
    gcsResourceManager.createArtifact("input/shard-bulk.json", shardFileContents);
  }

  protected class DataShard {
    String dataShardId;
    String host;
    String user;
    String password;
    String port;
    String dbName;
    String namespace;
    String connectionProperties;
    ArrayList<Database> databases;

    public DataShard(
        String dataShardId,
        String host,
        String user,
        String password,
        String port,
        String dbName,
        String namespace,
        String connectionProperties,
        ArrayList<Database> databases) {

      this.dataShardId = dataShardId;
      this.host = host;
      this.user = user;
      this.password = password;
      this.port = port;
      this.dbName = dbName;
      this.namespace = namespace;
      this.connectionProperties = connectionProperties;
      this.databases = databases;
    }
  }

  protected class Database {
    String dbName;
    String databaseId;
    String refDataShardId;

    public Database(String dbName, String databaseId, String refDataShardId) {
      this.dbName = dbName;
      this.databaseId = databaseId;
      this.refDataShardId = refDataShardId;
    }
  }

  public static class SevereErrorsCheck extends ConditionCheck {

    private Integer minErrors;

    private PipelineLauncher.LaunchInfo jobInfo;

    private PipelineLauncher pipelineLauncher;

    @Override
    public String getDescription() {
      return String.format("Severe errors check if %d errors are present", minErrors);
    }

    @Override
    public CheckResult check() {
      Double severeErrors = null;
      try {
        severeErrors =
            pipelineLauncher.getMetric(
                PROJECT, REGION, jobInfo.jobId(), SEVERE_ERRORS_COUNTER_NAME);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      if (severeErrors < minErrors) {
        return new CheckResult(
            false,
            String.format(
                "Expected at least %d errors but has only %.1f", minErrors, severeErrors));
      }
      return new CheckResult(
          true,
          String.format("Expected at least %d errors and found %.1f", minErrors, severeErrors));
    }

    public SevereErrorsCheck(
        PipelineLauncher pipelineLauncher, PipelineLauncher.LaunchInfo jobInfo, int minErrors) {
      this.pipelineLauncher = pipelineLauncher;
      this.jobInfo = jobInfo;
      this.minErrors = minErrors;
    }
  }
}
