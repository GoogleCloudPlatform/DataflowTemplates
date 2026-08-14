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

import com.google.common.io.Resources;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.gcp.cloudsql.CloudPostgresResourceManager;
import org.apache.beam.it.gcp.cloudsql.CloudSqlResourceManager;
import org.apache.beam.it.gcp.dataflow.FlexTemplateDataflowJobResourceManager;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.gcp.storage.GcsResourceManager;
import org.apache.beam.it.jdbc.JDBCResourceManager;
import org.json.JSONArray;
import org.json.JSONObject;

public abstract class EndToEndTestingITBase extends GCSSpannerDVITBase {
  private static final org.slf4j.Logger LOG =
      org.slf4j.LoggerFactory.getLogger(EndToEndTestingITBase.class);

  protected FlexTemplateDataflowJobResourceManager flexTemplateDataflowJobResourceManager;

  public record DataShard(
      String dataShardId,
      String host,
      String user,
      String password,
      String port,
      String dbName,
      String namespace,
      String connectionProperties,
      List<Database> databases) {}

  public record Database(String dbName, String databaseId, String refDataShardId) {}

  protected void createAndUploadShardConfigToGcs(
      List<DataShard> dataShardsList, GcsResourceManager gcsResourceManager) {
    JSONObject config = new JSONObject();
    JSONArray shardConfigs = new JSONArray();

    if (dataShardsList != null) {
      for (DataShard shardData : dataShardsList) {
        JSONObject shardJson = new JSONObject();
        shardJson.put("logicalShardId", shardData.dataShardId());
        shardJson.put("host", shardData.host());
        shardJson.put("user", shardData.user());
        shardJson.put("password", shardData.password());
        shardJson.put("port", shardData.port());
        shardJson.put("dbName", shardData.dbName());
        shardJson.put("namespace", shardData.namespace());
        shardJson.put("connectionProperties", shardData.connectionProperties());
        shardConfigs.put(shardJson);
      }
    }

    config.put("shardConfigs", shardConfigs);
    String shardFileContents = config.toString();
    gcsResourceManager.createArtifact("input/shard-config.json", shardFileContents);
  }

  protected PipelineLauncher.LaunchInfo launchBulkDataflowJob(
      String jobName,
      SpannerResourceManager spannerResourceManager,
      GcsResourceManager gcsResourceManager,
      CloudSqlResourceManager cloudSqlResourceManager,
      String sessionFileResourceName,
      boolean multiSharded)
      throws IOException {
    // launch dataflow template
    FlexTemplateDataflowJobResourceManager.Builder builder =
        FlexTemplateDataflowJobResourceManager.builder(jobName)
            .withTemplateName("Sourcedb_to_Spanner_Flex")
            .withTemplateModulePath("v2/sourcedb-to-spanner")
            .addParameter("instanceId", spannerResourceManager.getInstanceId())
            .addParameter("databaseId", spannerResourceManager.getDatabaseId())
            .addParameter("projectId", PROJECT)
            .addParameter("outputDirectory", "gs://" + artifactBucketName + "/" + testId)
            .addParameter("gcsOutputDirectory", "gs://" + artifactBucketName + "/" + testId)
            .addParameter("workerMachineType", "n2-standard-4")
            .addEnvironmentVariable(
                "additionalExperiments", Collections.singletonList("disable_runner_v2"));

    if (sessionFileResourceName != null) {
      LOG.info("Uploading session file from resource: {}", sessionFileResourceName);
      gcsResourceManager.uploadArtifact(
          "session.json", Resources.getResource(sessionFileResourceName).getPath());
      builder.addParameter("sessionFilePath", getGcsPath("session.json", gcsResourceManager));
    }

    String connectionProps = "useSSL=false&allowPublicKeyRetrieval=true";
    String jdbcDriver = "com.mysql.cj.jdbc.Driver";
    String dialect = "MYSQL";

    if (cloudSqlResourceManager instanceof CloudPostgresResourceManager) {
      connectionProps = null;
      jdbcDriver = "org.postgresql.Driver";
      dialect = "POSTGRESQL";
    }

    if (!multiSharded) {
      DataShard dataShard =
          new DataShard(
              "shard1",
              cloudSqlResourceManager.getHost(),
              cloudSqlResourceManager.getUsername(),
              cloudSqlResourceManager.getPassword(),
              String.valueOf(cloudSqlResourceManager.getPort()),
              cloudSqlResourceManager.getDatabaseName(),
              null,
              connectionProps,
              Collections.emptyList());
      createAndUploadShardConfigToGcs(Collections.singletonList(dataShard), gcsResourceManager);
    }

    builder.addParameter(
        "sourceConfigURL", getGcsPath("input/shard-config.json", gcsResourceManager));
    builder.addParameter("sourceDbDialect", dialect);
    builder.addParameter("jdbcDriverClassName", jdbcDriver);

    flexTemplateDataflowJobResourceManager = builder.build();
    PipelineLauncher.LaunchInfo jobInfo = flexTemplateDataflowJobResourceManager.launchJob();
    assertThatPipeline(jobInfo).isRunning();
    return jobInfo;
  }

  protected void executeSqlScript(JDBCResourceManager jdbcResourceManager, String ddlResource)
      throws IOException {
    String mysqlSql =
        Resources.toString(Resources.getResource(ddlResource), StandardCharsets.UTF_8);
    // Since the DDL file contains multiple CREATE statements, we split them by semicolon and
    // execute one single SQL statement at a time.
    for (String stmt : mysqlSql.split("(?m);\\s*$")) {
      if (!stmt.trim().isEmpty()) {
        jdbcResourceManager.runSQLUpdate(stmt);
      }
    }
  }
}
