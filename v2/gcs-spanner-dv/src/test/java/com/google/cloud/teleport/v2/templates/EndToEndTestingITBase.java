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
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.gcp.cloudsql.CloudSqlResourceManager;
import org.apache.beam.it.gcp.dataflow.FlexTemplateDataflowJobResourceManager;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.gcp.storage.GcsResourceManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class EndToEndTestingITBase extends GCSSpannerDVITBase {

  private static final Logger LOG = LoggerFactory.getLogger(EndToEndTestingITBase.class);
  protected FlexTemplateDataflowJobResourceManager flexTemplateDataflowJobResourceManager;

  protected PipelineLauncher.LaunchInfo launchBulkDataflowJob(
      String jobName,
      SpannerResourceManager spannerResourceManager,
      GcsResourceManager gcsResourceManager,
      CloudSqlResourceManager cloudSqlResourceManager,
      String sessionFilePath,
      Boolean multiSharded)
      throws IOException {
    // launch dataflow template
    FlexTemplateDataflowJobResourceManager.Builder builder =
        FlexTemplateDataflowJobResourceManager.builder(jobName)
            .withTemplateName("Sourcedb_to_Spanner_Flex")
            .withTemplateModulePath("v2/sourcedb-to-spanner")
            .addParameter("instanceId", spannerResourceManager.getInstanceId())
            .addParameter("databaseId", spannerResourceManager.getDatabaseId())
            .addParameter("projectId", PROJECT)
            .addParameter("outputDirectory", "gs://" + artifactBucketName)
            .addParameter("gcsOutputDirectory", "gs://" + artifactBucketName)
            .addEnvironmentVariable("workerMachineType", "n2-standard-4")
            .addEnvironmentVariable(
                "additionalExperiments", Collections.singletonList("disable_runner_v2"));

    if (sessionFilePath != null) {
      builder.addParameter("sessionFilePath", sessionFilePath);
    }

    if (multiSharded) {
      builder.addParameter(
          "sourceConfigURL", getGcsPath("input/shard-bulk.json", gcsResourceManager));
    } else {
      builder.addParameter(
          "sourceConfigURL",
          cloudSqlResourceManager.getUri() + "?useSSL=false&allowPublicKeyRetrieval=true");
      builder.addParameter("username", cloudSqlResourceManager.getUsername());
      builder.addParameter("password", cloudSqlResourceManager.getPassword());
      builder.addParameter("jdbcDriverClassName", "com.mysql.jdbc.Driver");
    }

    flexTemplateDataflowJobResourceManager = builder.build();

    // Run
    PipelineLauncher.LaunchInfo jobInfo = flexTemplateDataflowJobResourceManager.launchJob();
    assertThatPipeline(jobInfo).isRunning();
    return jobInfo;
  }

  protected void createMySQLDDL(CloudSqlResourceManager cloudSqlResourceManager, String ddlResource)
      throws IOException {
    String mysqlSql =
        Resources.toString(Resources.getResource(ddlResource), StandardCharsets.UTF_8);
    // Since the DDL file contains multiple CREATE statements, we split them by semicolon and
    // execute one single SQL statement at a time.
    for (String stmt : mysqlSql.split(";")) {
      if (!stmt.trim().isEmpty()) {
        cloudSqlResourceManager.runSQLUpdate(stmt);
      }
    }
  }
}
