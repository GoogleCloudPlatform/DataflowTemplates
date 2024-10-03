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

import com.google.cloud.teleport.v2.source.reader.io.jdbc.iowrapper.config.SQLDialect;
import com.google.cloud.teleport.v2.spanner.migrations.transformation.CustomTransformation;
import com.google.common.io.Resources;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.utils.IORedirectUtil;
import org.apache.beam.it.common.utils.PipelineUtils;
import org.apache.beam.it.gcp.JDBCBaseIT;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.jdbc.JDBCResourceManager;
import org.apache.beam.it.jdbc.MySQLResourceManager;
import org.apache.beam.it.jdbc.PostgresResourceManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for SourceDbToSpanner integration tests. It provides helper functions related to
 * environment setup and assertConditions.
 */
public class SourceDbToSpannerITBase extends JDBCBaseIT {
  private static final Logger LOG = LoggerFactory.getLogger(SourceDbToSpannerITBase.class);

  public MySQLResourceManager setUpMySQLResourceManager() {
    return MySQLResourceManager.builder(testName).build();
  }

  public PostgresResourceManager setUpPostgreSQLResourceManager() {
    return PostgresResourceManager.builder(testName).build();
  }

  public SpannerResourceManager setUpSpannerResourceManager() {
    return SpannerResourceManager.builder(testName, PROJECT, REGION)
        .maybeUseStaticInstance()
        .build();
  }

  protected void loadSQLFileResource(JDBCResourceManager jdbcResourceManager, String resourcePath)
      throws Exception {
    String sql =
        String.join(
            " ", Resources.readLines(Resources.getResource(resourcePath), StandardCharsets.UTF_8));
    loadSQLToJdbcResourceManager(jdbcResourceManager, sql);
  }

  protected void loadSQLToJdbcResourceManager(JDBCResourceManager jdbcResourceManager, String sql)
      throws Exception {
    LOG.info("Loading sql to jdbc resource manager with uri: {}", jdbcResourceManager.getUri());
    try {
      Connection connection =
          DriverManager.getConnection(
              jdbcResourceManager.getUri(),
              jdbcResourceManager.getUsername(),
              jdbcResourceManager.getPassword());

      // Preprocess SQL to handle multi-line statements and newlines
      sql = sql.replaceAll("\r\n", " ").replaceAll("\n", " ");

      // Split into individual statements
      String[] statements = sql.split(";");

      // Execute each statement
      Statement statement = connection.createStatement();
      for (String stmt : statements) {
        if (!stmt.trim().isEmpty()) {
          // Skip SELECT statements
          if (!stmt.trim().toUpperCase().startsWith("SELECT")) {
            LOG.info("Executing statement: {}", stmt);
            statement.executeUpdate(stmt);
          }
        }
      }
    } catch (Exception e) {
      LOG.info("failed to load SQL into database: {}", sql);
      throw new Exception("Failed to load SQL into database", e);
    }
    LOG.info("Successfully loaded sql to jdbc resource manager");
  }

  /**
   * Helper function for creating Spanner DDL. Reads the sql file from resources directory and
   * applies the DDL to Spanner instance.
   *
   * @param spannerResourceManager Initialized SpannerResourceManager instance
   * @param resourceName SQL file name with path relative to resources directory
   */
  public void createSpannerDDL(SpannerResourceManager spannerResourceManager, String resourceName)
      throws IOException {
    LOG.info("Creating spanner DDL");
    String ddl =
        String.join(
            " ", Resources.readLines(Resources.getResource(resourceName), StandardCharsets.UTF_8));
    ddl = ddl.trim();
    List<String> ddls =
        Arrays.stream(ddl.split(";")).filter(d -> !d.isBlank()).collect(Collectors.toList());
    spannerResourceManager.executeDdlStatements(ddls);
    LOG.info("Successfully created spanner DDL");
  }

  /**
   * Performs the following steps: Uploads session file to GCS. Creates Pubsub resources. Launches
   * DataStreamToSpanner dataflow job.
   *
   * @param identifierSuffix will be used as postfix in generated resource ids
   * @param sessionFileResourceName Session file name with path relative to resources directory
   * @param gcsPathPrefix Prefix directory name for this DF job. Data and DLQ directories will be
   *     created under this prefix.
   * @return dataflow jobInfo object
   * @throws IOException
   */
  protected PipelineLauncher.LaunchInfo launchDataflowJob(
      String identifierSuffix,
      String sessionFileResourceName,
      String gcsPathPrefix,
      JDBCResourceManager jdbcResourceManager,
      SpannerResourceManager spannerResourceManager,
      Map<String, String> jobParameters,
      CustomTransformation customTransformation)
      throws IOException {

    Map<String, String> params =
        new HashMap<>() {
          {
            put("projectId", PROJECT);
            put("instanceId", spannerResourceManager.getInstanceId());
            put("databaseId", spannerResourceManager.getDatabaseId());
            put("sourceDbDialect", sqlDialectFrom(jdbcResourceManager));
            put("sourceConfigURL", jdbcResourceManager.getUri());
            put("username", jdbcResourceManager.getUsername());
            put("password", jdbcResourceManager.getPassword());
            put("outputDirectory", "gs://" + artifactBucketName);
            put("jdbcDriverClassName", driverClassNameFrom(jdbcResourceManager));
          }
        };

    if (sessionFileResourceName != null) {
      String sessionPath = gcsPathPrefix + "/session.json";
      LOG.info("uploading session file to: {}", sessionPath);
      gcsClient.uploadArtifact(
          sessionPath, Resources.getResource(sessionFileResourceName).getPath());
      params.put("sessionFilePath", getGcsPath(sessionPath));
    }

    if (customTransformation != null) {
      params.put(
          "transformationJarPath",
          getGcsPath(gcsPathPrefix + "/" + customTransformation.jarPath()));
      params.put("transformationClassName", customTransformation.classPath());
    }

    // overridden parameters
    if (jobParameters != null) {
      for (Map.Entry<String, String> entry : jobParameters.entrySet()) {
        params.put(entry.getKey(), entry.getValue());
      }
    }

    // Construct template
    String jobName = PipelineUtils.createJobName(identifierSuffix);
    PipelineLauncher.LaunchConfig.Builder options =
        PipelineLauncher.LaunchConfig.builder(jobName, specPath);

    options.setParameters(params);
    options.addEnvironment("additionalExperiments", List.of("disable_runner_v2"));
    if (System.getProperty("numWorkers") != null) {
      options.addEnvironment("numWorkers", Integer.parseInt(System.getProperty("numWorkers")));
    } else {
      options.addEnvironment("numWorkers", 2);
    }
    // Run
    PipelineLauncher.LaunchInfo jobInfo = launchTemplate(options, false);
    assertThatPipeline(jobInfo).isRunning();

    return jobInfo;
  }

  public void createAndUploadJarToGcs(String gcsPathPrefix)
      throws IOException, InterruptedException {
    String[] shellCommand = {"/bin/bash", "-c", "cd ../spanner-custom-shard"};

    Process exec = Runtime.getRuntime().exec(shellCommand);

    IORedirectUtil.redirectLinesLog(exec.getInputStream(), LOG);
    IORedirectUtil.redirectLinesLog(exec.getErrorStream(), LOG);

    if (exec.waitFor() != 0) {
      throw new RuntimeException("Error staging template, check Maven logs.");
    }
    gcsClient.uploadArtifact(
        gcsPathPrefix + "/customTransformation.jar",
        "../spanner-custom-shard/target/spanner-custom-shard-1.0-SNAPSHOT.jar");
  }

  private String sqlDialectFrom(JDBCResourceManager jdbcResourceManager) {
    if (jdbcResourceManager instanceof PostgresResourceManager) {
      return SQLDialect.POSTGRESQL.name();
    }
    return SQLDialect.MYSQL.name();
  }

  private String driverClassNameFrom(JDBCResourceManager jdbcResourceManager) {
    try {
      if (jdbcResourceManager instanceof PostgresResourceManager) {
        return Class.forName("org.postgresql.Driver").getCanonicalName();
      }
      return Class.forName("com.mysql.jdbc.Driver").getCanonicalName();
    } catch (ClassNotFoundException e) {
      throw new IllegalArgumentException(e);
    }
  }
}
