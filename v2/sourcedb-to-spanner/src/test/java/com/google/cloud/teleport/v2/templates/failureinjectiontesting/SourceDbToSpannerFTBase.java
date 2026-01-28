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

import com.google.cloud.teleport.v2.spanner.migrations.transformation.CustomTransformation;
import com.google.pubsub.v1.SubscriptionName;
import com.google.pubsub.v1.TopicName;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.utils.IORedirectUtil;
import org.apache.beam.it.conditions.ConditionCheck;
import org.apache.beam.it.gcp.TemplateTestBase;
import org.apache.beam.it.gcp.cloudsql.CloudSqlResourceManager;
import org.apache.beam.it.gcp.dataflow.FlexTemplateDataflowJobResourceManager;
import org.apache.beam.it.gcp.datastream.conditions.DlqEventsCounter;
import org.apache.beam.it.gcp.pubsub.PubsubResourceManager;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.gcp.storage.GcsResourceManager;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class SourceDbToSpannerFTBase extends TemplateTestBase {

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
    return launchBulkDataflowJob(
        jobName,
        null,
        null,
        spannerResourceManager,
        gcsResourceManager,
        cloudSqlResourceManager,
        null);
  }

  protected PipelineLauncher.LaunchInfo launchBulkDataflowJob(
      String jobName,
      String additionalMavenProfile,
      Map<String, String> additionalParams,
      SpannerResourceManager spannerResourceManager,
      GcsResourceManager gcsResourceManager,
      CloudSqlResourceManager cloudSqlResourceManager,
      CustomTransformation customTransformation)
      throws IOException {
    // launch dataflow template
    FlexTemplateDataflowJobResourceManager.Builder flexTemplateBuilder =
        FlexTemplateDataflowJobResourceManager.builder(jobName)
            .withTemplateName("Sourcedb_to_Spanner_Flex")
            .withTemplateModulePath("v2/sourcedb-to-spanner")
            .addParameter("instanceId", spannerResourceManager.getInstanceId())
            .addParameter("databaseId", spannerResourceManager.getDatabaseId())
            .addParameter("projectId", PROJECT)
            .addParameter("outputDirectory", getGcsPath("output", gcsResourceManager))
            .addParameter("sourceConfigURL", cloudSqlResourceManager.getUri())
            .addParameter("username", cloudSqlResourceManager.getUsername())
            .addParameter("password", cloudSqlResourceManager.getPassword())
            .addParameter("jdbcDriverClassName", "com.mysql.jdbc.Driver")
            .addParameter("workerMachineType", "n2-standard-4")
            .addEnvironmentVariable(
                "additionalExperiments", Collections.singletonList("disable_runner_v2"));

    if (additionalMavenProfile != null && !additionalMavenProfile.isBlank()) {
      flexTemplateBuilder.withAdditionalMavenProfile(additionalMavenProfile);
    }

    if (customTransformation != null) {
      flexTemplateBuilder.addParameter(
          "transformationJarPath",
          getGcsPath(
              "CustomTransformationAllTypes/" + customTransformation.jarPath(),
              gcsResourceManager));
      flexTemplateBuilder.addParameter("transformationClassName", customTransformation.classPath());
      if (customTransformation.customParameters() != null) {
        flexTemplateBuilder.addParameter(
            "transformationCustomParameters", customTransformation.customParameters());
      }
    }

    if (additionalParams != null) {
      for (Entry<String, String> param : additionalParams.entrySet()) {
        flexTemplateBuilder.addParameter(param.getKey(), param.getValue());
      }
    }

    // Run
    PipelineLauncher.LaunchInfo jobInfo = flexTemplateBuilder.build().launchJob();
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
            .addParameter("outputDirectory", getGcsPath("output", gcsResourceManager))
            .addParameter("sessionFilePath", getGcsPath("input/session.json", gcsResourceManager))
            .addParameter(
                "sourceConfigURL", getGcsPath("input/shard-bulk.json", gcsResourceManager))
            .addEnvironmentVariable(
                "additionalExperiments", Collections.singletonList("disable_runner_v2"))
            .addParameter("workerMachineType", "n2-standard-4")
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

  public PubsubResourceManager setUpPubSubResourceManager() throws IOException {
    return PubsubResourceManager.builder(testName, PROJECT, credentialsProvider).build();
  }

  public SubscriptionName createPubsubResources(
      String identifierSuffix,
      PubsubResourceManager pubsubResourceManager,
      String gcsPrefix,
      GcsResourceManager gcsResourceManager) {
    String topicNameSuffix = "FT-" + identifierSuffix;
    String subscriptionNameSuffix = "FT" + identifierSuffix;
    TopicName topic = pubsubResourceManager.createTopic(topicNameSuffix);
    SubscriptionName subscription =
        pubsubResourceManager.createSubscription(topic, subscriptionNameSuffix);
    String prefix = gcsPrefix;
    if (prefix.startsWith("/")) {
      prefix = prefix.substring(1);
    }
    gcsResourceManager.createNotification(topic.toString(), prefix);
    return subscription;
  }

  public PipelineLauncher.LaunchInfo launchFwdDataflowJobInRetryDlqMode(
      SpannerResourceManager spannerResourceManager,
      String inputLocationFullPath,
      String dlqLocationFullPath,
      GcsResourceManager gcsResourceManager,
      CustomTransformation customTransformation)
      throws IOException {

    // launch dataflow template
    FlexTemplateDataflowJobResourceManager.Builder flexTemplateBuilder =
        FlexTemplateDataflowJobResourceManager.builder(testName)
            .withTemplateName("Cloud_Datastream_to_Spanner")
            .withTemplateModulePath("v2/datastream-to-spanner")
            .addParameter("inputFilePattern", inputLocationFullPath)
            .addParameter(
                "streamName", "projects/testProject/locations/us-central1/streams/testStream")
            .addParameter("instanceId", spannerResourceManager.getInstanceId())
            .addParameter("databaseId", spannerResourceManager.getDatabaseId())
            .addParameter("projectId", PROJECT)
            .addParameter("deadLetterQueueDirectory", dlqLocationFullPath)
            .addParameter("datastreamSourceType", "mysql")
            .addParameter("inputFileFormat", "avro")
            .addParameter("runMode", "retryDLQ")
            .addParameter("dlqRetryMinutes", "1")
            .addParameter("workerMachineType", "n2-standard-4");

    if (customTransformation != null) {
      flexTemplateBuilder.addParameter(
          "transformationJarPath",
          getGcsPath(
              "CustomTransformationAllTypes/" + customTransformation.jarPath(),
              gcsResourceManager));
      flexTemplateBuilder.addParameter("transformationClassName", customTransformation.classPath());
      if (customTransformation.customParameters() != null) {
        flexTemplateBuilder.addParameter(
            "transformationCustomParameters", customTransformation.customParameters());
      }
    }

    // Run
    PipelineLauncher.LaunchInfo jobInfo = flexTemplateBuilder.build().launchJob();
    return jobInfo;
  }

  public void createAndUploadJarToGcs(String gcsPathPrefix, GcsResourceManager gcsResourceManager)
      throws IOException, InterruptedException {
    String[] shellCommand = {"/bin/bash", "-c", "cd ../spanner-custom-shard"};

    Process exec = Runtime.getRuntime().exec(shellCommand);

    IORedirectUtil.redirectLinesLog(exec.getInputStream(), LOG);
    IORedirectUtil.redirectLinesLog(exec.getErrorStream(), LOG);

    if (exec.waitFor() != 0) {
      throw new RuntimeException("Error staging template, check Maven logs.");
    }
    gcsResourceManager.uploadArtifact(
        gcsPathPrefix + "/customTransformation.jar",
        "../spanner-custom-shard/target/spanner-custom-shard-1.0-SNAPSHOT.jar");
  }

  protected void loadSQLFileResource(
      org.apache.beam.it.jdbc.JDBCResourceManager jdbcResourceManager, String resourcePath)
      throws Exception {
    String sql =
        String.join(
            " ",
            com.google.common.io.Resources.readLines(
                com.google.common.io.Resources.getResource(resourcePath),
                java.nio.charset.StandardCharsets.UTF_8));
    loadSQLToJdbcResourceManager(jdbcResourceManager, sql);
  }

  protected void loadSQLToJdbcResourceManager(
      org.apache.beam.it.jdbc.JDBCResourceManager jdbcResourceManager, String sql)
      throws Exception {
    LOG.info("Loading sql to jdbc resource manager with uri: {}", jdbcResourceManager.getUri());
    try {
      java.sql.Connection connection =
          java.sql.DriverManager.getConnection(
              jdbcResourceManager.getUri(),
              jdbcResourceManager.getUsername(),
              jdbcResourceManager.getPassword());

      // Preprocess SQL to handle multi-line statements and newlines
      sql = sql.replaceAll("\r\n", " ").replaceAll("\n", " ");

      // Split into individual statements
      String[] statements = sql.split(";");

      // Execute each statement
      java.sql.Statement statement = connection.createStatement();
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

  public static class TotalEventsProcessedCheck extends ConditionCheck {

    private Long minTotalEventsExpected;
    private SpannerResourceManager spannerResourceManager;
    private List<String> tables;
    private GcsResourceManager gcsResourceManager;
    private String gcsPathPrefix;

    @Override
    public String getDescription() {
      return String.format(
          "Total Events processed check if rows in Spanner + number of errors = %d",
          minTotalEventsExpected);
    }

    @Override
    public CheckResult check() {
      Long totalRowsInSpanner = 0L;
      Long totalErrors = 0L;
      try {
        for (String table : tables) {
          totalRowsInSpanner += spannerResourceManager.getRowCount(table);
        }
        totalErrors = DlqEventsCounter.calculateTotalEvents(gcsResourceManager, gcsPathPrefix);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      if ((totalRowsInSpanner + totalErrors) < minTotalEventsExpected) {
        return new CheckResult(
            false,
            String.format(
                "Expected at least %d events processed but has only %d in Spanner + %d errors",
                minTotalEventsExpected, totalRowsInSpanner, totalErrors));
      }
      return new CheckResult(
          true,
          String.format(
              "Expected at least %d events processed and found %d rows in spanner + %d errors",
              minTotalEventsExpected, totalRowsInSpanner, totalErrors));
    }

    public TotalEventsProcessedCheck(
        SpannerResourceManager spannerResourceManager,
        List<String> tables,
        GcsResourceManager gcsResourceManager,
        String gcsPathPrefix,
        long minTotalEventsExpected) {
      this.spannerResourceManager = spannerResourceManager;
      this.tables = tables;
      this.gcsResourceManager = gcsResourceManager;
      this.gcsPathPrefix = gcsPathPrefix;
      this.minTotalEventsExpected = minTotalEventsExpected;
    }
  }
}
