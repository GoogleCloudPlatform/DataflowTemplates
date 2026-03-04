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

import static com.google.cloud.teleport.v2.spanner.migrations.constants.Constants.MYSQL_SOURCE_TYPE;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.teleport.v2.spanner.migrations.shard.Shard;
import com.google.cloud.teleport.v2.spanner.migrations.transformation.CustomTransformation;
import com.google.common.io.Resources;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.pubsub.v1.SubscriptionName;
import com.google.pubsub.v1.TopicName;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.beam.it.cassandra.CassandraResourceManager;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.utils.IORedirectUtil;
import org.apache.beam.it.common.utils.PipelineUtils;
import org.apache.beam.it.gcp.TemplateTestBase;
import org.apache.beam.it.gcp.artifacts.utils.ArtifactUtils;
import org.apache.beam.it.gcp.pubsub.PubsubResourceManager;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.gcp.storage.GcsResourceManager;
import org.apache.beam.it.jdbc.JDBCResourceManager;
import org.apache.beam.it.jdbc.MySQLResourceManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class SpannerToSourceDbITBase extends TemplateTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(SpannerToSourceDbITBase.class);

  protected SpannerResourceManager setUpSpannerResourceManager() {
    return SpannerResourceManager.builder("rr-main-" + testName, PROJECT, REGION)
        .maybeUseStaticInstance()
        .build();
  }

  protected SpannerResourceManager setUpPGDialectSpannerResourceManager() {
    return SpannerResourceManager.builder(
            "rr-main-" + testName, PROJECT, REGION, Dialect.POSTGRESQL)
        .maybeUseStaticInstance()
        .build();
  }

  protected void createSpannerDDL(
      SpannerResourceManager spannerResourceManager, String spannerSchemaFile) throws IOException {
    String ddl =
        String.join(
            "\n",
            Resources.readLines(Resources.getResource(spannerSchemaFile), StandardCharsets.UTF_8));
    if (ddl.isBlank()) {
      throw new IllegalStateException("DDL file is empty: " + spannerSchemaFile);
    }
    ddl = ddl.trim();
    List<String> ddls = Arrays.stream(ddl.split(";")).toList();
    spannerResourceManager.executeDdlStatements(ddls);
  }

  protected SpannerResourceManager createSpannerDatabase(String spannerSchemaFile)
      throws IOException {

    SpannerResourceManager spannerResourceManager = setUpSpannerResourceManager();
    createSpannerDDL(spannerResourceManager, spannerSchemaFile);
    return spannerResourceManager;
  }

  protected SpannerResourceManager createSpannerMetadataDatabase() throws IOException {
    SpannerResourceManager spannerMetadataResourceManager =
        SpannerResourceManager.builder("rr-meta-" + testName, PROJECT, REGION)
            .maybeUseStaticInstance()
            .build();
    String dummy = "CREATE TABLE IF NOT EXISTS t1(id INT64 ) primary key(id)";
    spannerMetadataResourceManager.executeDdlStatement(dummy);
    return spannerMetadataResourceManager;
  }

  protected SpannerResourceManager createPGDialectSpannerMetadataDatabase() {
    SpannerResourceManager spannerMetadataResourceManager =
        SpannerResourceManager.builder("rr-meta-" + testName, PROJECT, REGION, Dialect.POSTGRESQL)
            .maybeUseStaticInstance()
            .build();
    spannerMetadataResourceManager.ensureUsableAndCreateResources();
    return spannerMetadataResourceManager;
  }

  public PubsubResourceManager setUpPubSubResourceManager() throws IOException {
    return PubsubResourceManager.builder(testName, PROJECT, credentialsProvider).build();
  }

  public SubscriptionName createPubsubResources(
      String identifierSuffix,
      PubsubResourceManager pubsubResourceManager,
      String gcsPrefix,
      GcsResourceManager gcsResourceManager) {
    String topicNameSuffix = "rr-it" + identifierSuffix;
    String subscriptionNameSuffix = "rr-it-sub" + identifierSuffix;
    TopicName topic = pubsubResourceManager.createTopic(topicNameSuffix);
    SubscriptionName subscription =
        pubsubResourceManager.createSubscription(topic, subscriptionNameSuffix);
    String prefix = gcsPrefix;
    if (prefix.startsWith("/")) {
      prefix = prefix.substring(1);
    }
    prefix += "/retry/";
    gcsResourceManager.createNotification(topic.toString(), prefix);
    return subscription;
  }

  protected void createAndUploadShardConfigToGcs(
      GcsResourceManager gcsResourceManager, MySQLResourceManager jdbcResourceManager)
      throws IOException {
    Shard shard = new Shard();
    shard.setLogicalShardId("Shard1");
    shard.setUser(jdbcResourceManager.getUsername());
    shard.setHost(jdbcResourceManager.getHost());
    shard.setPassword(jdbcResourceManager.getPassword());
    shard.setPort(String.valueOf(jdbcResourceManager.getPort()));
    shard.setDbName(jdbcResourceManager.getDatabaseName());
    JsonObject jsObj = new Gson().toJsonTree(shard).getAsJsonObject();
    jsObj.remove("secretManagerUri"); // remove field secretManagerUri
    JsonArray ja = new JsonArray();
    ja.add(jsObj);
    String shardFileContents = ja.toString();
    LOG.info("Shard file contents: {}", shardFileContents);
    gcsResourceManager.createArtifact("input/shard.json", shardFileContents);
  }

  protected CassandraResourceManager generateKeyspaceAndBuildCassandraResource() {
    /* The default is Cassandra 4.1 image. TODO: Explore testing with non 4.1 tags. */

    /* Max Cassandra Keyspace is 48 characters. Base Resource Manager adds 24 characters of date-time at the end.
     * That's why we need to take a smaller subsequence of the testId.
     */
    String uniqueId = testId.substring(0, Math.min(20, testId.length()));
    return CassandraResourceManager.builder(uniqueId).build();
  }

  protected void createCassandraSchema(
      CassandraResourceManager cassandraResourceManager, String cassandraSchemaFile)
      throws IOException {
    String ddl =
        String.join(
            " ",
            Resources.readLines(
                Resources.getResource(cassandraSchemaFile), StandardCharsets.UTF_8));
    ddl = ddl.trim();
    String[] ddls = ddl.split(";");
    for (String d : ddls) {
      if (!d.isBlank()) {
        cassandraResourceManager.executeStatement(d);
      }
    }
  }

  public void createAndUploadCassandraConfigToGcs(
      GcsResourceManager gcsResourceManager,
      CassandraResourceManager cassandraResourceManagers,
      String cassandraConfigFile)
      throws IOException {

    String host = cassandraResourceManagers.getHost();
    int port = cassandraResourceManagers.getPort();
    String keyspaceName = cassandraResourceManagers.getKeyspaceName();
    String cassandraConfigContents;
    try (InputStream inputStream =
        Thread.currentThread().getContextClassLoader().getResourceAsStream(cassandraConfigFile)) {
      if (inputStream == null) {
        throw new FileNotFoundException("Resource file not found: " + cassandraConfigFile);
      }
      cassandraConfigContents = new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
    }

    cassandraConfigContents =
        cassandraConfigContents
            .replace("##host##", host)
            .replace("##port##", Integer.toString(port))
            .replace("##keyspace##", keyspaceName);

    gcsResourceManager.createArtifact("input/cassandra-config.conf", cassandraConfigContents);
  }

  public PipelineLauncher.LaunchInfo launchDataflowJob(
      GcsResourceManager gcsResourceManager,
      SpannerResourceManager spannerResourceManager,
      SpannerResourceManager spannerMetadataResourceManager,
      String subscriptionName,
      String identifierSuffix,
      String shardingCustomJarPath,
      String shardingCustomClassName,
      String sourceDbTimezoneOffset,
      CustomTransformation customTransformation,
      String sourceType,
      Map<String, String> jobParameters)
      throws IOException {

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
                getGcsPath(
                    !Objects.equals(sourceType, MYSQL_SOURCE_TYPE)
                        ? "input/cassandra-config.conf"
                        : "input/shard.json",
                    gcsResourceManager));
            put("changeStreamName", "allstream");
            put("dlqGcsPubSubSubscription", subscriptionName);
            put("deadLetterQueueDirectory", getGcsPath("dlq", gcsResourceManager));
            put("maxShardConnections", "5");
            put("maxNumWorkers", "1");
            put("numWorkers", "1");
            put("sourceType", sourceType);
            put("workerMachineType", "n2-standard-4");
          }
        };
    if (jobParameters != null) {
      params.putAll(jobParameters);
    }
    if (shardingCustomJarPath != null) {
      params.put(
          "shardingCustomJarPath",
          getGcsFullPath(gcsResourceManager, shardingCustomJarPath, identifierSuffix));
    }
    if (shardingCustomClassName != null) {
      params.put("shardingCustomClassName", shardingCustomClassName);
    }

    if (sourceDbTimezoneOffset != null) {
      params.put("sourceDbTimezoneOffset", sourceDbTimezoneOffset);
    }

    if (customTransformation != null) {
      params.put(
          "transformationJarPath", getGcsPath(customTransformation.jarPath(), gcsResourceManager));
      params.put("transformationClassName", customTransformation.classPath());
    }

    // Construct template
    String jobName = PipelineUtils.createJobName("rrev-it" + testName);
    // /-DunifiedWorker=true when using runner v2
    PipelineLauncher.LaunchConfig.Builder options =
        PipelineLauncher.LaunchConfig.builder(jobName, specPath);
    options.setParameters(params);
    options.addEnvironment("additionalExperiments", Collections.singletonList("use_runner_v2"));
    options.addEnvironment("ipConfiguration", "WORKER_IP_PRIVATE");
    // Run
    PipelineLauncher.LaunchInfo jobInfo = launchTemplate(options);
    assertThatPipeline(jobInfo).isRunning();
    return jobInfo;
  }

  protected void createMySQLSchema(MySQLResourceManager jdbcResourceManager, String mySqlSchemaFile)
      throws IOException {
    HashMap<String, String> columns = new HashMap<>();
    columns.put("id", "INT NOT NULL");
    columns.put("name", "VARCHAR(25)");
    JDBCResourceManager.JDBCSchema schema = new JDBCResourceManager.JDBCSchema(columns, "id");

    jdbcResourceManager.createTable("test", schema);

    String ddl =
        String.join(
            " ",
            Resources.readLines(Resources.getResource(mySqlSchemaFile), StandardCharsets.UTF_8));
    ddl = ddl.trim();
    String[] ddls = ddl.split(";");
    for (String d : ddls) {
      if (!d.isBlank()) {
        jdbcResourceManager.runSQLUpdate(d);
      }
    }
  }

  public String getGcsFullPath(
      GcsResourceManager gcsResourceManager, String artifactId, String identifierSuffix) {
    return ArtifactUtils.getFullGcsPath(
        gcsResourceManager.getBucket(), identifierSuffix, gcsResourceManager.runId(), artifactId);
  }

  protected void createAndUploadJarToGcs(GcsResourceManager gcsResourceManager)
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

  protected SpannerResourceManager createSpannerDBAndTableWithNColumns(
      String tableName, int n, String stringSize) throws Exception {
    // Validate the table name
    if (tableName == null || tableName.isBlank()) {
      throw new IllegalArgumentException("Table name must be specified and non-blank");
    }

    if (n < 1) {
      throw new IllegalArgumentException("Number of columns must be at least 1");
    }

    if (stringSize == null || stringSize.isBlank()) {
      throw new IllegalArgumentException("String size must be specified and non-blank");
    }

    SpannerResourceManager spannerResourceManager =
        SpannerResourceManager.builder("rr-main-table-per-columns-" + testName, PROJECT, REGION)
            .maybeUseStaticInstance()
            .build();

    StringBuilder ddlBuilder = new StringBuilder();
    ddlBuilder.append("CREATE TABLE ").append(tableName).append(" (\n");
    ddlBuilder.append("    id STRING(100) NOT NULL,\n");
    for (int i = 1; i <= n; i++) {
      ddlBuilder.append("    col_").append(i).append(" STRING(").append(stringSize).append("),\n");
    }

    ddlBuilder.setLength(ddlBuilder.length() - 2);
    ddlBuilder.append("\n) PRIMARY KEY (id)");

    String ddl = ddlBuilder.toString().trim();
    if (ddl.isBlank()) {
      throw new IllegalStateException("DDL generation failed for column count: " + n);
    }

    try {
      spannerResourceManager.executeDdlStatement(ddl);
    } catch (Exception e) {
      throw new RuntimeException("Error executing DDL statement: " + ddl, e);
    }

    String ddlStream =
        "CREATE CHANGE STREAM allstream FOR ALL OPTIONS (value_capture_type = 'NEW_ROW', retention_period = '7d', allow_txn_exclusion = true)";
    try {
      spannerResourceManager.executeDdlStatement(ddlStream);
    } catch (Exception e) {
      throw new RuntimeException("Error executing CREATE CHANGE STREAM statement: " + ddlStream, e);
    }

    return spannerResourceManager;
  }

  protected void createCassandraTableWithNColumns(
      CassandraResourceManager cassandraResourceManager, String tableName, int n) throws Exception {

    if (tableName == null || tableName.isBlank()) {
      throw new IllegalArgumentException("Table name must be specified and non-blank");
    }
    if (n < 1) {
      throw new IllegalArgumentException("Number of columns must be at least 1");
    }

    StringBuilder ddlBuilder = new StringBuilder();
    ddlBuilder
        .append("CREATE TABLE IF NOT EXISTS ")
        .append(cassandraResourceManager.getKeyspaceName())
        .append(".")
        .append(tableName)
        .append(" (\n");
    ddlBuilder.append("    id TEXT PRIMARY KEY,\n");
    for (int i = 1; i <= n; i++) {
      ddlBuilder.append("    col_").append(i).append(" TEXT,\n");
    }

    ddlBuilder.setLength(ddlBuilder.length() - 2);
    ddlBuilder.append("\n);");

    String ddl = ddlBuilder.toString().trim();
    if (ddl.isBlank()) {
      throw new IllegalStateException("DDL generation failed for column count: " + n);
    }

    try {
      cassandraResourceManager.executeStatement(ddl);
    } catch (Exception e) {
      throw new RuntimeException("Error executing DDL statement: " + ddl, e);
    }
  }

  protected void createMySQLTableWithNColumns(
      MySQLResourceManager jdbcResourceManager, String tableName, int n, String stringSize)
      throws Exception {
    // Validate the table name
    if (tableName == null || tableName.isBlank()) {
      throw new IllegalArgumentException("Table name must be specified and non-blank");
    }

    if (n < 1) {
      throw new IllegalArgumentException("Number of columns must be at least 1");
    }

    if (stringSize == null || stringSize.isBlank()) {
      throw new IllegalArgumentException("String size must be specified and non-blank");
    }

    StringBuilder ddlBuilder = new StringBuilder();
    ddlBuilder.append("CREATE TABLE ").append(tableName).append(" (\n");
    ddlBuilder.append("    id VARCHAR(20) NOT NULL PRIMARY KEY,\n");

    for (int i = 1; i <= n; i++) {
      ddlBuilder.append("    col_").append(i).append(" VARCHAR(").append(stringSize).append("),\n");
    }

    ddlBuilder.setLength(ddlBuilder.length() - 2);
    ddlBuilder.append("\n);");

    String ddl = ddlBuilder.toString().trim();
    if (ddl.isBlank()) {
      throw new IllegalStateException("DDL generation failed for column count: " + n);
    }

    try {
      jdbcResourceManager.runSQLUpdate(ddl);
    } catch (Exception e) {
      throw new RuntimeException("Error executing DDL statement: " + ddl, e);
    }
  }
}
