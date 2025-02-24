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

import com.google.cloud.teleport.v2.spanner.migrations.shard.Shard;
import com.google.cloud.teleport.v2.spanner.migrations.transformation.CustomTransformation;
import com.google.common.io.Resources;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.pubsub.v1.SubscriptionName;
import com.google.pubsub.v1.TopicName;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.TestProperties;
import org.apache.beam.it.common.utils.IORedirectUtil;
import org.apache.beam.it.common.utils.PipelineUtils;
import org.apache.beam.it.gcp.TemplateTestBase;
import org.apache.beam.it.gcp.artifacts.utils.ArtifactUtils;
import org.apache.beam.it.gcp.dataflow.FlexTemplateDataflowJobResourceManager;
import org.apache.beam.it.gcp.pubsub.PubsubResourceManager;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.gcp.storage.GcsResourceManager;
import org.apache.beam.it.jdbc.JDBCResourceManager;
import org.apache.beam.it.jdbc.MySQLResourceManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class SpannerToSourceDbITBase extends TemplateTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(SpannerToSourceDbITBase.class);
  private static FlexTemplateDataflowJobResourceManager flexTemplateDataflowJobResourceManager;

  protected SpannerResourceManager createSpannerDatabase(String spannerSchemaFile)
      throws IOException {
    SpannerResourceManager spannerResourceManager =
        SpannerResourceManager.builder("rr-main-" + testName, PROJECT, REGION)
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

    String[] ddls = ddl.trim().split(";");
    for (String d : ddls) {
      d = d.trim();
      if (!d.isEmpty()) {
        spannerResourceManager.executeDdlStatement(d);
      }
    }
    return spannerResourceManager;
  }

  protected SpannerResourceManager createSpannerMetadataDatabase() throws IOException {
    SpannerResourceManager spannerMetadataResourceManager =
        SpannerResourceManager.builder("rr-meta-" + testName, PROJECT, REGION)
            .maybeUseStaticInstance()
            .build();
    String dummy = "create table t1(id INT64 ) primary key(id)";
    spannerMetadataResourceManager.executeDdlStatement(dummy);
    return spannerMetadataResourceManager;
  }

  public PubsubResourceManager setUpPubSubResourceManager() throws IOException {
    return PubsubResourceManager.builder(testName, PROJECT, credentialsProvider).build();
  }

  public SubscriptionName createPubsubResources(
      String identifierSuffix, PubsubResourceManager pubsubResourceManager, String gcsPrefix) {
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
    gcsClient.createNotification(topic.toString(), prefix);
    return subscription;
  }

  protected void createAndUploadShardConfigToGcs(
      GcsResourceManager gcsResourceManager, MySQLResourceManager jdbcResourceManager)
      throws IOException {
    Shard shard = new Shard();
    shard.setLogicalShardId("Shard1");
    shard.setUser(jdbcResourceManager.getUsername());
    shard.setHost("10.128.0.2");
    shard.setPassword(jdbcResourceManager.getPassword());
    shard.setPort(String.valueOf(jdbcResourceManager.getPort()));
    shard.setDbName(jdbcResourceManager.getDatabaseName());
    JsonObject jsObj = new Gson().toJsonTree(shard).getAsJsonObject();
    jsObj.remove("secretManagerUri"); // remove field secretManagerUri
    JsonArray ja = new JsonArray();
    ja.add(jsObj);
    String shardFileContents = ja.toString();
    System.out.println("#####");
    System.out.println(TestProperties.hostIp());
    LOG.info("Shard file contents: {}", shardFileContents);
    gcsResourceManager.createArtifact("input/shard.json", shardFileContents);
  }

  protected CassandraResourceManager generateKeyspaceAndBuildCassandraResource() {
    return CassandraResourceManager.builder(testName).build();
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
        cassandraResourceManager.execute(d);
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
      String sourceType)
      throws IOException {

    Map<String, String> params =
        new HashMap<>() {
          {
            put("sessionFilePath", getGcsPath("input/session.json", gcsResourceManager));
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
    // Run
    PipelineLauncher.LaunchInfo jobInfo = launchTemplate(options, false);
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
        artifactBucketName, identifierSuffix, gcsResourceManager.runId(), artifactId);
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

  public PipelineLauncher.LaunchInfo launchRRDataflowJob(
      SpannerResourceManager spannerResourceManager,
      GcsResourceManager gcsResourceManager,
      SpannerResourceManager spannerMetadataResourceManager,
      String rrSubscriptionName,
      String sourceType)
      throws IOException {
    String rrJobName = PipelineUtils.createJobName("rrev-it" + testName);
    // default parameters
    flexTemplateDataflowJobResourceManager =
        FlexTemplateDataflowJobResourceManager.builder(rrJobName)
            .withTemplateName("Spanner_to_SourceDb")
            .withTemplateModulePath("v2/spanner-to-sourcedb")
            .addParameter("sessionFilePath", getGcsPath("input/session.json", gcsResourceManager))
            .addParameter("instanceId", spannerResourceManager.getInstanceId())
            .addParameter("databaseId", spannerResourceManager.getDatabaseId())
            .addParameter("spannerProjectId", PROJECT)
            .addParameter("metadataDatabase", spannerMetadataResourceManager.getDatabaseId())
            .addParameter("metadataInstance", spannerMetadataResourceManager.getInstanceId())
            .addParameter(
                "sourceShardsFilePath", getGcsPath("input/shard.json", gcsResourceManager))
            .addParameter("changeStreamName", "allstream")
            .addParameter("dlqGcsPubSubSubscription", rrSubscriptionName)
            .addParameter("deadLetterQueueDirectory", getGcsPath("dlq", gcsResourceManager))
            .addParameter("maxShardConnections", "5")
            .addParameter("maxNumWorkers", "1")
            .addParameter("numWorkers", "1")
            .addParameter("sourceType", sourceType)
            .addEnvironmentVariable(
                "additionalExperiments", Collections.singletonList("use_runner_v2"))
            .build();
    // Run
    PipelineLauncher.LaunchInfo jobInfo = flexTemplateDataflowJobResourceManager.launchJob();
    assertThatPipeline(jobInfo).isRunning();
    System.out.println("#####1");
    System.out.println(jobInfo.jobId());
    return jobInfo;
  }
}
