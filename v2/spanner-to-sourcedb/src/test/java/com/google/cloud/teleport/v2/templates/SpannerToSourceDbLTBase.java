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
import com.google.pubsub.v1.SubscriptionName;
import com.google.pubsub.v1.TopicName;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineLauncher.LaunchConfig;
import org.apache.beam.it.common.PipelineLauncher.LaunchInfo;
import org.apache.beam.it.common.TestProperties;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.TemplateLoadTestBase;
import org.apache.beam.it.gcp.artifacts.utils.ArtifactUtils;
import org.apache.beam.it.gcp.pubsub.PubsubResourceManager;
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
public class SpannerToSourceDbLTBase extends TemplateLoadTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(SpannerToSourceDbLTBase.class);

  private static final String TEMPLATE_SPEC_PATH =
      MoreObjects.firstNonNull(
          TestProperties.specPath(), "gs://dataflow-templates/latest/flex/Spanner_to_SourceDb");
  public SpannerResourceManager spannerResourceManager;
  public SpannerResourceManager spannerMetadataResourceManager;
  public List<JDBCResourceManager> jdbcResourceManagers;
  public GcsResourceManager gcsResourceManager;
  private static PubsubResourceManager pubsubResourceManager;
  private SubscriptionName subscriptionName;

  public void setupResourceManagers(
      String spannerDdlResource, String sessionFileResource, String artifactBucket)
      throws IOException {
    spannerResourceManager = createSpannerDatabase(spannerDdlResource);
    spannerMetadataResourceManager = createSpannerMetadataDatabase();

    gcsResourceManager =
        GcsResourceManager.builder(artifactBucket, getClass().getSimpleName(), CREDENTIALS).build();

    gcsResourceManager.uploadArtifact(
        "input/session.json", Resources.getResource(sessionFileResource).getPath());

    pubsubResourceManager = setUpPubSubResourceManager();
    subscriptionName =
        createPubsubResources(
            getClass().getSimpleName(),
            pubsubResourceManager,
            getGcsPath(artifactBucket, "dlq", gcsResourceManager)
                .replace("gs://" + artifactBucket, ""));
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
        spannerResourceManager,
        spannerMetadataResourceManager,
        gcsResourceManager,
        pubsubResourceManager);
    for (JDBCResourceManager jdbcResourceManager : jdbcResourceManagers) {
      ResourceManagerUtils.cleanResources(jdbcResourceManager);
    }
  }

  public PubsubResourceManager setUpPubSubResourceManager() throws IOException {
    return PubsubResourceManager.builder(testName, project, CREDENTIALS_PROVIDER)
        .setMonitoringClient(monitoringClient)
        .build();
  }

  public SubscriptionName createPubsubResources(
      String identifierSuffix, PubsubResourceManager pubsubResourceManager, String gcsPrefix) {
    String topicNameSuffix = "rr-load" + identifierSuffix;
    String subscriptionNameSuffix = "rr-load-sub" + identifierSuffix;
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

  public SpannerResourceManager createSpannerDatabase(String spannerDdlResourceFile)
      throws IOException {
    SpannerResourceManager spannerResourceManager =
        SpannerResourceManager.builder("rr-loadtest-" + testName, project, region)
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
    String dummy = "create table t1(id INT64 ) primary key(id)";
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

  public PipelineLauncher.LaunchInfo launchDataflowJob(
      String artifactBucket, int numWorkers, int maxWorkers) throws IOException {
    // default parameters

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
            put("dlqGcsPubSubSubscription", subscriptionName.toString());
            put("deadLetterQueueDirectory", getGcsPath(artifactBucket, "dlq", gcsResourceManager));
            put("maxShardConnections", "100");
          }
        };

    LaunchConfig.Builder options =
        LaunchConfig.builder(getClass().getSimpleName(), TEMPLATE_SPEC_PATH);
    options
        .addEnvironment("maxWorkers", maxWorkers)
        .addEnvironment("numWorkers", numWorkers)
        .addEnvironment("additionalExperiments", Collections.singletonList("use_runner_v2"));

    options.setParameters(params);
    PipelineLauncher.LaunchInfo jobInfo = pipelineLauncher.launch(project, region, options.build());
    return jobInfo;
  }

  public String getGcsPath(
      String bucket, String artifactId, GcsResourceManager gcsResourceManager) {
    return ArtifactUtils.getFullGcsPath(
        bucket, getClass().getSimpleName(), gcsResourceManager.runId(), artifactId);
  }

  public Map<String, Double> getCustomCounters(
      LaunchInfo launchInfo, int numShards, Map<String, Double> metrics) throws IOException {
    Double successfulEvents =
        pipelineLauncher.getMetric(project, region, launchInfo.jobId(), "success_record_count");
    metrics.put(
        "Custom_Counter_SuccessRecordCount", successfulEvents != null ? successfulEvents : 0.0);
    Double retryableErrors =
        pipelineLauncher.getMetric(project, region, launchInfo.jobId(), "retryable_record_count");
    metrics.put(
        "Custom_Counter_RetryableRecordCount", retryableErrors != null ? retryableErrors : 0.0);

    Double severeErrorCount =
        pipelineLauncher.getMetric(project, region, launchInfo.jobId(), "severe_error_count");
    metrics.put(
        "Custom_Counter_SevereErrorCount", severeErrorCount != null ? severeErrorCount : 0.0);
    Double skippedRecordCount =
        pipelineLauncher.getMetric(project, region, launchInfo.jobId(), "skipped_record_count");
    metrics.put(
        "Custom_Counter_SkippedRecordCount", skippedRecordCount != null ? skippedRecordCount : 0.0);

    for (int i = 1; i <= numShards; ++i) {
      Double replicationLag =
          pipelineLauncher.getMetric(
              project,
              region,
              launchInfo.jobId(),
              "replication_lag_in_seconds_Shard" + i + "_MEAN");
      metrics.put(
          "Custom_Counter_MeanReplicationLagShard" + i,
          replicationLag != null ? replicationLag : 0.0);
    }
    return metrics;
  }

  public void exportMetrics(PipelineLauncher.LaunchInfo jobInfo, int numShards)
      throws ParseException, IOException, InterruptedException {
    Map<String, Double> metrics = getMetrics(jobInfo);
    getCustomCounters(jobInfo, numShards, metrics);
    getResourceManagerMetrics(metrics);

    // export results
    exportMetricsToBigQuery(jobInfo, metrics);
  }

  public void getResourceManagerMetrics(Map<String, Double> metrics) {
    pubsubResourceManager.collectMetrics(metrics);
  }
}
