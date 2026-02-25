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

import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;

import com.google.cloud.teleport.v2.spanner.migrations.shard.Shard;
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
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.gcp.TemplateTestBase;
import org.apache.beam.it.gcp.cloudsql.CloudSqlResourceManager;
import org.apache.beam.it.gcp.dataflow.FlexTemplateDataflowJobResourceManager;
import org.apache.beam.it.gcp.pubsub.PubsubResourceManager;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.gcp.storage.GcsResourceManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SpannerToSourceDbFTBase extends TemplateTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(SpannerToSourceDbFTBase.class);

  protected SpannerResourceManager createSpannerDatabase(String spannerSchemaFile)
      throws IOException {
    SpannerResourceManager spannerResourceManager =
        SpannerResourceManager.builder("ft-main-" + testName, PROJECT, REGION)
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

  protected SpannerResourceManager createSpannerMetadataDatabase() {
    SpannerResourceManager spannerMetadataResourceManager =
        SpannerResourceManager.builder("ft-meta-" + testName, PROJECT, REGION)
            .maybeUseStaticInstance()
            .build();
    String dummy = "create table t1(id INT64 ) primary key(id)";
    spannerMetadataResourceManager.executeDdlStatement(dummy);
    return spannerMetadataResourceManager;
  }

  protected void createAndUploadReverseShardConfigToGcs(
      GcsResourceManager gcsResourceManager,
      CloudSqlResourceManager cloudSqlResourceManager,
      String privateHost) {
    Shard shard = new Shard();
    shard.setLogicalShardId("Shard1");
    shard.setUser(cloudSqlResourceManager.getUsername());
    shard.setHost(privateHost);
    shard.setPassword(cloudSqlResourceManager.getPassword());
    shard.setPort(String.valueOf(cloudSqlResourceManager.getPort()));
    shard.setDbName(cloudSqlResourceManager.getDatabaseName());
    JsonObject jsObj = new Gson().toJsonTree(shard).getAsJsonObject();
    jsObj.remove("secretManagerUri"); // remove field secretManagerUri
    JsonArray ja = new JsonArray();
    ja.add(jsObj);
    String shardFileContents = ja.toString();
    LOG.info("Shard file contents: {}", shardFileContents);
    gcsResourceManager.createArtifact("input/shard.json", shardFileContents);
  }

  protected void createAndUploadReverseShardConfigToGcs(
      GcsResourceManager gcsResourceManager,
      List<CloudSqlResourceManager> cloudSqlResourceManagers,
      String privateHost,
      List<String> logicalShardIds) {
    JsonArray ja = new JsonArray();
    for (int i = 0; i < cloudSqlResourceManagers.size(); ++i) {
      Shard shard = new Shard();
      shard.setLogicalShardId(logicalShardIds.get(i));
      CloudSqlResourceManager cloudSqlResourceManager = cloudSqlResourceManagers.get(i);
      shard.setUser(cloudSqlResourceManager.getUsername());
      shard.setHost(privateHost);
      shard.setPassword(cloudSqlResourceManager.getPassword());
      shard.setPort(String.valueOf(cloudSqlResourceManager.getPort()));
      shard.setDbName(cloudSqlResourceManager.getDatabaseName());
      JsonObject jsObj = new Gson().toJsonTree(shard).getAsJsonObject();
      jsObj.remove("secretManagerUri"); // remove field secretManagerUri
      ja.add(jsObj);
    }
    String shardFileContents = ja.toString();
    LOG.info("Shard file contents: {}", shardFileContents);
    gcsResourceManager.createArtifact("input/shard.json", shardFileContents);
  }

  public PubsubResourceManager setUpPubSubResourceManager() throws IOException {
    return PubsubResourceManager.builder(testName, PROJECT, credentialsProvider).build();
  }

  public SubscriptionName createPubsubResources(
      String identifierSuffix,
      PubsubResourceManager pubsubResourceManager,
      String gcsPrefix,
      GcsResourceManager gcsResourceManager) {
    String topicNameSuffix = "rr-ft" + identifierSuffix;
    String subscriptionNameSuffix = "rr-ft-sub" + identifierSuffix;
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

  public PipelineLauncher.LaunchInfo launchRRDataflowJob(
      String jobName,
      SpannerResourceManager spannerResourceManager,
      GcsResourceManager gcsResourceManager,
      SpannerResourceManager spannerMetadataResourceManager,
      PubsubResourceManager pubsubResourceManager,
      String sourceType)
      throws IOException {

    // create subscription
    SubscriptionName rrSubscriptionName =
        createPubsubResources(
            getClass().getSimpleName(),
            pubsubResourceManager,
            getGcsPath("dlq", gcsResourceManager).replace("gs://" + artifactBucketName, ""),
            gcsResourceManager);

    // Launch Dataflow template
    FlexTemplateDataflowJobResourceManager flexTemplateDataflowJobResourceManager =
        FlexTemplateDataflowJobResourceManager.builder(jobName)
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
            .addParameter("dlqGcsPubSubSubscription", rrSubscriptionName.toString())
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
    return jobInfo;
  }

  public PipelineLauncher.LaunchInfo launchRRDataflowJob(
      String jobName,
      String additionalMavenProfile,
      Map<String, String> additionalParams,
      SubscriptionName dlqPubsubSubscription,
      SpannerResourceManager spannerResourceManager,
      GcsResourceManager gcsResourceManager,
      SpannerResourceManager spannerMetadataResourceManager,
      PubsubResourceManager pubsubResourceManager,
      String sourceType)
      throws IOException {

    if (dlqPubsubSubscription == null) {
      // create subscription
      dlqPubsubSubscription =
          createPubsubResources(
              getClass().getSimpleName(),
              pubsubResourceManager,
              getGcsPath("dlq", gcsResourceManager).replace("gs://" + artifactBucketName, ""),
              gcsResourceManager);
    }

    // Launch Dataflow template
    FlexTemplateDataflowJobResourceManager.Builder flexTemplateBuilder =
        FlexTemplateDataflowJobResourceManager.builder(jobName)
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
            .addParameter("dlqGcsPubSubSubscription", dlqPubsubSubscription.toString())
            .addParameter("deadLetterQueueDirectory", getGcsPath("dlq", gcsResourceManager))
            .addParameter("maxShardConnections", "5")
            .addParameter("maxNumWorkers", "1")
            .addParameter("numWorkers", "1")
            .addParameter("sourceType", sourceType)
            .addEnvironmentVariable(
                "additionalExperiments", Collections.singletonList("use_runner_v2"));

    if (additionalMavenProfile != null && !additionalMavenProfile.isBlank()) {
      flexTemplateBuilder.withAdditionalMavenProfile(additionalMavenProfile);
    }

    if (additionalParams != null) {
      for (Entry<String, String> param : additionalParams.entrySet()) {
        flexTemplateBuilder.addParameter(param.getKey(), param.getValue());
      }
    }

    // Run
    PipelineLauncher.LaunchInfo jobInfo = flexTemplateBuilder.build().launchJob();
    assertThatPipeline(jobInfo).isRunning();
    return jobInfo;
  }
}
