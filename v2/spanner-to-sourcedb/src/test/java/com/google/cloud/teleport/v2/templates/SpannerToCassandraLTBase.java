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

import com.google.common.base.MoreObjects;
import com.google.common.io.Resources;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineLauncher.LaunchConfig;
import org.apache.beam.it.common.PipelineLauncher.LaunchInfo;
import org.apache.beam.it.common.TestProperties;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.artifacts.utils.ArtifactUtils;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.gcp.storage.GcsResourceManager;

/**
 * Base class for Spanner to sourcedb Load tests. It provides helper functions related to
 * environment setup and assertConditions.
 */
public class SpannerToCassandraLTBase extends SpannerToSourceDbLTBase {

  private static final String TEMPLATE_SPEC_PATH =
      MoreObjects.firstNonNull(
          TestProperties.specPath(),
          "gs://dataflow-templates-spanner-to-cassandra/templates/flex/Spanner_to_SourceDb");
  public CassandraResourceManager cassandraSharedResourceManager;

  public void setupResourceManagers(
      String spannerDdlResource, String cassandraDdlResource, String artifactBucket)
      throws IOException {
    spannerResourceManager = createSpannerDatabase(spannerDdlResource);
    spannerMetadataResourceManager = createSpannerMetadataDatabase();
    cassandraSharedResourceManager = generateKeyspaceAndBuildCassandraResource();

    gcsResourceManager =
        GcsResourceManager.builder(artifactBucket, getClass().getSimpleName(), CREDENTIALS).build();
    createCassandraSchema(cassandraSharedResourceManager, cassandraDdlResource);
    createAndUploadCassandraConfigToGcs(gcsResourceManager, cassandraSharedResourceManager);
    pubsubResourceManager = setUpPubSubResourceManager();
    subscriptionName =
        createPubsubResources(
            getClass().getSimpleName(),
            pubsubResourceManager,
            getGcsPath(artifactBucket, "dlq", gcsResourceManager)
                .replace("gs://" + artifactBucket, ""));
  }

  public CassandraResourceManager generateKeyspaceAndBuildCassandraResource() {
    String keyspaceName =
        ResourceManagerUtils.generateResourceId(
                testName,
                Pattern.compile("[/\\\\. \"\u0000$]"),
                "-",
                27,
                DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss-SSSSSS"))
            .replace('-', '_');
    if (keyspaceName.length() > 48) {
      keyspaceName = keyspaceName.substring(0, 48);
    }

    return CassandraResourceManager.builder(testName)
        .setKeyspaceName(keyspaceName)
        .sePreGeneratedKeyspaceName(true)
        .build();
  }

  public void cleanupResourceManagers() {
    ResourceManagerUtils.cleanResources(
        spannerResourceManager,
        spannerMetadataResourceManager,
        gcsResourceManager,
        pubsubResourceManager,
        cassandraSharedResourceManager);
  }

  public SpannerResourceManager createSpannerDatabase(String spannerDdlResourceFile)
      throws IOException {
    SpannerResourceManager spannerResourceManager =
        SpannerResourceManager.builder("rr-lt-main" + testName, project, region)
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

  public void createAndUploadCassandraConfigToGcs(
      GcsResourceManager gcsResourceManager, CassandraResourceManager cassandraResourceManagers)
      throws IOException {

    String host = cassandraResourceManagers.getHost();
    int port = cassandraResourceManagers.getPort();
    String keyspaceName = cassandraResourceManagers.getKeyspaceName();

    String cassandraConfigContents;
    try (InputStream inputStream =
        Thread.currentThread()
            .getContextClassLoader()
            .getResourceAsStream("SpannerToCassandraSourceLT/cassandra-config-template.conf")) {
      if (inputStream == null) {
        throw new FileNotFoundException(
            "Resource file not found: SpannerToCassandraSourceLT/cassandra-config-template.conf");
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

  public void createCassandraSchema(
      CassandraResourceManager cassandraResourceManager, String cassandraDdlResourceFile)
      throws IOException {
    String ddl =
        String.join(
            " ",
            Resources.readLines(
                Resources.getResource(cassandraDdlResourceFile), StandardCharsets.UTF_8));

    ddl = ddl.trim();
    String[] ddls = ddl.split(";");
    for (String d : ddls) {
      if (!d.isBlank()) {
        cassandraResourceManager.executeStatement(d);
      }
    }
  }

  public PipelineLauncher.LaunchInfo launchDataflowJob(
      String artifactBucket, int numWorkers, int maxWorkers) throws IOException {
    // default parameters

    Map<String, String> params =
        new HashMap<>() {
          {
            put("instanceId", spannerResourceManager.getInstanceId());
            put("databaseId", spannerResourceManager.getDatabaseId());
            put("spannerProjectId", project);
            put("metadataDatabase", spannerMetadataResourceManager.getDatabaseId());
            put("metadataInstance", spannerMetadataResourceManager.getInstanceId());
            put(
                "sourceShardsFilePath",
                getGcsPath(artifactBucket, "input/cassandra-config.conf", gcsResourceManager));
            put("changeStreamName", "allstream");
            put("dlqGcsPubSubSubscription", subscriptionName.toString());
            put("deadLetterQueueDirectory", getGcsPath(artifactBucket, "dlq", gcsResourceManager));
            put("maxShardConnections", "100");
            put("sourceType", "cassandra");
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
}
