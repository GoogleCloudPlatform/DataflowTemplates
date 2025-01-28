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
package com.google.cloud.teleport.v2.templates;

import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;

import com.google.cloud.teleport.v2.spanner.migrations.transformation.CustomTransformation;
import com.google.common.io.Resources;
import com.google.pubsub.v1.SubscriptionName;
import com.google.pubsub.v1.TopicName;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.utils.PipelineUtils;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.TemplateTestBase;
import org.apache.beam.it.gcp.artifacts.utils.ArtifactUtils;
import org.apache.beam.it.gcp.pubsub.PubsubResourceManager;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.gcp.storage.GcsResourceManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class SpannerToCassandraDbITBase extends TemplateTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(SpannerToCassandraDbITBase.class);

  protected SpannerResourceManager createSpannerDatabase(String spannerSchemaFile)
      throws IOException {
    SpannerResourceManager spannerResourceManager =
        SpannerResourceManager.builder("rr-main-" + testName, PROJECT, REGION)
            .maybeUseStaticInstance()
            .build();
    String ddl =
        String.join(
            " ",
            Resources.readLines(Resources.getResource(spannerSchemaFile), StandardCharsets.UTF_8));
    ddl = ddl.trim();
    String[] ddls = ddl.split(";");
    for (String d : ddls) {
      if (!d.isBlank()) {
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

  public CassandraSharedResourceManager generateKeyspaceAndBuildCassandraResource() {
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
    return CassandraSharedResourceManager.builder(testName)
        .setKeyspaceName(keyspaceName)
        .sePreGeneratedKeyspaceName(true)
        .build();
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

  public void createAndUploadCassandraConfigToGcs(
      GcsResourceManager gcsResourceManager,
      CassandraSharedResourceManager cassandraResourceManagers)
      throws IOException {

    String host = cassandraResourceManagers.getHost();
    int port = cassandraResourceManagers.getPort();
    String keyspaceName = cassandraResourceManagers.getKeyspaceName();
    System.out.println("Cassandra keyspaceName :: {}" + keyspaceName);
    System.out.println("Cassandra host :: {}" + host);
    System.out.println("Cassandra port :: {}" + port);
    String cassandraConfigContents;
    try (InputStream inputStream =
        Thread.currentThread()
            .getContextClassLoader()
            .getResourceAsStream("SpannerToCassandraSourceIT/cassandra-config-template.conf")) {
      if (inputStream == null) {
        throw new FileNotFoundException(
            "Resource file not found: SpannerToCassandraSourceIT/cassandra-config-template.conf");
      }
      cassandraConfigContents = new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
    }

    cassandraConfigContents =
        cassandraConfigContents
            .replace("##host##", host)
            .replace("##port##", Integer.toString(port))
            .replace("##keyspace##", keyspaceName);

    System.out.println("Cassandra file contents: {}" + cassandraConfigContents);
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
      CustomTransformation customTransformation)
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
                getGcsPath("input/cassandra-config.conf", gcsResourceManager));
            put("changeStreamName", "allstream");
            put("dlqGcsPubSubSubscription", subscriptionName);
            put("deadLetterQueueDirectory", getGcsPath("dlq", gcsResourceManager));
            put("maxShardConnections", "5");
            put("maxNumWorkers", "1");
            put("numWorkers", "1");
            put("sourceType", "cassandra");
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

  protected void createCassandraSchema(
      CassandraSharedResourceManager cassandraResourceManager, String cassandraSchemaFile)
      throws IOException {
    String ddl =
        String.join(
            " ",
            Resources.readLines(
                Resources.getResource(cassandraSchemaFile), StandardCharsets.UTF_8));
    ddl = ddl.trim();
    System.out.println("DDL {}" + ddl);
    String[] ddls = ddl.split(";");
    System.out.println("DDLs statement {}" + Arrays.toString(ddls));
    for (String d : ddls) {
      System.out.println("DDL statement {}" + d);
      if (!d.isBlank()) {
        cassandraResourceManager.execute(d);
      }
    }
  }

  public String getGcsFullPath(
      GcsResourceManager gcsResourceManager, String artifactId, String identifierSuffix) {
    return ArtifactUtils.getFullGcsPath(
        artifactBucketName, identifierSuffix, gcsResourceManager.runId(), artifactId);
  }
}
