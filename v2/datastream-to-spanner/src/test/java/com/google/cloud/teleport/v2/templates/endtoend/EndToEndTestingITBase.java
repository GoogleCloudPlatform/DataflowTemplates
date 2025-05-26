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
package com.google.cloud.teleport.v2.templates.endtoend;

import static java.util.Arrays.stream;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.cloud.datastream.v1.DestinationConfig;
import com.google.cloud.datastream.v1.SourceConfig;
import com.google.cloud.datastream.v1.Stream;
import com.google.cloud.teleport.v2.spanner.migrations.shard.Shard;
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
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.TestProperties;
import org.apache.beam.it.common.utils.PipelineUtils;
import org.apache.beam.it.conditions.ConditionCheck;
import org.apache.beam.it.gcp.TemplateTestBase;
import org.apache.beam.it.gcp.artifacts.utils.ArtifactUtils;
import org.apache.beam.it.gcp.cloudsql.CloudSqlResourceManager;
import org.apache.beam.it.gcp.dataflow.FlexTemplateDataflowJobResourceManager;
import org.apache.beam.it.gcp.datastream.DatastreamResourceManager;
import org.apache.beam.it.gcp.datastream.JDBCSource;
import org.apache.beam.it.gcp.datastream.MySQLSource;
import org.apache.beam.it.gcp.pubsub.PubsubResourceManager;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.gcp.storage.GcsResourceManager;
import org.apache.beam.it.jdbc.JDBCResourceManager;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class EndToEndTestingITBase extends TemplateTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(EndToEndTestingITBase.class);
  private static FlexTemplateDataflowJobResourceManager flexTemplateDataflowJobResourceManager;
  public DatastreamResourceManager datastreamResourceManager;
  protected JDBCSource jdbcSource;

  protected SpannerResourceManager createSpannerDatabase(String spannerSchemaFile)
      throws IOException {
    SpannerResourceManager spannerResourceManager =
        SpannerResourceManager.builder("e2e-main-" + testName, PROJECT, REGION)
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
        SpannerResourceManager.builder("e2e-meta-" + testName, PROJECT, REGION)
            .maybeUseStaticInstance()
            .build();
    String dummy = "create table t1(id INT64 ) primary key(id)";
    spannerMetadataResourceManager.executeDdlStatement(dummy);
    return spannerMetadataResourceManager;
  }

  public PubsubResourceManager setUpPubSubResourceManager() throws IOException {
    return PubsubResourceManager.builder(testName, PROJECT, credentialsProvider).build();
  }

  // createPubsubResources generates pubsub topic, subscription and notification for migration.
  // It can be run in different modes based on type of migration.
  // Modes can be rr for reverse replication and fwd for forward migration
  public SubscriptionName createPubsubResources(
      String identifierSuffix,
      PubsubResourceManager pubsubResourceManager,
      String gcsPrefix,
      GcsResourceManager gcsResourceManager,
      String mode) {
    String topicNameSuffix = mode + "-it" + identifierSuffix;
    String subscriptionNameSuffix = mode + "-it-sub" + identifierSuffix;
    TopicName topic = pubsubResourceManager.createTopic(topicNameSuffix);
    SubscriptionName subscription =
        pubsubResourceManager.createSubscription(topic, subscriptionNameSuffix);
    String prefix = gcsPrefix;
    if (prefix.startsWith("/")) {
      prefix = prefix.substring(1);
    }
    // create retry directory for reverse migration
    if (mode == "rr") {
      prefix += "/retry/";
    }
    gcsResourceManager.createNotification(topic.toString(), prefix);
    return subscription;
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

  public String getGcsFullPath(
      GcsResourceManager gcsResourceManager, String artifactId, String identifierSuffix) {
    return ArtifactUtils.getFullGcsPath(
        artifactBucketName, identifierSuffix, gcsResourceManager.runId(), artifactId);
  }

  public PipelineLauncher.LaunchInfo launchRRDataflowJob(
      SpannerResourceManager spannerResourceManager,
      GcsResourceManager gcsResourceManager,
      SpannerResourceManager spannerMetadataResourceManager,
      PubsubResourceManager pubsubResourceManager,
      String sourceType)
      throws IOException {
    String rrJobName = PipelineUtils.createJobName("rrev-it" + testName);

    // create subscription
    SubscriptionName rrSubscriptionName =
        createPubsubResources(
            getClass().getSimpleName(),
            pubsubResourceManager,
            getGcsPath("dlq", gcsResourceManager).replace("gs://" + artifactBucketName, ""),
            gcsResourceManager,
            "rr");

    // Launch Dataflow template
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

  public String getGcsPath(String... pathParts) {
    checkArgument(pathParts.length != 0, "Must provide at least one path part");
    checkArgument(
        stream(pathParts).noneMatch(Strings::isNullOrEmpty), "No path part can be null or empty");

    return String.format("gs://%s", String.join("/", pathParts));
  }

  public PipelineLauncher.LaunchInfo launchFwdDataflowJob(
      SpannerResourceManager spannerResourceManager,
      GcsResourceManager gcsResourceManager,
      PubsubResourceManager pubsubResourceManager)
      throws IOException {
    String testRootDir = getClass().getSimpleName();

    // create subscriptions
    String gcsPrefix =
        String.join("/", new String[] {testRootDir, gcsResourceManager.runId(), testName, "cdc"});
    SubscriptionName subscription =
        createPubsubResources(
            testRootDir + testName, pubsubResourceManager, gcsPrefix, gcsResourceManager, "fwd");

    String dlqGcsPrefix =
        String.join("/", new String[] {testRootDir, gcsResourceManager.runId(), testName, "dlq"});
    SubscriptionName dlqSubscription =
        createPubsubResources(
            testRootDir + testName + "dlq",
            pubsubResourceManager,
            dlqGcsPrefix,
            gcsResourceManager,
            "fwd");
    String artifactBucket = TestProperties.artifactBucket();

    // launch datastream
    datastreamResourceManager =
        DatastreamResourceManager.builder(testName, PROJECT, REGION)
            .setCredentialsProvider(credentialsProvider)
            .setPrivateConnectivity("datastream-private-connect-us-central1")
            .build();
    Stream stream =
        createDataStreamResources(artifactBucket, gcsPrefix, jdbcSource, datastreamResourceManager);

    String jobName = PipelineUtils.createJobName("fwd-" + getClass().getSimpleName());
    // launch dataflow template
    flexTemplateDataflowJobResourceManager =
        FlexTemplateDataflowJobResourceManager.builder(jobName)
            .withTemplateName("Cloud_Datastream_to_Spanner")
            .withTemplateModulePath("v2/datastream-to-spanner")
            .addParameter("inputFilePattern", getGcsPath(artifactBucket, gcsPrefix))
            .addParameter("streamName", stream.getName())
            .addParameter("instanceId", spannerResourceManager.getInstanceId())
            .addParameter("databaseId", spannerResourceManager.getDatabaseId())
            .addParameter("projectId", PROJECT)
            .addParameter("deadLetterQueueDirectory", getGcsPath(artifactBucket, dlqGcsPrefix))
            .addParameter("gcsPubSubSubscription", subscription.toString())
            .addParameter("dlqGcsPubSubSubscription", dlqSubscription.toString())
            .addParameter("datastreamSourceType", "mysql")
            .addParameter("inputFileFormat", "avro")
            .addParameter("sessionFilePath", getGcsPath("input/session.json", gcsResourceManager))
            .addEnvironmentVariable(
                "additionalExperiments", Collections.singletonList("use_runner_v2"))
            .build();

    // Run
    PipelineLauncher.LaunchInfo jobInfo = flexTemplateDataflowJobResourceManager.launchJob();
    assertThatPipeline(jobInfo).isRunning();
    return jobInfo;
  }

  public Stream createDataStreamResources(
      String artifactBucketName,
      String gcsPrefix,
      JDBCSource jdbcSource,
      DatastreamResourceManager datastreamResourceManager) {
    SourceConfig sourceConfig =
        datastreamResourceManager.buildJDBCSourceConfig("mysql", jdbcSource);

    // Create DataStream GCS Destination Connection profile and config
    DestinationConfig destinationConfig =
        datastreamResourceManager.buildGCSDestinationConfig(
            "gcs",
            artifactBucketName,
            gcsPrefix,
            DatastreamResourceManager.DestinationOutputFormat.AVRO_FILE_FORMAT);

    // Create and start DataStream stream
    Stream stream =
        datastreamResourceManager.createStream("ds-spanner", sourceConfig, destinationConfig);
    datastreamResourceManager.startStream(stream);
    return stream;
  }

  protected ConditionCheck writeJdbcData(
      String tableName,
      Integer numRows,
      Map<String, Object> columns,
      Map<String, List<Map<String, Object>>> cdcEvents,
      CloudSqlResourceManager cloudSqlResourceManager) {
    return new ConditionCheck() {
      @Override
      protected String getDescription() {
        return "Send initial JDBC events.";
      }

      @Override
      protected CheckResult check() {
        boolean success = true;
        List<String> messages = new ArrayList<>();
        List<Map<String, Object>> rows = new ArrayList<>();
        for (int i = 0; i < numRows; i++) {
          Map<String, Object> values = new HashMap<>();
          values.put("id", i);
          values.putAll(columns);
          rows.add(values);
        }
        cdcEvents.put(tableName, rows);
        success &= cloudSqlResourceManager.write(tableName, rows);
        messages.add(String.format("%d rows to %s", rows.size(), tableName));

        return new CheckResult(success, "Sent " + String.join(", ", messages) + ".");
      }
    };
  }

  protected String generateSessionFile(String srcDb, String spannerDb, String sessionFileResource)
      throws IOException {
    String sessionFile =
        Files.readString(Paths.get(Resources.getResource(sessionFileResource).getPath()));
    return sessionFile.replaceAll("SRC_DATABASE", srcDb).replaceAll("SP_DATABASE", spannerDb);
  }

  protected JDBCSource createMySqlDatabase(
      CloudSqlResourceManager cloudSqlResourceManager, Map<String, Map<String, String>> tables) {
    for (HashMap.Entry<String, Map<String, String>> entry : tables.entrySet()) {
      cloudSqlResourceManager.createTable(
          entry.getKey(), new JDBCResourceManager.JDBCSchema(entry.getValue(), "id"));
    }
    return MySQLSource.builder(
            cloudSqlResourceManager.getHost(),
            cloudSqlResourceManager.getUsername(),
            cloudSqlResourceManager.getPassword(),
            cloudSqlResourceManager.getPort())
        .setAllowedTables(
            Map.of(cloudSqlResourceManager.getDatabaseName(), tables.keySet().stream().toList()))
        .build();
  }
}
