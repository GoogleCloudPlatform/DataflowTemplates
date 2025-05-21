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

import static com.google.cloud.teleport.v2.templates.constants.DatastreamToSpannerConstants.RETRYABLE_ERRORS_COUNTER_NAME;
import static java.util.Arrays.stream;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.cloud.datastream.v1.DestinationConfig;
import com.google.cloud.datastream.v1.SourceConfig;
import com.google.cloud.datastream.v1.Stream;
import com.google.common.io.Resources;
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
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineLauncher.LaunchInfo;
import org.apache.beam.it.common.TestProperties;
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
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class DataStreamToSpannerFTBase extends TemplateTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(DataStreamToSpannerFTBase.class);
  public DatastreamResourceManager datastreamResourceManager;

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

  public TopicName createPubsubTopic(
      String identifierSuffix, PubsubResourceManager pubsubResourceManager) {
    String topicNameSuffix = "FT-" + identifierSuffix;
    TopicName topic = pubsubResourceManager.createTopic(topicNameSuffix);
    return topic;
  }

  public SubscriptionName createPubsubSubscription(
      String identifierSuffix, PubsubResourceManager pubsubResourceManager, TopicName topic) {
    String subscriptionNameSuffix = "FT-" + identifierSuffix;
    SubscriptionName subscription =
        pubsubResourceManager.createSubscription(topic, subscriptionNameSuffix);
    return subscription;
  }

  public void createGcsPubsubNotification(
      TopicName topic, String gcsPrefix, GcsResourceManager gcsResourceManager) {
    String prefix = gcsPrefix;
    if (prefix.startsWith("/")) {
      prefix = prefix.substring(1);
    }
    gcsResourceManager.createNotification(topic.toString(), prefix);
  }

  public String getGcsFullPath(
      GcsResourceManager gcsResourceManager, String artifactId, String identifierSuffix) {
    return ArtifactUtils.getFullGcsPath(
        artifactBucketName, identifierSuffix, gcsResourceManager.runId(), artifactId);
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
      PubsubResourceManager pubsubResourceManager,
      FlexTemplateDataflowJobResourceManager.Builder flexTemplateDataflowJobResourceManagerBuilder,
      JDBCSource sourceConnectionProfile)
      throws IOException {
    String testRootDir = getClass().getSimpleName();

    // create subscriptions
    String gcsPrefix =
        String.join("/", new String[] {testRootDir, gcsResourceManager.runId(), testName, "cdc"});
    SubscriptionName subscription =
        createPubsubResources(
            testRootDir + testName, pubsubResourceManager, gcsPrefix, gcsResourceManager);

    String dlqGcsPrefix =
        String.join("/", new String[] {testRootDir, gcsResourceManager.runId(), testName, "dlq"});
    SubscriptionName dlqSubscription =
        createPubsubResources(
            testRootDir + testName + "dlq",
            pubsubResourceManager,
            dlqGcsPrefix,
            gcsResourceManager);
    String artifactBucket = TestProperties.artifactBucket();

    // launch datastream
    datastreamResourceManager =
        DatastreamResourceManager.builder(testName, PROJECT, REGION)
            .setCredentialsProvider(credentialsProvider)
            .setPrivateConnectivity("datastream-private-connect-us-central1")
            .build();
    Stream stream =
        createDataStreamResources(
            artifactBucket, gcsPrefix, sourceConnectionProfile, datastreamResourceManager);

    // launch dataflow template
    LaunchInfo jobInfo =
        launchFwdDataflowJob(
            spannerResourceManager,
            gcsPrefix,
            stream.getName(),
            dlqGcsPrefix,
            subscription.toString(),
            dlqSubscription.toString(),
            flexTemplateDataflowJobResourceManagerBuilder);
    assertThatPipeline(jobInfo).isRunning();
    return jobInfo;
  }

  public PipelineLauncher.LaunchInfo launchFwdDataflowJob(
      SpannerResourceManager spannerResourceManager,
      String gcsPrefix,
      String streamName,
      String dlqGcsPrefix,
      String pubSubSubscription,
      String dlqPubSubSubscription,
      FlexTemplateDataflowJobResourceManager.Builder flexTemplateDataflowJobResourceManagerBuilder)
      throws IOException {
    String artifactBucket = TestProperties.artifactBucket();

    // launch dataflow template
    FlexTemplateDataflowJobResourceManager flexTemplateDataflowJobResourceManager =
        flexTemplateDataflowJobResourceManagerBuilder
            .withTemplateName("Cloud_Datastream_to_Spanner")
            .withTemplateModulePath("v2/datastream-to-spanner")
            .addParameter("inputFilePattern", getGcsPath(artifactBucket, gcsPrefix))
            .addParameter("streamName", streamName)
            .addParameter("instanceId", spannerResourceManager.getInstanceId())
            .addParameter("databaseId", spannerResourceManager.getDatabaseId())
            .addParameter("projectId", PROJECT)
            .addParameter("deadLetterQueueDirectory", getGcsPath(artifactBucket, dlqGcsPrefix))
            .addParameter("gcsPubSubSubscription", pubSubSubscription)
            .addParameter("dlqGcsPubSubSubscription", dlqPubSubSubscription)
            .addParameter("datastreamSourceType", "mysql")
            .addParameter("inputFileFormat", "avro")
            .build();

    // Run
    PipelineLauncher.LaunchInfo jobInfo = flexTemplateDataflowJobResourceManager.launchJob();
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

  protected String generateSessionFile(String srcDb, String spannerDb, String sessionFileResource)
      throws IOException {
    String sessionFile =
        Files.readString(Paths.get(Resources.getResource(sessionFileResource).getPath()));
    return sessionFile.replaceAll("SRC_DATABASE", srcDb).replaceAll("SP_DATABASE", spannerDb);
  }

  protected JDBCSource createMySQLSourceConnectionProfile(
      CloudSqlResourceManager cloudSqlResourceManager, List<String> tables) {
    return MySQLSource.builder(
            cloudSqlResourceManager.getHost(),
            cloudSqlResourceManager.getUsername(),
            cloudSqlResourceManager.getPassword(),
            cloudSqlResourceManager.getPort())
        .setAllowedTables(Map.of(cloudSqlResourceManager.getDatabaseName(), tables))
        .build();
  }

  public static class RetryableErrorsCheck extends ConditionCheck {

    private Integer minErrors;

    private PipelineLauncher.LaunchInfo jobInfo;

    private PipelineLauncher pipelineLauncher;

    @Override
    public String getDescription() {
      return String.format("Retryable errors check if %d errors are present", minErrors);
    }

    @Override
    public CheckResult check() {
      Double retryableErrors = null;
      try {
        retryableErrors =
            pipelineLauncher.getMetric(
                PROJECT, REGION, jobInfo.jobId(), RETRYABLE_ERRORS_COUNTER_NAME);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      if (retryableErrors < minErrors) {
        return new CheckResult(
            false,
            String.format(
                "Expected at least %d errors but has only %.1f", minErrors, retryableErrors));
      }
      return new CheckResult(
          true,
          String.format("Expected at least %d errors and found %.1f", minErrors, retryableErrors));
    }

    public RetryableErrorsCheck(
        PipelineLauncher pipelineLauncher, PipelineLauncher.LaunchInfo jobInfo, int minErrors) {
      this.pipelineLauncher = pipelineLauncher;
      this.jobInfo = jobInfo;
      this.minErrors = minErrors;
    }
  }
}
